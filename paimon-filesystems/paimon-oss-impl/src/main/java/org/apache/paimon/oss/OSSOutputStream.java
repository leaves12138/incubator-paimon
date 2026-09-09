/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.oss;

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.BlockingExecutor;

import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Disk-buffered output with a bounded number of concurrent multipart uploads. */
final class OSSOutputStream extends PositionOutputStream {
    private final OSSFileSystem fs;
    private final Path path;
    private final String key;
    private final boolean overwrite;
    private final long partSize;
    private final OSSMultiPartUpload store;
    private final BlockingExecutor executor;
    private final Configuration bufferConfiguration;
    private final LocalDirAllocator directoryAllocator;
    private final List<CompletableFuture<OSSPartETag>> parts = new ArrayList<>();
    private final AtomicReference<Throwable> uploadFailure = new AtomicReference<>();

    private File file;
    private OutputStream out;
    private long buffered;
    private long position;
    private String uploadId;
    private boolean closed;
    private IOException failure;

    OSSOutputStream(OSSFileSystem fs, Path path, boolean overwrite) throws IOException {
        this.fs = fs;
        this.path = path;
        this.key = fs.objectKey(path);
        this.overwrite = overwrite;
        this.partSize = fs.getConf().getLong("fs.oss.multipart.upload.size", 100L * 1024 * 1024);
        int active = fs.getConf().getInt("fs.oss.upload.active.blocks", 4);
        checkArgument(
                partSize >= 100 * 1024 && partSize <= Integer.MAX_VALUE && active > 0,
                "Invalid OSS multipart size or active block count.");
        this.store = new OSSMultiPartUpload(fs, overwrite);
        this.executor = new BlockingExecutor(fs.uploadExecutor(), active);
        this.bufferConfiguration = new Configuration(fs.getConf());
        if (bufferConfiguration.getTrimmed("fs.oss.buffer.dir", "").isEmpty()) {
            bufferConfiguration.set(
                    "fs.oss.buffer.dir",
                    bufferConfiguration.get("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"))
                            + "/oss");
        }
        this.directoryAllocator = new LocalDirAllocator("fs.oss.buffer.dir");
        openBuffer();
    }

    private void openBuffer() throws IOException {
        // Preserve Hadoop's multi-directory allocation and free-space checks for upload buffers.
        file =
                directoryAllocator.createTmpFileForWrite(
                        "paimon-oss-", partSize, bufferConfiguration);
        try {
            out = new BufferedOutputStream(new FileOutputStream(file), 64 * 1024);
        } catch (IOException e) {
            Files.deleteIfExists(file.toPath());
            throw e;
        }
        buffered = 0;
    }

    @Override
    public long getPos() {
        return position;
    }

    @Override
    public void write(int value) throws IOException {
        ensureOpen();
        try {
            checkUploadFailure();
            out.write(value);
            buffered++;
            position++;
            if (buffered == partSize) {
                submitPart();
                openBuffer();
            }
        } catch (IOException | RuntimeException e) {
            fail(e);
            throw failure;
        } catch (Error e) {
            fail(e);
            throw e;
        }
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IndexOutOfBoundsException();
        }
        ensureOpen();
        try {
            while (length > 0) {
                checkUploadFailure();
                int count = (int) Math.min(length, partSize - buffered);
                out.write(bytes, offset, count);
                offset += count;
                length -= count;
                buffered += count;
                position += count;
                if (buffered == partSize) {
                    submitPart();
                    openBuffer();
                }
            }
        } catch (IOException | RuntimeException e) {
            fail(e);
            throw failure;
        } catch (Error e) {
            fail(e);
            throw e;
        }
    }

    private void submitPart() throws IOException {
        if (parts.size() >= 10_000) {
            throw new IOException("OSS multipart upload exceeds 10000 parts.");
        }
        out.close();
        out = null;
        if (uploadId == null) {
            uploadId = store.startMultiPartUpload(key);
        }
        final File pendingFile = file;
        final int size = (int) buffered;
        final int number = parts.size() + 1;
        final String id = uploadId;
        CompletableFuture<OSSPartETag> future = new CompletableFuture<>();
        executor.submit(
                () -> {
                    OSSPartETag part = null;
                    Throwable failure = null;
                    try {
                        part = store.uploadPart(key, id, number, pendingFile, size);
                    } catch (Throwable e) {
                        failure = e;
                    } finally {
                        try {
                            Files.deleteIfExists(pendingFile.toPath());
                        } catch (Throwable e) {
                            if (failure == null) {
                                failure = e;
                            } else {
                                failure.addSuppressed(e);
                            }
                        }
                    }
                    // close() must observe cleanup failures before committing the object.
                    if (failure == null) {
                        future.complete(part);
                    } else {
                        uploadFailure.compareAndSet(null, failure);
                        future.completeExceptionally(failure);
                    }
                });
        parts.add(future);
        file = null;
        buffered = 0;
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();
        try {
            checkUploadFailure();
            out.flush();
        } catch (IOException | RuntimeException e) {
            fail(e);
            throw failure;
        } catch (Error e) {
            fail(e);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            if (failure != null) {
                throw failure;
            }
            return;
        }
        closed = true;
        try {
            checkUploadFailure();
            if (uploadId == null) {
                out.close();
                out = null;
                try (FileInputStream input = new FileInputStream(file)) {
                    fs.client()
                            .putObject(
                                    PutObjectRequest.newBuilder()
                                            .bucket(fs.bucket())
                                            .key(key)
                                            .headers(fs.writeHeaders())
                                            .forbidOverwrite(!overwrite)
                                            .body(OSSMultiPartUpload.fileBody(input, file.length()))
                                            .build());
                }
            } else {
                if (buffered > 0) {
                    submitPart();
                } else {
                    out.close();
                    out = null;
                }
                List<OSSPartETag> uploaded = new ArrayList<>();
                for (CompletableFuture<OSSPartETag> part : parts) {
                    uploaded.add(part.get());
                }
                checkUploadFailure();
                store.completeMultipartUpload(key, uploadId, uploaded, position);
                uploadId = null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e);
        } catch (ExecutionException | IOException | RuntimeException e) {
            fail(e);
        } catch (Error e) {
            fail(e);
            throw e;
        } finally {
            if (file != null) {
                try {
                    Files.deleteIfExists(file.toPath());
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
                file = null;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void ensureOpen() throws IOException {
        if (failure != null) {
            throw failure;
        }
        if (closed) {
            throw new IOException("OSS output stream is closed.");
        }
    }

    private void checkUploadFailure() throws IOException {
        Throwable error = uploadFailure.get();
        if (error != null) {
            throw new IOException("OSS part upload failed.", error);
        }
    }

    private void fail(Throwable error) {
        closed = true;
        failure = OSSFileSystem.ioException("Failed to write object", path, error);
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
            out = null;
        }
        if (uploadId != null) {
            try {
                store.abortMultipartUpload(key, uploadId);
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
        }
        if (file != null) {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
            file = null;
        }
    }
}
