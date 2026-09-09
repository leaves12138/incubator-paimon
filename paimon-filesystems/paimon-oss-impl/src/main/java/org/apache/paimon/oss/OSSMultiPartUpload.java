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

import org.apache.paimon.fs.MultiPartUploadStore;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUpload;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.Part;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartCopyRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartCopyResult;
import com.aliyun.sdk.service.oss2.models.UploadPartRequest;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Multipart operations implemented entirely by OSS SDK v2. */
public class OSSMultiPartUpload
        implements MultiPartUploadStore<OSSPartETag, CompleteMultipartUploadResult> {
    private final OSSFileSystem fs;
    private final boolean overwrite;

    OSSMultiPartUpload(OSSFileSystem fs, boolean overwrite) {
        this.fs = fs;
        this.overwrite = overwrite;
    }

    @Override
    public Path workingDirectory() {
        return fs.getWorkingDirectory();
    }

    @Override
    public String startMultiPartUpload(String objectName) throws IOException {
        try {
            return fs.client()
                    .initiateMultipartUpload(
                            InitiateMultipartUploadRequest.newBuilder()
                                    .bucket(fs.bucket())
                                    .key(objectName)
                                    .headers(fs.writeHeaders())
                                    .forbidOverwrite(!overwrite)
                                    .build())
                    .initiateMultipartUpload()
                    .uploadId();
        } catch (RuntimeException e) {
            throw OSSFileSystem.ioException(
                    "Failed to start multipart upload",
                    new Path(fs.getUri() + "/" + objectName),
                    e);
        }
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(
            String objectName, String uploadId, List<OSSPartETag> parts, long size)
            throws IOException {
        try {
            if (parts.isEmpty()) {
                abortMultipartUpload(objectName, uploadId);
                fs.client()
                        .putObject(
                                PutObjectRequest.newBuilder()
                                        .bucket(fs.bucket())
                                        .key(objectName)
                                        .headers(fs.writeHeaders())
                                        .forbidOverwrite(!overwrite)
                                        .body(BinaryData.fromBytes(new byte[0]))
                                        .build());
                return CompleteMultipartUploadResult.newBuilder().build();
            }
            return fs.client()
                    .completeMultipartUpload(
                            CompleteMultipartUploadRequest.newBuilder()
                                    .bucket(fs.bucket())
                                    .key(objectName)
                                    .uploadId(uploadId)
                                    .forbidOverwrite(!overwrite)
                                    .completeMultipartUpload(
                                            CompleteMultipartUpload.newBuilder()
                                                    .parts(
                                                            parts.stream()
                                                                    .map(OSSPartETag::toSdkPart)
                                                                    .collect(Collectors.toList()))
                                                    .build())
                                    .build());
        } catch (RuntimeException e) {
            throw OSSFileSystem.ioException(
                    "Failed to complete multipart upload",
                    new Path(fs.getUri() + "/" + objectName),
                    e);
        }
    }

    @Override
    public OSSPartETag uploadPart(
            String objectName, String uploadId, int partNumber, File file, int byteLength)
            throws IOException {
        try (FileInputStream input = new FileInputStream(file)) {
            return new OSSPartETag(
                    partNumber,
                    fs.client()
                            .uploadPart(
                                    UploadPartRequest.newBuilder()
                                            .bucket(fs.bucket())
                                            .key(objectName)
                                            .uploadId(uploadId)
                                            .partNumber((long) partNumber)
                                            .contentLength((long) byteLength)
                                            .body(fileBody(input, byteLength))
                                            .build())
                            .eTag());
        } catch (RuntimeException e) {
            throw OSSFileSystem.ioException(
                    "Failed to upload part", new Path(fs.getUri() + "/" + objectName), e);
        }
    }

    @Override
    public void abortMultipartUpload(String objectName, String uploadId) throws IOException {
        try {
            fs.client()
                    .abortMultipartUpload(
                            AbortMultipartUploadRequest.newBuilder()
                                    .bucket(fs.bucket())
                                    .key(objectName)
                                    .uploadId(uploadId)
                                    .build());
        } catch (RuntimeException e) {
            ServiceException service = ServiceException.asCause(e);
            if (service == null || !"NoSuchUpload".equals(service.errorCode())) {
                throw OSSFileSystem.ioException(
                        "Failed to abort multipart upload",
                        new Path(fs.getUri() + "/" + objectName),
                        e);
            }
        }
    }

    static BinaryData fileBody(FileInputStream input, long length) {
        return BinaryData.fromStream(
                new FilterInputStream(input) {
                    private long marked;

                    @Override
                    public boolean markSupported() {
                        return true;
                    }

                    @Override
                    public void mark(int limit) {
                        try {
                            marked = input.getChannel().position();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public void reset() throws IOException {
                        input.getChannel().position(marked);
                    }

                    @Override
                    public void close() {
                        // HTTP attempts may close their entity; the caller owns the file until
                        // retries end.
                    }
                },
                length);
    }

    static void copyRange(
            OSSClient client,
            String bucket,
            String source,
            String target,
            long offset,
            long length,
            Map<String, String> headers,
            Map<String, String> metadata)
            throws IOException {
        checkArgument(
                offset >= 0 && length > 0 && offset <= Long.MAX_VALUE - length,
                "Invalid copy range.");
        long partSize = Math.max(100L * 1024 * 1024, length / 10_000 + 1);
        checkArgument(partSize <= 5L * 1024 * 1024 * 1024, "Copy exceeds multipart size limits.");
        String uploadId = null;
        try {
            uploadId =
                    client.initiateMultipartUpload(
                                    InitiateMultipartUploadRequest.newBuilder()
                                            .bucket(bucket)
                                            .key(target)
                                            .headers(headers)
                                            .metadata(metadata)
                                            .build())
                            .initiateMultipartUpload()
                            .uploadId();
            List<Part> parts = new ArrayList<>();
            long copied = 0;
            while (copied < length) {
                long size = Math.min(partSize, length - copied);
                long number = parts.size() + 1L;
                UploadPartCopyResult result =
                        client.uploadPartCopy(
                                UploadPartCopyRequest.newBuilder()
                                        .bucket(bucket)
                                        .key(target)
                                        .sourceBucket(bucket)
                                        .sourceKey(source)
                                        .uploadId(uploadId)
                                        .partNumber(number)
                                        .copySourceRange(
                                                "bytes="
                                                        + (offset + copied)
                                                        + "-"
                                                        + (offset + copied + size - 1))
                                        .build());
                parts.add(
                        Part.newBuilder()
                                .partNumber(number)
                                .eTag(result.copyPartResult().eTag())
                                .build());
                copied += size;
            }
            client.completeMultipartUpload(
                    CompleteMultipartUploadRequest.newBuilder()
                            .bucket(bucket)
                            .key(target)
                            .uploadId(uploadId)
                            .forbidOverwrite(
                                    Boolean.parseBoolean(headers.get("x-oss-forbid-overwrite")))
                            .completeMultipartUpload(
                                    CompleteMultipartUpload.newBuilder().parts(parts).build())
                            .build());
            uploadId = null;
        } catch (RuntimeException e) {
            if (uploadId != null) {
                try {
                    client.abortMultipartUpload(
                            AbortMultipartUploadRequest.newBuilder()
                                    .bucket(bucket)
                                    .key(target)
                                    .uploadId(uploadId)
                                    .build());
                } catch (RuntimeException abort) {
                    if (abort != e) {
                        e.addSuppressed(abort);
                    }
                }
            }
            throw OSSFileSystem.ioException(
                    "Failed to copy object range", new Path("oss://" + bucket + "/" + source), e);
        }
    }
}
