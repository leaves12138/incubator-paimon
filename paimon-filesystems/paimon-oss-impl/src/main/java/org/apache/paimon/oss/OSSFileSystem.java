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

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.CommonPrefix;
import com.aliyun.sdk.service.oss2.models.CopyObjectRequest;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsResult;
import com.aliyun.sdk.service.oss2.models.DeleteObject;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Result;
import com.aliyun.sdk.service.oss2.models.ObjectSummary;
import com.aliyun.sdk.service.oss2.models.PutBucketAclRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** The Hadoop FileSystem contract implemented with OSS SDK v2, without hadoop-aliyun. */
public class OSSFileSystem extends FileSystem {
    private URI uri;
    private URI endpoint;
    private String bucket;
    private Path workingDirectory;
    private OSSClient client;
    private OSSClient publicClient;
    private Map<String, String> writeHeaders;
    private ExecutorService uploadExecutor;
    private volatile boolean closed;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);
        checkArgument(
                "oss".equals(name.getScheme())
                        && name.getHost() != null
                        && name.getUserInfo() == null,
                "Expected an oss URI with a bucket and without credentials.");
        bucket = name.getHost();
        uri = URI.create("oss://" + bucket);
        workingDirectory =
                new Path("/user/" + System.getProperty("user.name"))
                        .makeQualified(uri, new Path("/"));
        Options options = new Options();
        conf.forEach(e -> options.set(e.getKey(), e.getValue()));
        writeHeaders = Collections.unmodifiableMap(OSSFileIO.writeHeaders(options));
        endpoint = OSSClientFactory.endpoint(conf);
        client = OSSClientFactory.create(conf, endpoint);
        String acl = conf.getTrimmed("fs.oss.acl.default", "");
        if (!acl.isEmpty()) {
            // This legacy setting applies to the bucket, not to individual objects.
            String normalized = acl.replace("-", "").toLowerCase(Locale.ROOT);
            if (normalized.equals("publicread")) {
                normalized = "public-read";
            }
            if (normalized.equals("publicreadwrite")) {
                normalized = "public-read-write";
            }
            checkArgument(
                    normalized.equals("private")
                            || normalized.equals("public-read")
                            || normalized.equals("public-read-write"),
                    "Invalid fs.oss.acl.default bucket ACL.");
            client().putBucketAcl(
                            PutBucketAclRequest.newBuilder()
                                    .bucket(bucket)
                                    .acl(normalized)
                                    .build());
        }
        uploadExecutor =
                BlockingThreadPoolExecutorService.newInstance(
                        Math.max(1, conf.getInt("fs.oss.multipart.download.threads", 10)),
                        Math.max(1, conf.getInt("fs.oss.max.total.tasks", 128)),
                        conf.getLong("fs.oss.threads.keepalivetime", 60),
                        TimeUnit.SECONDS,
                        "oss-transfer-shared");
    }

    @Override
    public String getScheme() {
        return "oss";
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    protected int getDefaultPort() {
        return -1;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    @Override
    public void setWorkingDirectory(Path path) {
        workingDirectory = makeQualified(path);
    }

    OSSClient client() {
        if (closed) {
            throw new IllegalStateException("OSS filesystem is closed.");
        }
        return client;
    }

    String bucket() {
        return bucket;
    }

    Map<String, String> writeHeaders() {
        return writeHeaders;
    }

    ExecutorService uploadExecutor() {
        return uploadExecutor;
    }

    URI publicEndpoint() throws IOException {
        String host = endpoint.getHost();
        String suffix = "-internal.aliyuncs.com";
        if (host.toLowerCase(Locale.ROOT).endsWith(suffix)) {
            host = host.substring(0, host.length() - suffix.length()) + ".aliyuncs.com";
        }
        try {
            return new URI(
                    endpoint.getScheme(),
                    null,
                    host,
                    endpoint.getPort(),
                    endpoint.getPath(),
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    synchronized OSSClient publicClient() throws IOException {
        OSSClient current = client();
        URI target = publicEndpoint();
        if (target.equals(endpoint)) {
            return current;
        }
        if (publicClient == null) {
            publicClient = OSSClientFactory.create(getConf(), target);
        }
        return publicClient;
    }

    String objectKey(Path path) {
        checkPath(path);
        return makeQualified(path).toUri().getPath().substring(1);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        FileStatus status = getFileStatus(path);
        if (status.isDirectory()) {
            throw new FileNotFoundException(path + " is a directory");
        }
        OSSRangeInputStream input =
                new OSSRangeInputStream(
                        client(),
                        bucket,
                        objectKey(path),
                        status.getLen(),
                        statistics,
                        getConf().getInt("fs.oss.multipart.download.size", 512 * 1024));
        return new FSDataInputStream(
                new FSInputStream() {
                    @Override
                    public void seek(long position) throws IOException {
                        input.seek(position);
                    }

                    @Override
                    public long getPos() throws IOException {
                        return input.getPos();
                    }

                    @Override
                    public boolean seekToNewSource(long position) {
                        return false;
                    }

                    @Override
                    public int read() throws IOException {
                        return input.read();
                    }

                    @Override
                    public int read(byte[] bytes, int offset, int length) throws IOException {
                        return input.read(bytes, offset, length);
                    }

                    @Override
                    public int read(long position, byte[] bytes, int offset, int length)
                            throws IOException {
                        return input.pread(position, bytes, offset, length);
                    }

                    @Override
                    public void close() throws IOException {
                        input.close();
                    }
                });
    }

    void checkCreate(Path path, boolean overwrite) throws IOException {
        checkArgument(!objectKey(path).isEmpty(), "Cannot create a file at the bucket root.");
        try {
            FileStatus status = getFileStatus(path);
            if (status.isDirectory() || !overwrite) {
                throw new FileAlreadyExistsException("Path already exists: " + path);
            }
        } catch (FileNotFoundException ignored) {
            // A new object is expected.
        }
        for (Path parent = makeQualified(path).getParent();
                parent != null;
                parent = parent.getParent()) {
            try {
                if (!getFileStatus(parent).isDirectory()) {
                    throw new FileAlreadyExistsException("Parent is a file: " + parent);
                }
                break;
            } catch (FileNotFoundException ignored) {
                // Parent directories are implicit in OSS.
            }
        }
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        checkCreate(path, overwrite);
        return new FSDataOutputStream(new OSSOutputStream(this, path, overwrite), statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) {
        throw new UnsupportedOperationException("OSS does not support filesystem append.");
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        String key = objectKey(path);
        Path qualified = makeQualified(path);
        if (key.isEmpty()) {
            return new FileStatus(0, true, 1, 0, 0, qualified);
        }
        try {
            HeadObjectResult head =
                    client().headObject(
                                    HeadObjectRequest.newBuilder().bucket(bucket).key(key).build());
            return new FileStatus(
                    head.contentLength(),
                    false,
                    1,
                    getConf().getLong("fs.oss.block.size", 64 * 1024 * 1024),
                    modificationTime(head.lastModified()),
                    qualified);
        } catch (RuntimeException e) {
            if (!missing(e)) {
                throw ioException("Failed to stat object", path, e);
            }
        }
        try {
            HeadObjectResult head =
                    client().headObject(
                                    HeadObjectRequest.newBuilder()
                                            .bucket(bucket)
                                            .key(key + "/")
                                            .build());
            return new FileStatus(0, true, 1, 0, modificationTime(head.lastModified()), qualified);
        } catch (RuntimeException e) {
            if (!missing(e)) {
                throw ioException("Failed to stat directory", path, e);
            }
        }
        try {
            ListObjectsV2Result page =
                    client().listObjectsV2(
                                    ListObjectsV2Request.newBuilder()
                                            .bucket(bucket)
                                            .prefix(key + "/")
                                            .maxKeys(1L)
                                            .build());
            if (page.contents() != null && !page.contents().isEmpty()) {
                return new FileStatus(0, true, 1, 0, 0, qualified);
            }
        } catch (RuntimeException e) {
            throw ioException("Failed to stat directory", path, e);
        }
        throw new FileNotFoundException(path.toString());
    }

    private static long modificationTime(String value) {
        return value == null
                ? 0
                : ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                        .toInstant()
                        .toEpochMilli();
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        FileStatus status = getFileStatus(path);
        if (status.isFile()) {
            return new FileStatus[] {status};
        }
        List<FileStatus> result = new ArrayList<>();
        RemoteIterator<FileStatus> files = listObjects(path, false, true);
        while (files.hasNext()) {
            result.add(files.next());
        }
        return result.toArray(new FileStatus[0]);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path path, boolean recursive)
            throws IOException {
        FileStatus status = getFileStatus(path);
        if (status.isFile()) {
            return new RemoteIterator<LocatedFileStatus>() {
                private boolean available = true;

                @Override
                public boolean hasNext() {
                    return available;
                }

                @Override
                public LocatedFileStatus next() {
                    if (!available) {
                        throw new NoSuchElementException();
                    }
                    available = false;
                    return new LocatedFileStatus(status, new BlockLocation[0]);
                }
            };
        }
        RemoteIterator<FileStatus> files = listObjects(path, recursive, false);
        return new RemoteIterator<LocatedFileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return files.hasNext();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                return new LocatedFileStatus(files.next(), new BlockLocation[0]);
            }
        };
    }

    private RemoteIterator<FileStatus> listObjects(
            Path path, boolean recursive, boolean includeDirectories) {
        String key = objectKey(path);
        String prefix = key.isEmpty() ? "" : key + "/";
        return new RemoteIterator<FileStatus>() {
            private final Queue<FileStatus> pending = new ArrayDeque<>();
            private String token;
            private boolean done;

            @Override
            public boolean hasNext() throws IOException {
                while (pending.isEmpty() && !done) {
                    ListObjectsV2Result page;
                    try {
                        ListObjectsV2Request.Builder request =
                                ListObjectsV2Request.newBuilder()
                                        .bucket(bucket)
                                        .prefix(prefix)
                                        .maxKeys(
                                                (long)
                                                        getConf()
                                                                .getInt(
                                                                        "fs.oss.paging.maximum",
                                                                        1000));
                        if (!recursive) {
                            request.delimiter("/");
                        }
                        if (token != null) {
                            request.continuationToken(token);
                        }
                        page = client().listObjectsV2(request.build());
                    } catch (RuntimeException e) {
                        throw ioException("Failed to list objects", path, e);
                    }
                    for (ObjectSummary object :
                            page.contents() == null
                                    ? Collections.<ObjectSummary>emptyList()
                                    : page.contents()) {
                        boolean dir = object.key().endsWith("/");
                        if (!object.key().equals(prefix) && (includeDirectories || !dir)) {
                            pending.add(
                                    new FileStatus(
                                            dir ? 0 : object.size(),
                                            dir,
                                            1,
                                            getConf()
                                                    .getLong("fs.oss.block.size", 64 * 1024 * 1024),
                                            object.lastModified() == null
                                                    ? 0
                                                    : object.lastModified().toEpochMilli(),
                                            new Path(uri + "/" + object.key())));
                        }
                    }
                    if (includeDirectories && page.commonPrefixes() != null) {
                        for (CommonPrefix dir : page.commonPrefixes()) {
                            pending.add(
                                    new FileStatus(
                                            0, true, 1, 0, 0, new Path(uri + "/" + dir.prefix())));
                        }
                    }
                    done = !Boolean.TRUE.equals(page.isTruncated());
                    String next = page.nextContinuationToken();
                    if (!done && (next == null || Objects.equals(token, next))) {
                        throw new IOException(
                                "OSS listing did not advance its continuation token.");
                    }
                    token = next;
                }
                return !pending.isEmpty();
            }

            @Override
            public FileStatus next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return pending.remove();
            }
        };
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        try {
            if (!getFileStatus(path).isDirectory()) {
                throw new FileAlreadyExistsException("Path is a file: " + path);
            }
            return true;
        } catch (FileNotFoundException ignored) {
            checkCreate(new Path(path, ".directory-check"), false);
        }
        String key = objectKey(path);
        if (key.isEmpty()) {
            return true;
        }
        try {
            client().putObject(
                            PutObjectRequest.newBuilder()
                                    .bucket(bucket)
                                    .key(key + "/")
                                    .headers(writeHeaders)
                                    .body(BinaryData.fromBytes(new byte[0]))
                                    .build());
            return true;
        } catch (RuntimeException e) {
            throw ioException("Failed to create directory", path, e);
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        String key = objectKey(path);
        if (key.isEmpty()) {
            return false;
        }
        FileStatus status;
        try {
            status = getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        }
        try {
            if (status.isFile()) {
                client().deleteObject(
                                DeleteObjectRequest.newBuilder().bucket(bucket).key(key).build());
            } else {
                if (!recursive && listStatus(path).length > 0) {
                    throw new IOException("Directory is not empty: " + path);
                }
                RemoteIterator<FileStatus> entries = listObjects(path, true, true);
                List<DeleteObject> batch = new ArrayList<>();
                while (entries.hasNext()) {
                    FileStatus entry = entries.next();
                    String child = objectKey(entry.getPath()) + (entry.isDirectory() ? "/" : "");
                    batch.add(DeleteObject.newBuilder().key(child).build());
                    if (batch.size() == 1000) {
                        deleteBatch(batch);
                        batch.clear();
                    }
                }
                batch.add(DeleteObject.newBuilder().key(key + "/").build());
                deleteBatch(batch);
            }
            // Preserve an empty parent directory after deleting its last child.
            Path parent = makeQualified(path).getParent();
            if (parent != null && !objectKey(parent).isEmpty()) {
                mkdirs(parent);
            }
            return true;
        } catch (RuntimeException e) {
            throw ioException("Failed to delete object", path, e);
        }
    }

    private void deleteBatch(List<DeleteObject> objects) throws IOException {
        if (objects.isEmpty()) {
            return;
        }
        DeleteMultipleObjectsResult result =
                client().deleteMultipleObjects(
                                DeleteMultipleObjectsRequest.newBuilder()
                                        .bucket(bucket)
                                        .deleteObjects(new ArrayList<>(objects))
                                        .quiet(false)
                                        .build());
        Set<String> deleted =
                result.deletedObjects() == null
                        ? Collections.emptySet()
                        : result.deletedObjects().stream()
                                .map(entry -> entry.key())
                                .collect(Collectors.toSet());
        if (objects.stream().anyMatch(object -> !deleted.contains(object.key()))) {
            throw new IOException("OSS did not acknowledge every object in the delete batch.");
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        URI targetUri = dst.toUri();
        if ((targetUri.getScheme() != null && !"oss".equals(targetUri.getScheme()))
                || (targetUri.getAuthority() != null && !bucket.equals(targetUri.getAuthority()))) {
            return false;
        }
        FileStatus source;
        try {
            source = getFileStatus(src);
        } catch (FileNotFoundException e) {
            return false;
        }
        String sourceKey = objectKey(src);
        if (sourceKey.isEmpty()) {
            return false;
        }
        if (makeQualified(src).equals(makeQualified(dst))) {
            return true;
        }
        Path target = dst;
        try {
            if (!getFileStatus(dst).isDirectory()) {
                return false;
            }
            target = new Path(dst, src.getName());
            if (exists(target)) {
                return false;
            }
        } catch (FileNotFoundException ignored) {
            // Destination is not present.
        }
        Path parent = makeQualified(target).getParent();
        if (parent == null || !exists(parent) || !getFileStatus(parent).isDirectory()) {
            return false;
        }
        String targetKey = objectKey(target);
        if (source.isDirectory() && targetKey.startsWith(sourceKey + "/")) {
            return false;
        }
        if (source.isFile()) {
            copy(sourceKey, targetKey, source.getLen());
        } else {
            mkdirs(target);
            RemoteIterator<FileStatus> entries = listObjects(src, true, true);
            while (entries.hasNext()) {
                FileStatus entry = entries.next();
                String child = objectKey(entry.getPath()) + (entry.isDirectory() ? "/" : "");
                copy(child, targetKey + child.substring(sourceKey.length()), entry.getLen());
            }
        }
        return delete(src, source.isDirectory());
    }

    void copy(String source, String target, long size) throws IOException {
        try {
            if (size <= 1024L * 1024 * 1024) {
                client().copyObject(
                                CopyObjectRequest.newBuilder()
                                        .bucket(bucket)
                                        .key(target)
                                        .sourceBucket(bucket)
                                        .sourceKey(source)
                                        .forbidOverwrite("true")
                                        .headers(writeHeaders)
                                        .build());
            } else {
                HeadObjectResult metadata =
                        client().headObject(
                                        HeadObjectRequest.newBuilder()
                                                .bucket(bucket)
                                                .key(source)
                                                .build());
                Map<String, String> headers = new HashMap<>(writeHeaders);
                for (String header :
                        new String[] {
                            "Content-Type",
                            "Content-Encoding",
                            "Content-Disposition",
                            "Cache-Control",
                            "Expires"
                        }) {
                    if (metadata.headers().containsKey(header)) {
                        headers.put(header, metadata.headers().get(header));
                    }
                }
                headers.put("x-oss-forbid-overwrite", "true");
                OSSMultiPartUpload.copyRange(
                        client(), bucket, source, target, 0, size, headers, metadata.metadata());
            }
        } catch (RuntimeException e) {
            throw ioException("Failed to copy object", new Path(uri + "/" + source), e);
        }
    }

    static boolean missing(Throwable failure) {
        ServiceException service = ServiceException.asCause(failure);
        return service != null
                && ("NoSuchKey".equals(service.errorCode())
                        || "NoSuchObject".equals(service.errorCode())
                        // HEAD errors may have no XML body. SDK v2 then reports BadErrorResponse
                        // instead of NoSuchKey; keep explicit bucket/permission errors distinct.
                        || (service.statusCode() == 404
                                && "BadErrorResponse".equals(service.errorCode())));
    }

    static IOException ioException(String message, Path path, Throwable failure) {
        ServiceException service = ServiceException.asCause(failure);
        if (service != null) {
            if (missing(service)) {
                FileNotFoundException missing = new FileNotFoundException(path.toString());
                missing.initCause(failure);
                return missing;
            }
            if ("FileAlreadyExists".equals(service.errorCode())) {
                return new FileAlreadyExistsException(path.toString());
            }
        }
        return new IOException(message + ": " + path, failure);
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            if (uploadExecutor != null) {
                uploadExecutor.shutdown();
            }
            IOUtils.closeQuietly(publicClient);
            IOUtils.closeQuietly(client);
            super.close();
        }
    }
}
