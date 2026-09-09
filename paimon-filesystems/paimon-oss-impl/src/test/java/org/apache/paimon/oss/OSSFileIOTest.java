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

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.credentials.Credentials;
import com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.CopyObjectRequest;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsResult;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.PresignResult;
import com.aliyun.sdk.service.oss2.models.PutBucketAclRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartCopyRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartRequest;
import com.aliyun.sdk.service.oss2.models.internal.DeleteResultXml;
import com.aliyun.sdk.service.oss2.signer.SigningContext;
import com.aliyun.sdk.service.oss2.transport.RequestMessage;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** SDK-v2 filesystem and Blob regression tests, without access to a remote OSS service. */
public class OSSFileIOTest {
    private static final Path ROOT = new Path("oss://bucket/table");

    @Test
    void testUploadBufferDirectoryFallback(@TempDir java.nio.file.Path directory) throws Exception {
        java.nio.file.Path unavailable = java.nio.file.Files.createFile(directory.resolve("file"));
        java.nio.file.Path available =
                java.nio.file.Files.createDirectory(directory.resolve("disk"));
        try (OSSV2TestFixture f =
                new OSSV2TestFixture(
                        Collections.singletonMap(
                                "fs.oss.buffer.dir", unavailable + "," + available))) {
            byte[] bytes = new byte[300_123];
            Arrays.fill(bytes, (byte) 91);
            try (PositionOutputStream out =
                    f.io.newOutputStream(new Path(ROOT, "buffers"), false)) {
                out.write(bytes);
            }
            assertThat(f.objects.get("table/buffers").bytes).containsExactly(bytes);
            try (java.util.stream.Stream<java.nio.file.Path> files =
                    java.nio.file.Files.list(available)) {
                assertThat(files).isEmpty();
            }
        }
    }

    @Test
    void testUploadBufferUsesHadoopTmpDir(@TempDir java.nio.file.Path directory) throws Exception {
        try (OSSV2TestFixture f =
                new OSSV2TestFixture(
                        Collections.singletonMap("hadoop.tmp.dir", directory.toString()))) {
            try (PositionOutputStream out = f.io.newOutputStream(new Path(ROOT, "buffer"), false)) {
                out.write(42);
                assertThat(java.nio.file.Files.isDirectory(directory.resolve("oss"))).isTrue();
            }
            try (java.util.stream.Stream<java.nio.file.Path> files =
                    java.nio.file.Files.list(directory.resolve("oss"))) {
                assertThat(files).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {102_399, 102_400, 102_401, 204_800, 204_801})
    void testMultipartBoundaries(int size) throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            byte[] bytes = new byte[size];
            new java.util.Random(1).nextBytes(bytes);
            try (PositionOutputStream out =
                    f.io.newOutputStream(new Path(ROOT, "boundary"), false)) {
                out.write(bytes);
                out.flush();
            }
            assertThat(f.objects.get("table/boundary").bytes).containsExactly(bytes);
            assertThat(f.uploads).isEmpty();
        }
    }

    @Test
    void testReadWriteStatusAndOverwrite() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            Path path = new Path(ROOT, "a file");
            byte[] data = new byte[300_123];
            Arrays.fill(data, (byte) 42);
            try (PositionOutputStream out = f.io.newOutputStream(path, false)) {
                out.write(data);
                assertThat(out.getPos()).isEqualTo(data.length);
            }
            assertThat(f.io.getFileStatus(path).getLen()).isEqualTo(data.length);
            byte[] actual = new byte[data.length];
            try (SeekableInputStream in = f.io.newInputStream(path)) {
                IOUtils.readFully(in, actual, 0, actual.length);
                assertThat(in.read()).isEqualTo(-1);
            }
            assertThat(actual).containsExactly(data);
            assertThatThrownBy(() -> f.io.newOutputStream(path, false))
                    .isInstanceOf(org.apache.hadoop.fs.FileAlreadyExistsException.class);
            try (PositionOutputStream out = f.io.newOutputStream(path, true)) {
                out.write(9);
            }
            assertThat(f.objects.get("table/a file").bytes).containsExactly((byte) 9);
            assertThat(f.uploads).isEmpty();
        }
    }

    @Test
    void testEmptyFileAndCloseSemantics() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            Path path = new Path(ROOT, "empty");
            PositionOutputStream out = f.io.newOutputStream(path, false);
            out.close();
            out.close();
            assertThatThrownBy(() -> out.write(1)).isInstanceOf(java.io.IOException.class);
            assertThat(f.io.getFileStatus(path).isDir()).isFalse();
            try (SeekableInputStream input = f.io.newInputStream(path)) {
                assertThat(input.read()).isEqualTo(-1);
            }
            assertThat(f.uploads).isEmpty();
        }
    }

    @Test
    void testAtomicWriteNeverOverwrites() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            Path path = new Path(ROOT, "snapshot");
            assertThat(f.io.tryToWriteAtomic(path, "first")).isTrue();
            assertThat(f.io.tryToWriteAtomic(path, "second")).isFalse();
            assertThat(f.objects.get("table/snapshot").bytes)
                    .containsExactly("first".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void testUploadBodyCanReplayAfterEntityClose(@TempDir java.nio.file.Path directory)
            throws Exception {
        byte[] expected = new byte[] {1, 2, 3, 4};
        java.nio.file.Path file = java.nio.file.Files.write(directory.resolve("part"), expected);
        try (java.io.FileInputStream input = new java.io.FileInputStream(file.toFile())) {
            com.aliyun.sdk.service.oss2.transport.BinaryData body =
                    OSSMultiPartUpload.fileBody(input, expected.length);
            assertThat(body.isReplayable()).isTrue();
            java.io.InputStream first = body.toStream();
            assertThat(first.read()).isEqualTo(1);
            first.close();
            byte[] actual = new byte[expected.length];
            IOUtils.readFully(body.toStream(), actual, 0, actual.length);
            assertThat(actual).containsExactly(expected);
            assertThat(input.getChannel().isOpen()).isTrue();
        }
    }

    @Test
    void testSdkV2SendsSignedStsAndSseHeaders() throws Exception {
        List<com.aliyun.sdk.service.oss2.transport.RequestMessage> requests = new ArrayList<>();
        com.aliyun.sdk.service.oss2.transport.HttpClient transport =
                new com.aliyun.sdk.service.oss2.transport.HttpClient() {
                    @Override
                    public com.aliyun.sdk.service.oss2.transport.ResponseMessage send(
                            com.aliyun.sdk.service.oss2.transport.RequestMessage request,
                            com.aliyun.sdk.service.oss2.transport.RequestContext context) {
                        requests.add(request);
                        String xml = "<CopyObjectResult><ETag>tag</ETag></CopyObjectResult>";
                        return com.aliyun.sdk.service.oss2.transport.ResponseMessage.newBuilder()
                                .request(request)
                                .statusCode(200)
                                .headers(Collections.singletonMap("x-oss-request-id", "test"))
                                .body(
                                        com.aliyun.sdk.service.oss2.transport.BinaryData.fromString(
                                                xml))
                                .build();
                    }
                };
        try (OSSClient client =
                OSSClient.newBuilder()
                        .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                        .credentialsProvider(
                                new com.aliyun.sdk.service.oss2.credentials
                                        .StaticCredentialsProvider(
                                        "test-key", "test-secret", "test-token"))
                        .signer(OSSClientFactory.v1Signer())
                        .httpClient(transport)
                        .build()) {
            Options options = new Options();
            options.set("fs.oss.server-side-encryption", "KMS");
            options.set("fs.oss.server-side-encryption-key-id", "cmk");
            client.copyObject(
                    CopyObjectRequest.newBuilder()
                            .bucket("bucket")
                            .key("target")
                            .sourceBucket("bucket")
                            .sourceKey("source")
                            .headers(OSSFileIO.writeHeaders(options))
                            .build());
        }
        assertThat(requests).hasSize(1);
        assertThat(requests.get(0).headers())
                .containsEntry("x-oss-security-token", "test-token")
                .containsEntry("x-oss-server-side-encryption", "KMS")
                .containsEntry("x-oss-server-side-encryption-key-id", "cmk")
                .doesNotContainKey("security-token")
                .doesNotContainKey("x-oss-metadata-directive");
    }

    @Test
    void testLegacyAclSettingIsAppliedToBucketOnly() throws Exception {
        try (OSSV2TestFixture f =
                new OSSV2TestFixture(Collections.singletonMap("fs.oss.acl.default", "Private"))) {
            ArgumentCaptor<PutBucketAclRequest> request =
                    ArgumentCaptor.forClass(PutBucketAclRequest.class);
            verify(f.client).putBucketAcl(request.capture());
            assertThat(request.getValue().acl()).isEqualTo("private");
            assertThat(f.fs.writeHeaders()).doesNotContainKey("x-oss-object-acl");
        }
    }

    @Test
    void testMultipartFailureAbortsWithoutReplacingTarget() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            f.put("table/a", new byte[] {7});
            when(f.client.uploadPart(any(UploadPartRequest.class)))
                    .thenThrow(new IllegalStateException("part failure"));
            PositionOutputStream out = f.io.newOutputStream(new Path(ROOT, "a"), true);
            assertThatThrownBy(
                            () -> {
                                out.write(new byte[200_000]);
                                out.close();
                            })
                    .isInstanceOf(java.io.IOException.class);
            try {
                out.close();
            } catch (java.io.IOException ignored) {
            }
            assertThat(f.objects.get("table/a").bytes).containsExactly((byte) 7);
            assertThat(f.uploads).isEmpty();
            verify(f.client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3, 11 * 1024 * 1024})
    void testTwoPhaseCommitStateIsSerializable(int length) throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            Path path = new Path(ROOT, "two-phase");
            TwoPhaseOutputStream out = f.io.newTwoPhaseOutputStream(path, false);
            out.write(new byte[length]);
            TwoPhaseOutputStream.Committer committer = out.closeForCommit();
            assertThat(f.io.exists(path)).isFalse();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream object = new ObjectOutputStream(bytes)) {
                object.writeObject(committer);
            }
            try (ObjectInputStream object =
                    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                committer = (TwoPhaseOutputStream.Committer) object.readObject();
            }
            committer.commit(f.io);
            assertThat(f.io.getFileStatus(path).getLen()).isEqualTo(length);
            committer.discardStaging(f.io);
            assertThat(f.io.exists(path)).isTrue();
            assertThat(f.uploads).isEmpty();
        }
    }

    @Test
    void testDirectoryListingPaginationAndDeleteBoundaries() throws Exception {
        try (OSSV2TestFixture f =
                new OSSV2TestFixture(Collections.singletonMap("fs.oss.paging.maximum", "2"))) {
            f.put("table/a", new byte[] {1});
            f.put("table/b", new byte[] {2});
            f.put("table/sub/c", new byte[] {3});
            f.put("table/sub/d", new byte[] {4});
            f.put("table-other/keep", new byte[] {5});
            assertThat(f.io.getFileStatus(ROOT).isDir()).isTrue();
            assertThat(f.io.listStatus(ROOT)).hasSize(3);
            List<FileStatus> files = new ArrayList<>();
            RemoteIterator<FileStatus> iterator = f.io.listFilesIterative(ROOT, true);
            while (iterator.hasNext()) {
                files.add(iterator.next());
            }
            assertThat(files).hasSize(4).allMatch(status -> !status.isDir());
            assertThat(f.io.listFiles(ROOT, false)).hasSize(2);
            assertThatThrownBy(() -> f.io.delete(ROOT, false))
                    .isInstanceOf(java.io.IOException.class);
            assertThat(f.io.delete(ROOT, true)).isTrue();
            assertThat(f.io.exists(ROOT)).isFalse();
            assertThat(f.objects).containsKey("table-other/keep");
            assertThat(f.io.delete(new Path("oss://bucket/"), true)).isFalse();
            assertThat(f.io.delete(ROOT, true)).isFalse();
        }
    }

    @Test
    void testMkdirsAndRename() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            assertThat(f.io.mkdirs(new Path(ROOT, "empty"))).isTrue();
            assertThat(f.io.listStatus(new Path(ROOT, "empty"))).isEmpty();
            f.put("table/src/a", new byte[] {1});
            f.put("table/src/sub/b", new byte[] {2});
            assertThat(f.io.rename(new Path(ROOT, "src"), new Path(ROOT, "dst"))).isTrue();
            assertThat(f.objects).containsKeys("table/dst/a", "table/dst/sub/b");
            assertThat(f.io.exists(new Path(ROOT, "src"))).isFalse();
            assertThat(f.io.rename(new Path(ROOT, "dst"), new Path(ROOT, "dst/sub/new"))).isFalse();
            assertThat(f.io.rename(new Path(ROOT, "dst/a"), new Path(ROOT, "dst/sub/b"))).isFalse();
            assertThat(f.io.rename(new Path(ROOT, "dst/a"), new Path("oss://other/a"))).isFalse();
            assertThatThrownBy(() -> f.io.mkdirs(new Path(ROOT, "dst/a/child")))
                    .isInstanceOf(java.io.IOException.class);
            assertThatThrownBy(() -> f.io.newInputStream(new Path(ROOT, "missing")))
                    .isInstanceOf(FileNotFoundException.class);
        }
    }

    @Test
    void testPartialDeleteDoesNotPretendSuccess() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            f.put("table/a", new byte[] {1});
            DeleteResultXml xml = new DeleteResultXml();
            xml.deleted = Collections.emptyList();
            when(f.client.deleteMultipleObjects(any(DeleteMultipleObjectsRequest.class)))
                    .thenReturn(DeleteMultipleObjectsResult.newBuilder().innerBody(xml).build());
            assertThatThrownBy(() -> f.io.delete(ROOT, true))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("acknowledge");
            assertThat(f.objects).containsKey("table/a");
        }
    }

    @Test
    void testCopyFailureKeepsSource() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            f.put("table/src/a", new byte[] {1});
            when(f.client.copyObject(any(CopyObjectRequest.class)))
                    .thenThrow(new IllegalStateException("copy failed"));
            assertThatThrownBy(() -> f.io.rename(new Path(ROOT, "src"), new Path(ROOT, "dst")))
                    .isInstanceOf(java.io.IOException.class);
            assertThat(f.objects).containsKey("table/src/a");
        }
    }

    @Test
    void testSseStampsEveryWritePathAndPreservesCopyMetadata() throws Exception {
        Map<String, String> conf = new HashMap<>();
        conf.put("fs.oss.server-side-encryption", "kms");
        conf.put("fs.oss.server-side-encryption-key-id", "my-cmk");
        conf.put("fs.oss.server-side-data-encryption", "sm4");
        try (OSSV2TestFixture f = new OSSV2TestFixture(conf)) {
            assertThat(f.io.tryToWriteAtomic(new Path(ROOT, "atomic"), "x")).isTrue();
            try (PositionOutputStream out = f.io.newOutputStream(new Path(ROOT, "file"), false)) {
                out.write(new byte[200_000]);
            }
            f.io.mkdirs(new Path(ROOT, "directory"));
            assertThat(f.io.rename(new Path(ROOT, "file"), new Path(ROOT, "renamed"))).isTrue();
            for (String key : Arrays.asList("table/atomic", "table/renamed", "table/directory/")) {
                assertThat(f.objects.get(key).headers)
                        .containsEntry("x-oss-server-side-encryption", "KMS")
                        .containsEntry("x-oss-server-side-encryption-key-id", "my-cmk")
                        .containsEntry("x-oss-server-side-data-encryption", "SM4");
            }
            ArgumentCaptor<CopyObjectRequest> request =
                    ArgumentCaptor.forClass(CopyObjectRequest.class);
            verify(f.client).copyObject(request.capture());
            assertThat(request.getValue().metadataDirective()).isNull();
        }
        Options options = new Options();
        options.set("fs.oss.server-side-encryption-algorithm", "AES256");
        assertThat(OSSFileIO.writeHeaders(options))
                .containsEntry("x-oss-server-side-encryption", "AES256");
    }

    @Test
    void testCreateBlobPresignedUrlMaterializesSinglePart() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};
            f.put("table/data/file", bytes);
            BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 2, 4);
            String url = f.io.createBlobPresignedUrl(ROOT, descriptor, Duration.ofHours(1));
            String key = URI.create(url).getPath().substring(1);
            assertThat(f.objects.get(key).bytes)
                    .containsExactly((byte) 2, (byte) 3, (byte) 4, (byte) 5);
            assertThat(f.objects.get(key).headers)
                    .containsEntry("Content-Type", "application/octet-stream")
                    .containsEntry(
                            "x-oss-meta-paimon-blob-descriptor-sha256", fingerprint(descriptor));
            assertThat(f.io.createBlobPresignedUrl(ROOT, descriptor, Duration.ofHours(1)))
                    .isEqualTo(url);
            verify(f.client).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
            assertThat(f.uploads).isEmpty();
        }
    }

    @Test
    void testCreateBlobPresignedUrlCacheHitOnlyPresigns() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 0, 3);
            String key = "table/data/_bloburl_" + fingerprint(descriptor);
            f.objects.put(key, new OSSV2TestFixture.Stored(new byte[3], blobHeaders(descriptor)));
            f.io.createBlobPresignedUrl(ROOT, descriptor, Duration.ofMinutes(5));
            verify(f.client, never())
                    .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
            verify(f.client, never()).putObject(any(PutObjectRequest.class));
        }
    }

    @Test
    void testCreateBlobPresignedUrlUsesPublicEndpointWithV4Signing() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("fs.oss.endpoint", "https://oss-cn-hangzhou-internal.aliyuncs.com");
        config.put("fs.oss.signature.version", "v4");
        try (OSSV2TestFixture f = new OSSV2TestFixture(config)) {
            BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 0, 0);
            f.objects.put(
                    "table/data/_bloburl_" + fingerprint(descriptor),
                    new OSSV2TestFixture.Stored(new byte[0], blobHeaders(descriptor)));
            URI url =
                    URI.create(
                            f.io.createBlobPresignedUrl(ROOT, descriptor, Duration.ofMinutes(5)));
            assertThat(url.getHost()).isEqualTo("bucket.oss-cn-hangzhou.aliyuncs.com");
            assertThat(url.getQuery()).contains("OSS4-HMAC-SHA256");
            assertThat(url.getPath()).endsWith("_bloburl_" + fingerprint(descriptor));
        }
    }

    @Test
    void testCreateBlobPresignedUrlControlsMultipartCount() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            long length = 100L * 1024 * 1024 * 10_000 + 1;
            BlobDescriptor descriptor =
                    new BlobDescriptor("oss://bucket/table/data/file", 1, length);
            AtomicInteger targetHeads = new AtomicInteger();
            when(f.client.headObject(any(HeadObjectRequest.class)))
                    .thenAnswer(
                            call -> {
                                HeadObjectRequest request = call.getArgument(0);
                                if (request.key().contains("_bloburl_")) {
                                    if (targetHeads.getAndIncrement() == 0) {
                                        throw OSSV2TestFixture.missing();
                                    }
                                    return OSSV2TestFixture.head(length, blobHeaders(descriptor));
                                }
                                return OSSV2TestFixture.head(length + 1, Collections.emptyMap());
                            });
            when(f.client.uploadPartCopy(any(UploadPartCopyRequest.class)))
                    .thenReturn(OSSV2TestFixture.copied("tag"));
            when(f.client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                    .thenReturn(CompleteMultipartUploadResult.newBuilder().build());
            f.io.createBlobPresignedUrl(ROOT, descriptor, Duration.ofMinutes(5));
            ArgumentCaptor<CompleteMultipartUploadRequest> complete =
                    ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
            verify(f.client).completeMultipartUpload(complete.capture());
            assertThat(complete.getValue().completeMultipartUpload().parts())
                    .hasSizeLessThanOrEqualTo(10_000);
            ArgumentCaptor<UploadPartCopyRequest> copied =
                    ArgumentCaptor.forClass(UploadPartCopyRequest.class);
            verify(f.client, org.mockito.Mockito.atLeastOnce()).uploadPartCopy(copied.capture());
            long sum = 0;
            for (UploadPartCopyRequest request : copied.getAllValues()) {
                String[] range = request.copySourceRange().substring(6).split("-");
                long from = Long.parseLong(range[0]), to = Long.parseLong(range[1]);
                assertThat(from).isEqualTo(1 + sum);
                sum += to - from + 1;
            }
            assertThat(sum).isEqualTo(length);
        }
    }

    @Test
    void testCreateBlobPresignedUrlHandlesZeroLength() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            f.put("table/data/file", new byte[] {1});
            BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 1, 0);
            String key =
                    URI.create(f.io.createBlobPresignedUrl(ROOT, descriptor, Duration.ofHours(1)))
                            .getPath()
                            .substring(1);
            assertThat(f.objects.get(key).bytes).isEmpty();
            verify(f.client, never())
                    .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
        }
    }

    @Test
    void testCreateBlobPresignedUrlAbortsFailedUpload() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            f.put("table/data/file", new byte[10]);
            when(f.client.uploadPartCopy(any(UploadPartCopyRequest.class)))
                    .thenThrow(new IllegalStateException("copy failure"));
            assertThatThrownBy(
                            () ->
                                    f.io.createBlobPresignedUrl(
                                            ROOT,
                                            new BlobDescriptor(
                                                    "oss://bucket/table/data/file", 0, 3),
                                            Duration.ofHours(1)))
                    .isInstanceOf(java.io.IOException.class);
            assertThat(f.uploads).isEmpty();
            verify(f.client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        }
    }

    @Test
    void testCreateBlobPresignedUrlRejectsFinalHeadMismatch() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            f.put("table/data/file", new byte[10]);
            AtomicInteger heads = new AtomicInteger();
            when(f.client.headObject(any(HeadObjectRequest.class)))
                    .thenAnswer(
                            call -> {
                                HeadObjectRequest request = call.getArgument(0);
                                if (request.key().contains("_bloburl_")) {
                                    if (heads.getAndIncrement() == 0) {
                                        throw OSSV2TestFixture.missing();
                                    }
                                    return OSSV2TestFixture.head(99, Collections.emptyMap());
                                }
                                return OSSV2TestFixture.head(10, Collections.emptyMap());
                            });
            assertThatThrownBy(
                            () ->
                                    f.io.createBlobPresignedUrl(
                                            ROOT,
                                            new BlobDescriptor(
                                                    "oss://bucket/table/data/file", 0, 3),
                                            Duration.ofHours(1)))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("does not match");
            verify(f.client, never())
                    .presign(any(GetObjectRequest.class), any(PresignOptions.class));
        }
    }

    @Test
    void testCreateBlobPresignedUrlRejectsInvalidArgumentsAndRange() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            f.put("table/data/file", new byte[10]);
            BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 0, 3);
            for (Duration validity :
                    Arrays.asList(
                            null, Duration.ZERO, Duration.ofSeconds(-1), Duration.ofMillis(1))) {
                assertThatThrownBy(() -> f.io.createBlobPresignedUrl(ROOT, descriptor, validity))
                        .isInstanceOf(java.io.IOException.class);
            }
            assertThatThrownBy(
                            () ->
                                    f.io.createBlobPresignedUrl(
                                            ROOT,
                                            new BlobDescriptor(
                                                    "oss://bucket/table-other/data", 0, 1),
                                            Duration.ofHours(1)))
                    .isInstanceOf(java.io.IOException.class);
            assertThatThrownBy(
                            () ->
                                    f.io.createBlobPresignedUrl(
                                            ROOT,
                                            new BlobDescriptor(
                                                    "oss://bucket/table/data/file", 9, 2),
                                            Duration.ofHours(1)))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("outside");
        }
    }

    @Test
    void testCreateBlobPresignedUrlRejectsInvalidTarget() throws Exception {
        try (OSSV2TestFixture f = new OSSV2TestFixture()) {
            BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 0, 3);
            f.objects.put(
                    "table/data/_bloburl_" + fingerprint(descriptor),
                    new OSSV2TestFixture.Stored(new byte[3], blobHeaders(descriptor)));
            when(f.client.presign(any(GetObjectRequest.class), any(PresignOptions.class)))
                    .thenReturn(
                            PresignResult.newBuilder().url("https://other.example/wrong").build());
            assertThatThrownBy(
                            () ->
                                    f.io.createBlobPresignedUrl(
                                            ROOT, descriptor, Duration.ofHours(1)))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("invalid target");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCnameAndSignatureConfiguration(boolean cname) throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("fs.oss.accessKeyId", "test-key");
        conf.set("fs.oss.accessKeySecret", "test-secret");
        conf.setBoolean("fs.oss.cname.enabled", cname);
        try (OSSClient client =
                OSSClientFactory.create(conf, URI.create("https://custom.example"))) {
            URI url =
                    URI.create(
                            client.presign(
                                            GetObjectRequest.newBuilder()
                                                    .bucket("bucket")
                                                    .key("a b")
                                                    .build())
                                    .url());
            assertThat(url.getHost()).isEqualTo(cname ? "custom.example" : "bucket.custom.example");
            assertThat(url.getPath()).isEqualTo("/a b");
        }
        try (OSSClient client =
                OSSClientFactory.create(conf, URI.create("https://oss-cn-hangzhou.aliyuncs.com"))) {
            assertThat(
                            URI.create(
                                            client.presign(
                                                            GetObjectRequest.newBuilder()
                                                                    .bucket("bucket")
                                                                    .key("file")
                                                                    .build())
                                                    .url())
                                    .getHost())
                    .isEqualTo("bucket.oss-cn-hangzhou.aliyuncs.com");
        }
    }

    @Test
    void testV1StsTokenUsesSignedOssHeader() throws Exception {
        SigningContext context = new SigningContext();
        context.setCredentials(new Credentials("test-key", "test-secret", "test-token"));
        context.setBucket("bucket");
        context.setKey("file");
        context.setRequest(
                RequestMessage.newBuilder()
                        .method("GET")
                        .uri(URI.create("https://bucket.oss-cn-hangzhou.aliyuncs.com/file"))
                        .headers(new HashMap<>())
                        .build());
        OSSClientFactory.v1Signer().sign(context);
        assertThat(context.getRequest().headers())
                .containsEntry("x-oss-security-token", "test-token")
                .doesNotContainKey("security-token");
        assertThat(context.getStringToSign()).contains("x-oss-security-token:test-token");
        context.setCredentials(new Credentials("test-key", "test-secret"));
        OSSClientFactory.v1Signer().sign(context);
        assertThat(context.getRequest().headers()).doesNotContainKey("x-oss-security-token");
    }

    private static Map<String, String> blobHeaders(BlobDescriptor descriptor) throws Exception {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("Content-Type", "application/octet-stream");
        metadata.put("x-oss-meta-paimon-blob-descriptor-sha256", fingerprint(descriptor));
        return metadata;
    }

    private static String fingerprint(BlobDescriptor descriptor) throws Exception {
        StringBuilder result = new StringBuilder();
        for (byte value : MessageDigest.getInstance("SHA-256").digest(descriptor.serialize())) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }

    @Test
    public void testResolveSse() {
        // Nothing set -> no SSE.
        assertThat(OSSFileIO.resolveSse(null, null, null)).isNull();

        // Method only (no key-id / data-encryption).
        OSSFileIO.SseConfig aes = OSSFileIO.resolveSse("AES256", null, null);
        assertThat(aes.method).isEqualTo("AES256");
        assertThat(aes.keyId).isNull();
        assertThat(aes.dataEnc).isNull();

        // A key id / SM4 data-encryption each default the method to KMS.
        assertThat(OSSFileIO.resolveSse(null, "my-cmk", null).method).isEqualTo("KMS");
        OSSFileIO.SseConfig dataOnly = OSSFileIO.resolveSse(null, null, "SM4");
        assertThat(dataOnly.method).isEqualTo("KMS");
        assertThat(dataOnly.dataEnc).isEqualTo("SM4");
        OSSFileIO.SseConfig full = OSSFileIO.resolveSse("KMS", "my-cmk", "SM4");
        assertThat(full.method).isEqualTo("KMS");
        assertThat(full.keyId).isEqualTo("my-cmk");
        assertThat(full.dataEnc).isEqualTo("SM4");

        // The method is canonicalized to the exact OSS value.
        assertThat(OSSFileIO.resolveSse("kms", "my-cmk", "sm4").method).isEqualTo("KMS");
        assertThat(OSSFileIO.resolveSse("kms", "my-cmk", "sm4").dataEnc).isEqualTo("SM4");

        // Invalid / conflicting config is rejected fail-fast.
        assertThatThrownBy(() -> OSSFileIO.resolveSse("AES-256", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AES256/KMS/SM4");
        assertThatThrownBy(() -> OSSFileIO.resolveSse("AES256", "my-cmk", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires");
        assertThatThrownBy(() -> OSSFileIO.resolveSse("AES256", null, "SM4"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires");
        assertThatThrownBy(() -> OSSFileIO.resolveSse("KMS", "my-cmk", "AES256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SM4");
        assertThatThrownBy(() -> OSSFileIO.resolveSse("KMS", "bad key", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whitespace");

        // A present-but-blank value is a misconfiguration, not "unset".
        assertThatThrownBy(() -> OSSFileIO.resolveSse(null, "  ", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
        assertThatThrownBy(() -> OSSFileIO.resolveSse(" ", "", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
    }
}
