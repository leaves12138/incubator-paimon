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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.AbortMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.AbortMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.CommonPrefix;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.CopyObjectRequest;
import com.aliyun.sdk.service.oss2.models.CopyObjectResult;
import com.aliyun.sdk.service.oss2.models.CopyPartResult;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsRequest;
import com.aliyun.sdk.service.oss2.models.DeleteMultipleObjectsResult;
import com.aliyun.sdk.service.oss2.models.DeleteObjectRequest;
import com.aliyun.sdk.service.oss2.models.DeleteObjectResult;
import com.aliyun.sdk.service.oss2.models.DeletedInfo;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.GetObjectResult;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUpload;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadRequest;
import com.aliyun.sdk.service.oss2.models.InitiateMultipartUploadResult;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Request;
import com.aliyun.sdk.service.oss2.models.ListObjectsV2Result;
import com.aliyun.sdk.service.oss2.models.ObjectSummary;
import com.aliyun.sdk.service.oss2.models.Part;
import com.aliyun.sdk.service.oss2.models.PresignResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.models.PutObjectResult;
import com.aliyun.sdk.service.oss2.models.UploadPartCopyRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartCopyResult;
import com.aliyun.sdk.service.oss2.models.UploadPartRequest;
import com.aliyun.sdk.service.oss2.models.UploadPartResult;
import com.aliyun.sdk.service.oss2.models.internal.DeleteResultXml;
import com.aliyun.sdk.service.oss2.models.internal.ListBucketV2ResultXml;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** In-memory service fixture. No cloud requests or cloud mutations are performed. */
final class OSSV2TestFixture implements AutoCloseable {
    final OSSClient client = mock(OSSClient.class);
    final Map<String, Stored> objects = new ConcurrentSkipListMap<>();
    final Map<String, Upload> uploads = new ConcurrentHashMap<>();
    final OSSFileSystem fs;
    final OSSFileIO io;
    final Configuration conf;

    OSSV2TestFixture() throws Exception {
        this(Collections.emptyMap());
    }

    OSSV2TestFixture(Map<String, String> overrides) throws Exception {
        conf = new Configuration(false);
        conf.set("fs.oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com");
        conf.set("fs.oss.region", "cn-hangzhou");
        conf.set("fs.oss.accessKeyId", "test-access-key");
        conf.set("fs.oss.accessKeySecret", "test-secret");
        conf.set("fs.oss.multipart.upload.size", "102400");
        overrides.forEach(conf::set);
        fs = spy(new OSSFileSystem());
        doReturn(client).when(fs).client();
        fs.initialize(URI.create("oss://bucket"), conf);
        io =
                new OSSFileIO() {
                    @Override
                    protected OSSFileSystem createFileSystem(Path path) {
                        return fs;
                    }
                };
        Options options = new Options();
        conf.forEach(e -> options.set(e.getKey(), e.getValue()));
        io.configure(CatalogContext.create(options));

        when(client.headObject(any(HeadObjectRequest.class)))
                .thenAnswer(
                        call -> {
                            String key = ((HeadObjectRequest) call.getArgument(0)).key();
                            Stored object = objects.get(key);
                            if (object == null) {
                                throw missing();
                            }
                            return head(object.bytes.length, object.headers);
                        });
        when(client.getObject(any(GetObjectRequest.class)))
                .thenAnswer(
                        call -> {
                            GetObjectRequest request = call.getArgument(0);
                            Stored object = objects.get(request.key());
                            if (object == null) {
                                throw missing();
                            }
                            int start = 0, end = object.bytes.length;
                            if (request.range() != null) {
                                String[] limits = request.range().substring(6).split("-");
                                start = Integer.parseInt(limits[0]);
                                end = Math.min(end, Integer.parseInt(limits[1]) + 1);
                            }
                            return GetObjectResult.newBuilder()
                                    .innerBody(
                                            new ByteArrayInputStream(
                                                    Arrays.copyOfRange(object.bytes, start, end)))
                                    .build();
                        });
        when(client.putObject(any(PutObjectRequest.class)))
                .thenAnswer(
                        call -> {
                            PutObjectRequest request = call.getArgument(0);
                            Stored object = new Stored(request.body().toBytes(), request.headers());
                            if (Boolean.TRUE.equals(request.forbidOverwrite())) {
                                if (objects.putIfAbsent(request.key(), object) != null) {
                                    throw exists();
                                }
                            } else {
                                objects.put(request.key(), object);
                            }
                            return PutObjectResult.newBuilder().build();
                        });
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenAnswer(
                        call -> {
                            ListObjectsV2Request request = call.getArgument(0);
                            String prefix = request.prefix() == null ? "" : request.prefix();
                            TreeMap<String, Stored> entries = new TreeMap<>();
                            for (Map.Entry<String, Stored> entry : objects.entrySet()) {
                                if (!entry.getKey().startsWith(prefix)) {
                                    continue;
                                }
                                String rest = entry.getKey().substring(prefix.length());
                                int slash = rest.indexOf('/');
                                if ("/".equals(request.delimiter()) && slash >= 0) {
                                    entries.put(prefix + rest.substring(0, slash + 1), null);
                                } else {
                                    entries.put(entry.getKey(), entry.getValue());
                                }
                            }
                            if (request.continuationToken() != null) {
                                entries =
                                        new TreeMap<>(
                                                entries.tailMap(
                                                        request.continuationToken(), false));
                            }
                            int maximum =
                                    request.maxKeys() == null ? 1000 : request.maxKeys().intValue();
                            ListBucketV2ResultXml xml = new ListBucketV2ResultXml();
                            xml.contents = new ArrayList<>();
                            xml.commonPrefixes = new ArrayList<>();
                            xml.isTruncated = entries.size() > maximum;
                            int count = 0;
                            for (Map.Entry<String, Stored> entry : entries.entrySet()) {
                                if (count++ == maximum) {
                                    break;
                                }
                                if (entry.getValue() == null) {
                                    xml.commonPrefixes.add(
                                            CommonPrefix.newBuilder()
                                                    .prefix(entry.getKey())
                                                    .build());
                                } else {
                                    xml.contents.add(
                                            ObjectSummary.newBuilder()
                                                    .key(entry.getKey())
                                                    .size((long) entry.getValue().bytes.length)
                                                    .lastModified(
                                                            Instant.parse("2026-09-09T00:00:00Z"))
                                                    .build());
                                }
                                xml.nextContinuationToken = entry.getKey();
                            }
                            if (!xml.isTruncated) {
                                xml.nextContinuationToken = null;
                            }
                            return ListObjectsV2Result.newBuilder().innerBody(xml).build();
                        });
        when(client.deleteObject(any(DeleteObjectRequest.class)))
                .thenAnswer(
                        call -> {
                            objects.remove(((DeleteObjectRequest) call.getArgument(0)).key());
                            return DeleteObjectResult.newBuilder().build();
                        });
        when(client.deleteMultipleObjects(any(DeleteMultipleObjectsRequest.class)))
                .thenAnswer(
                        call -> {
                            DeleteMultipleObjectsRequest request = call.getArgument(0);
                            DeleteResultXml xml = new DeleteResultXml();
                            xml.deleted =
                                    request.deleteObjects().stream()
                                            .map(
                                                    object -> {
                                                        objects.remove(object.key());
                                                        return DeletedInfo.newBuilder()
                                                                .key(object.key())
                                                                .build();
                                                    })
                                            .collect(Collectors.toList());
                            return DeleteMultipleObjectsResult.newBuilder().innerBody(xml).build();
                        });
        when(client.copyObject(any(CopyObjectRequest.class)))
                .thenAnswer(
                        call -> {
                            CopyObjectRequest request = call.getArgument(0);
                            Stored source = objects.get(request.sourceKey());
                            if (source == null) {
                                throw missing();
                            }
                            Map<String, String> headers = new HashMap<>(source.headers);
                            headers.putAll(request.headers());
                            if (objects.putIfAbsent(
                                            request.key(), new Stored(source.bytes, headers))
                                    != null) {
                                throw exists();
                            }
                            return CopyObjectResult.newBuilder().build();
                        });
        when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
                .thenAnswer(
                        call -> {
                            InitiateMultipartUploadRequest request = call.getArgument(0);
                            String id = UUID.randomUUID().toString();
                            uploads.put(id, new Upload(request.key(), request.headers()));
                            return initiated(id);
                        });
        when(client.uploadPart(any(UploadPartRequest.class)))
                .thenAnswer(
                        call -> {
                            UploadPartRequest request = call.getArgument(0);
                            Upload upload = uploads.get(request.uploadId());
                            if (upload == null) {
                                throw missingUpload();
                            }
                            upload.parts.put(
                                    request.partNumber().intValue(), request.body().toBytes());
                            return UploadPartResult.newBuilder()
                                    .headers(
                                            Collections.singletonMap(
                                                    "ETag", "etag-" + request.partNumber()))
                                    .build();
                        });
        when(client.uploadPartCopy(any(UploadPartCopyRequest.class)))
                .thenAnswer(
                        call -> {
                            UploadPartCopyRequest request = call.getArgument(0);
                            Upload upload = uploads.get(request.uploadId());
                            if (upload == null) {
                                throw missingUpload();
                            }
                            Stored source = objects.get(request.sourceKey());
                            if (source == null) {
                                throw missing();
                            }
                            String[] range = request.copySourceRange().substring(6).split("-");
                            upload.parts.put(
                                    request.partNumber().intValue(),
                                    Arrays.copyOfRange(
                                            source.bytes,
                                            Integer.parseInt(range[0]),
                                            Integer.parseInt(range[1]) + 1));
                            return copied("etag-" + request.partNumber());
                        });
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenAnswer(
                        call -> {
                            CompleteMultipartUploadRequest request = call.getArgument(0);
                            Upload upload = uploads.get(request.uploadId());
                            if (upload == null) {
                                throw missingUpload();
                            }
                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                            for (Part part : request.completeMultipartUpload().parts()) {
                                byte[] bytes = upload.parts.get(part.partNumber().intValue());
                                if (bytes == null) {
                                    throw new IllegalStateException("Missing part");
                                }
                                buffer.write(bytes);
                            }
                            Stored object = new Stored(buffer.toByteArray(), upload.headers);
                            if (Boolean.TRUE.equals(request.forbidOverwrite())) {
                                if (objects.putIfAbsent(request.key(), object) != null) {
                                    throw exists();
                                }
                            } else {
                                objects.put(request.key(), object);
                            }
                            uploads.remove(request.uploadId());
                            return CompleteMultipartUploadResult.newBuilder().build();
                        });
        when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
                .thenAnswer(
                        call -> {
                            uploads.remove(
                                    ((AbortMultipartUploadRequest) call.getArgument(0)).uploadId());
                            return AbortMultipartUploadResult.newBuilder().build();
                        });
        when(client.presign(any(GetObjectRequest.class), any(PresignOptions.class)))
                .thenAnswer(
                        call -> {
                            GetObjectRequest request = call.getArgument(0);
                            return PresignResult.newBuilder()
                                    .url(
                                            new URI(
                                                            "https",
                                                            request.bucket()
                                                                    + ".oss-cn-hangzhou.aliyuncs.com",
                                                            "/" + request.key(),
                                                            "signature=test",
                                                            null)
                                                    .toASCIIString())
                                    .build();
                        });
    }

    void put(String key, byte[] bytes) {
        objects.put(key, new Stored(bytes, Collections.emptyMap()));
    }

    static HeadObjectResult head(long length, Map<String, String> headers) {
        Map<String, String> values = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        values.putAll(headers);
        values.put("Content-Length", Long.toString(length));
        values.put("Last-Modified", "Wed, 09 Sep 2026 00:00:00 GMT");
        return HeadObjectResult.newBuilder().headers(values).build();
    }

    static InitiateMultipartUploadResult initiated(String id) {
        return InitiateMultipartUploadResult.newBuilder()
                .innerBody(InitiateMultipartUpload.newBuilder().uploadId(id).build())
                .build();
    }

    static UploadPartCopyResult copied(String tag) {
        return UploadPartCopyResult.newBuilder()
                .innerBody(CopyPartResult.newBuilder().eTag(tag).build())
                .build();
    }

    static ServiceException missing() {
        return ServiceException.newBuilder()
                .statusCode(404)
                .errorFields(Collections.singletonMap("Code", "NoSuchKey"))
                .build();
    }

    static ServiceException missingUpload() {
        return ServiceException.newBuilder()
                .statusCode(404)
                .errorFields(Collections.singletonMap("Code", "NoSuchUpload"))
                .build();
    }

    static ServiceException exists() {
        return ServiceException.newBuilder()
                .statusCode(409)
                .errorFields(Collections.singletonMap("Code", "FileAlreadyExists"))
                .build();
    }

    @Override
    public void close() throws Exception {
        fs.close();
    }

    static final class Stored {
        final byte[] bytes;
        final Map<String, String> headers;

        Stored(byte[] bytes, Map<String, String> headers) {
            this.bytes = bytes.clone();
            this.headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            this.headers.putAll(headers);
        }
    }

    private static final class Upload {
        final String key;
        final Map<String, String> headers;
        final Map<Integer, byte[]> parts = new ConcurrentSkipListMap<>();

        Upload(String key, Map<String, String> headers) {
            this.key = key;
            this.headers = new HashMap<>(headers);
        }
    }
}
