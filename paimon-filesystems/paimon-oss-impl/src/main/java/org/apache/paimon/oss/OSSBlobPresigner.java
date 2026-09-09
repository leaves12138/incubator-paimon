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
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.BlobDescriptorUtils;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.PresignOptions;
import com.aliyun.sdk.service.oss2.models.GetObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectRequest;
import com.aliyun.sdk.service.oss2.models.HeadObjectResult;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.transport.BinaryData;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Materializes blob ranges and creates OSS presigned URLs using SDK v2. */
public final class OSSBlobPresigner {
    private static final String BLOB_FINGERPRINT_METADATA = "paimon-blob-descriptor-sha256";
    private static final String BLOB_CONTENT_TYPE = "application/octet-stream";
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private OSSBlobPresigner() {}

    static String create(
            OSSFileSystem fs, Path tableRoot, BlobDescriptor descriptor, Duration validity)
            throws IOException {
        BlobDescriptorUtils.validateTableRoot(tableRoot, descriptor);
        if (validity == null
                || validity.isZero()
                || validity.isNegative()
                || validity.getNano() != 0) {
            throw new IOException("Blob presigned URL validity must be positive whole seconds.");
        }
        URI endpoint = fs.publicEndpoint();
        if (!"https".equalsIgnoreCase(endpoint.getScheme())) {
            throw new IOException("Blob presigned URLs require an HTTPS endpoint.");
        }
        try {
            OSSClient client = fs.client();
            URI source = new Path(descriptor.uri()).toUri();
            String bucket = source.getAuthority();
            String sourceKey = objectKey(source);
            String fingerprint = sha256Hex(descriptor.serialize());
            int parentEnd = sourceKey.lastIndexOf('/') + 1;
            String targetKey = sourceKey.substring(0, parentEnd) + "_bloburl_" + fingerprint;

            HeadObjectResult target = headObjectIfExists(client, bucket, targetKey);
            if (!matches(target, descriptor.length(), fingerprint)) {
                HeadObjectResult metadata =
                        client.headObject(
                                HeadObjectRequest.newBuilder()
                                        .bucket(bucket)
                                        .key(sourceKey)
                                        .build());
                validateRange(descriptor, metadata.contentLength());
                Map<String, String> headers = new HashMap<>(fs.writeHeaders());
                headers.put("Content-Type", BLOB_CONTENT_TYPE);
                Map<String, String> fingerprintMetadata =
                        Collections.singletonMap(BLOB_FINGERPRINT_METADATA, fingerprint);
                if (descriptor.length() == 0) {
                    client.putObject(
                            PutObjectRequest.newBuilder()
                                    .bucket(bucket)
                                    .key(targetKey)
                                    .headers(headers)
                                    .metadata(fingerprintMetadata)
                                    .body(BinaryData.fromBytes(new byte[0]))
                                    .build());
                } else {
                    OSSMultiPartUpload.copyRange(
                            client,
                            bucket,
                            sourceKey,
                            targetKey,
                            descriptor.offset(),
                            descriptor.length(),
                            headers,
                            fingerprintMetadata);
                }
                target =
                        client.headObject(
                                HeadObjectRequest.newBuilder()
                                        .bucket(bucket)
                                        .key(targetKey)
                                        .build());
                if (!matches(target, descriptor.length(), fingerprint)) {
                    throw new IOException(
                            "Materialized blob object metadata does not match descriptor.");
                }
            }

            // Sign the public endpoint itself; rewriting the host after V4 signing is invalid.
            URL url =
                    new URL(
                            fs.publicClient()
                                    .presign(
                                            GetObjectRequest.newBuilder()
                                                    .bucket(bucket)
                                                    .key(targetKey)
                                                    .build(),
                                            PresignOptions.newBuilder()
                                                    .expiration(validity)
                                                    .build())
                                    .url());
            String expectedHost = bucket + "." + endpoint.getHost();
            if (!"https".equalsIgnoreCase(url.getProtocol())
                    || !expectedHost.equalsIgnoreCase(url.getHost())
                    || !("/" + targetKey).equals(url.toURI().getPath())) {
                throw new IOException(
                        "OSS client generated a presigned URL for an invalid target.");
            }
            return url.toString();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to create blob presigned URL.", e);
        }
    }

    private static HeadObjectResult headObjectIfExists(
            OSSClient client, String bucket, String key) {
        try {
            return client.headObject(
                    HeadObjectRequest.newBuilder().bucket(bucket).key(key).build());
        } catch (RuntimeException e) {
            if (OSSFileSystem.missing(e)) {
                return null;
            }
            throw e;
        }
    }

    private static boolean matches(HeadObjectResult metadata, long length, String fingerprint) {
        return metadata != null
                && metadata.contentLength() != null
                && metadata.contentLength() == length
                && BLOB_CONTENT_TYPE.equals(metadata.contentType())
                && fingerprint.equals(metadata.metadata().get(BLOB_FINGERPRINT_METADATA));
    }

    private static void validateRange(BlobDescriptor descriptor, long sourceLength)
            throws IOException {
        if (descriptor.offset() < 0
                || descriptor.length() < 0
                || descriptor.offset() > sourceLength
                || descriptor.length() > sourceLength - descriptor.offset()) {
            throw new IOException("Blob descriptor range is outside the source object.");
        }
    }

    private static String objectKey(URI uri) throws IOException {
        String path = uri.getPath();
        if (path == null || !path.startsWith("/") || path.length() == 1) {
            throw new IOException("Blob descriptor URI must contain an OSS object key.");
        }
        return path.substring(1);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
            char[] chars = new char[hash.length * 2];
            for (int i = 0; i < hash.length; i++) {
                chars[i * 2] = HEX_CHARS[(hash[i] >> 4) & 0x0f];
                chars[i * 2 + 1] = HEX_CHARS[hash[i] & 0x0f];
            }
            return new String(chars);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available.", e);
        }
    }
}
