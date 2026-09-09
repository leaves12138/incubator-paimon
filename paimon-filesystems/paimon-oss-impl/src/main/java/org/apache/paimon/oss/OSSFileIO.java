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
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.StringUtils;

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.aliyun.sdk.service.oss2.models.PutObjectRequest;
import com.aliyun.sdk.service.oss2.transport.BinaryData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** OSS {@link FileIO}, backed entirely by the OSS Java SDK v2. */
public class OSSFileIO extends HadoopCompliantFileIO implements HadoopOptionsProvider {

    private static final long serialVersionUID = 2L;
    private static final String OSS_SSE_METHOD = "fs.oss.server-side-encryption";
    private static final String OSS_SSE_KMS_KEY_ID = "fs.oss.server-side-encryption-key-id";
    private static final String OSS_SSE_DATA_ENCRYPTION = "fs.oss.server-side-data-encryption";
    private static final String SSE_METHOD_AES256 = "AES256";
    private static final String SSE_METHOD_KMS = "KMS";
    private static final String SSE_DATA_SM4 = "SM4";
    private static final Map<CacheKey, OSSFileSystem> CACHE = new ConcurrentHashMap<>();
    private static final Configuration SHARED_CONFIG = new Configuration();

    private Options hadoopOptions;
    private boolean allowCache = true;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        allowCache = context.options().get(FILE_IO_ALLOW_CACHE);
        hadoopOptions = new Options();
        for (String key : context.options().keySet()) {
            if (key.startsWith("fs.oss.")) {
                String normalized = key;
                for (String sensitive :
                        new String[] {
                            "fs.oss.accessKeyId", "fs.oss.accessKeySecret", "fs.oss.securityToken"
                        }) {
                    if (key.equalsIgnoreCase(sensitive)) {
                        normalized = sensitive;
                    }
                }
                hadoopOptions.set(normalized, context.options().get(key));
            }
        }
        resolveSse(
                hadoopOptions.get(OSS_SSE_METHOD),
                hadoopOptions.get(OSS_SSE_KMS_KEY_ID),
                hadoopOptions.get(OSS_SSE_DATA_ENCRYPTION));
    }

    public Options hadoopOptions() {
        return hadoopOptions;
    }

    @Override
    public Options hadoopOptions(Path path, String opType) {
        return hadoopOptions;
    }

    @Override
    protected OSSFileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
        URI uri = path.toUri();
        Supplier<OSSFileSystem> supplier =
                () -> {
                    Configuration configuration = new Configuration(SHARED_CONFIG);
                    hadoopOptions.toMap().forEach(configuration::set);
                    OSSFileSystem fs = new OSSFileSystem();
                    try {
                        fs.initialize(uri, configuration);
                        return fs;
                    } catch (IOException e) {
                        IOUtils.closeQuietly(fs);
                        throw new UncheckedIOException(e);
                    } catch (RuntimeException | Error e) {
                        IOUtils.closeQuietly(fs);
                        throw e;
                    }
                };
        return allowCache
                ? CACHE.computeIfAbsent(
                        new CacheKey(hadoopOptions, uri.getAuthority()), k -> supplier.get())
                : supplier.get();
    }

    @Override
    public SeekableInputStream newInputStream(Path path, long fileSize) throws IOException {
        if (fileSize < 0) {
            return super.newInputStream(path);
        }
        try {
            URI uri = path.toUri();
            return new OSSRangeInputStream(
                    ossClient(path),
                    uri.getHost(),
                    uri.getPath().substring(1),
                    fileSize,
                    FileSystem.getStatistics("oss", OSSFileSystem.class));
        } catch (RuntimeException e) {
            throw new IOException("Failed to open OSS file " + path, e);
        }
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        OSSFileSystem fs = (OSSFileSystem) getFileSystem(path(path));
        fs.checkCreate(path(path), overwrite);
        return new OssTwoPhaseOutputStream(
                new OSSMultiPartUpload(fs, overwrite), path(path), path, overwrite);
    }

    @Override
    public boolean tryToWriteAtomic(Path path, String content) throws IOException {
        OSSFileSystem fs = (OSSFileSystem) getFileSystem(path(path));
        try {
            fs.client()
                    .putObject(
                            PutObjectRequest.newBuilder()
                                    .bucket(fs.bucket())
                                    .key(fs.objectKey(path(path)))
                                    .forbidOverwrite(true)
                                    .headers(fs.writeHeaders())
                                    .body(
                                            BinaryData.fromBytes(
                                                    content.getBytes(StandardCharsets.UTF_8)))
                                    .build());
            return true;
        } catch (RuntimeException e) {
            ServiceException service = ServiceException.asCause(e);
            if (service != null && "FileAlreadyExists".equals(service.errorCode())) {
                return false;
            }
            throw OSSFileSystem.ioException("Failed to write atomically", path(path), e);
        }
    }

    @Override
    public String createBlobPresignedUrl(
            Path tableRoot, BlobDescriptor descriptor, Duration validity) throws IOException {
        OSSFileSystem fs = (OSSFileSystem) getFileSystem(path(new Path(descriptor.uri())));
        return OSSBlobPresigner.create(fs, tableRoot, descriptor, validity);
    }

    OSSClient ossClient(Path path) throws IOException {
        return ((OSSFileSystem) getFileSystem(path(path))).client();
    }

    @Override
    public void close() {
        if (!allowCache && fsMap != null) {
            fsMap.values().forEach(IOUtils::closeQuietly);
            fsMap.clear();
        }
    }

    /** Parse the three SSE keys into an {@link SseConfig}, or null. Throws on bad input. */
    static SseConfig resolveSse(String method, String keyId, String dataEncryption) {
        // Reject present-but-blank values fail-fast instead of silently writing plaintext.
        checkNotBlankIfSet(method, OSS_SSE_METHOD);
        checkNotBlankIfSet(keyId, OSS_SSE_KMS_KEY_ID);
        checkNotBlankIfSet(dataEncryption, OSS_SSE_DATA_ENCRYPTION);
        method = method == null ? null : method.trim();
        keyId = keyId == null ? null : keyId.trim();
        dataEncryption = dataEncryption == null ? null : dataEncryption.trim();
        if (method == null && keyId == null && dataEncryption == null) {
            return null;
        }
        // A key id / data-encryption implies KMS; default the method to KMS when unset.
        method = canonicalSseMethod(method == null ? SSE_METHOD_KMS : method);
        if (keyId != null) {
            checkArgument(
                    keyId.chars().noneMatch(Character::isWhitespace),
                    "Invalid value for '%s': the CMK key id must not contain whitespace/newlines.",
                    OSS_SSE_KMS_KEY_ID);
            checkArgument(
                    SSE_METHOD_KMS.equals(method),
                    "'%s' requires '%s=KMS', but got '%s'.",
                    OSS_SSE_KMS_KEY_ID,
                    OSS_SSE_METHOD,
                    method);
        }
        if (dataEncryption != null) {
            checkArgument(
                    SSE_METHOD_KMS.equals(method),
                    "'%s' requires '%s=KMS', but got '%s'.",
                    OSS_SSE_DATA_ENCRYPTION,
                    OSS_SSE_METHOD,
                    method);
            checkArgument(
                    SSE_DATA_SM4.equalsIgnoreCase(dataEncryption),
                    "'%s' only supports 'SM4', but got '%s'.",
                    OSS_SSE_DATA_ENCRYPTION,
                    dataEncryption);
            dataEncryption = SSE_DATA_SM4;
        }
        return new SseConfig(method, keyId, dataEncryption);
    }

    /** Reject a config value that is present but blank (encryption requested yet misconfigured). */
    private static void checkNotBlankIfSet(String value, String key) {
        checkArgument(
                value == null || !StringUtils.isNullOrWhitespaceOnly(value),
                "'%s' is set but blank.",
                key);
    }

    /** Canonicalize the SSE method to the exact OSS header value; reject unknown values. */
    private static String canonicalSseMethod(String method) {
        if (SSE_METHOD_AES256.equalsIgnoreCase(method)) {
            return SSE_METHOD_AES256;
        }
        if (SSE_METHOD_KMS.equalsIgnoreCase(method)) {
            return SSE_METHOD_KMS;
        }
        if (SSE_DATA_SM4.equalsIgnoreCase(method)) {
            return SSE_DATA_SM4;
        }
        throw new IllegalArgumentException(
                "'"
                        + OSS_SSE_METHOD
                        + "' must be one of AES256/KMS/SM4, but got '"
                        + method
                        + "'.");
    }

    /** Resolved OSS server-side-encryption settings. */
    static final class SseConfig {
        final String method;
        final String keyId;
        final String dataEnc;

        SseConfig(String method, String keyId, String dataEnc) {
            this.method = method;
            this.keyId = keyId;
            this.dataEnc = dataEnc;
        }
    }

    static Map<String, String> writeHeaders(Options options) {
        SseConfig sse =
                resolveSse(
                        options.get(OSS_SSE_METHOD),
                        options.get(OSS_SSE_KMS_KEY_ID),
                        options.get(OSS_SSE_DATA_ENCRYPTION));
        Map<String, String> headers = new HashMap<>();
        if (sse != null) {
            headers.put("x-oss-server-side-encryption", sse.method);
            if (sse.keyId != null) {
                headers.put("x-oss-server-side-encryption-key-id", sse.keyId);
            }
            if (sse.dataEnc != null) {
                headers.put("x-oss-server-side-data-encryption", sse.dataEnc);
            }
        } else {
            String fallback = options.get("fs.oss.server-side-encryption-algorithm");
            if (fallback != null && !fallback.isEmpty()) {
                headers.put("x-oss-server-side-encryption", fallback);
            }
        }
        return headers;
    }

    private static class CacheKey {
        private final Options options;
        private final String authority;

        CacheKey(Options options, String authority) {
            this.options = new Options(options.toMap());
            this.authority = authority;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CacheKey)) {
                return false;
            }
            CacheKey that = (CacheKey) other;
            return options.equals(that.options) && Objects.equals(authority, that.authority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(options, authority);
        }
    }
}
