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

import com.aliyun.sdk.service.oss2.OSSClient;
import com.aliyun.sdk.service.oss2.OSSClientBuilder;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.credentials.StaticCredentialsProvider;
import com.aliyun.sdk.service.oss2.signer.Signer;
import com.aliyun.sdk.service.oss2.signer.SignerV1;
import com.aliyun.sdk.service.oss2.signer.SigningContext;
import com.aliyun.sdk.service.oss2.transport.HttpClientOptions;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClient;
import com.aliyun.sdk.service.oss2.transport.apache5client.Apache5HttpClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Maps the existing fs.oss options to OSS SDK v2 without loading the Hadoop OSS connector. */
final class OSSClientFactory {
    private OSSClientFactory() {}

    static URI endpoint(Configuration conf) {
        String endpoint = conf.getTrimmed("fs.oss.endpoint");
        checkArgument(endpoint != null && !endpoint.isEmpty(), "fs.oss.endpoint is required.");
        if (!endpoint.contains("://")) {
            endpoint =
                    (conf.getBoolean("fs.oss.connection.secure.enabled", true)
                                    ? "https://"
                                    : "http://")
                            + endpoint;
        }
        URI uri = URI.create(endpoint);
        checkArgument(
                uri.getHost() != null && uri.getUserInfo() == null,
                "OSS endpoint must have a host and must not contain credentials.");
        return uri;
    }

    static OSSClient create(Configuration conf, URI endpoint) throws IOException {
        CredentialsProvider credentials;
        String provider = conf.getTrimmed("fs.oss.credentials.provider", "");
        if (!provider.isEmpty()) {
            try {
                Class<?> clazz =
                        Class.forName(
                                provider, true, Thread.currentThread().getContextClassLoader());
                Object instance;
                try {
                    instance = clazz.getConstructor(Configuration.class).newInstance(conf);
                } catch (NoSuchMethodException e) {
                    instance = clazz.getConstructor().newInstance();
                }
                checkArgument(
                        instance instanceof CredentialsProvider,
                        "fs.oss.credentials.provider must implement OSS SDK v2 CredentialsProvider.");
                credentials = (CredentialsProvider) instance;
            } catch (ReflectiveOperationException e) {
                throw new IOException("Failed to create OSS credentials provider", e);
            }
        } else {
            char[] id = conf.getPassword("fs.oss.accessKeyId");
            char[] secret = conf.getPassword("fs.oss.accessKeySecret");
            char[] token = conf.getPassword("fs.oss.securityToken");
            checkArgument(
                    id != null && secret != null,
                    "fs.oss.accessKeyId and fs.oss.accessKeySecret are required.");
            credentials =
                    new StaticCredentialsProvider(
                            new String(id),
                            new String(secret),
                            token == null ? null : new String(token));
        }

        int maxConnections = conf.getInt("fs.oss.connection.maximum", 32);
        int connectTimeout = conf.getInt("fs.oss.connection.establish.timeout", 50_000);
        int readTimeout = conf.getInt("fs.oss.connection.timeout", 200_000);
        int poolTimeout = conf.getInt("fs.oss.connection.request.timeout", -1);
        checkArgument(
                maxConnections > 0 && connectTimeout >= 0 && readTimeout >= 0,
                "Invalid OSS connection limit or timeout.");
        HttpClientOptions options =
                HttpClientOptions.custom()
                        .connectTimeout(Duration.ofMillis(connectTimeout))
                        .readWriteTimeout(Duration.ofMillis(readTimeout))
                        .build();
        Apache5HttpClientBuilder transport =
                Apache5HttpClientBuilder.create()
                        .options(options)
                        .maxConnections(maxConnections)
                        .connectionRequestTimeout(poolTimeout)
                        .idleConnectionTime(conf.getLong("fs.oss.connection.idle.time", 60_000));
        String proxy = conf.getTrimmed("fs.oss.proxy.host", "");
        if (!proxy.isEmpty()) {
            checkArgument(
                    conf.get("fs.oss.proxy.username") == null,
                    "Authenticated OSS proxies need a v2 HTTP transport configuration.");
            transport.requestConfig(
                    RequestConfig.custom()
                            .setProxy(
                                    new HttpHost(
                                            "http", proxy, conf.getInt("fs.oss.proxy.port", 80)))
                            .setResponseTimeout(Timeout.ofMilliseconds(readTimeout))
                            .setConnectionRequestTimeout(
                                    Timeout.ofMilliseconds(Math.max(0, poolTimeout)))
                            .build());
        }

        String signature =
                conf.getTrimmed("fs.oss.signature.version", "v1").toLowerCase(Locale.ROOT);
        checkArgument(
                signature.equals("v1") || signature.equals("v4"),
                "fs.oss.signature.version must be v1 or v4.");
        String region = conf.getTrimmed("fs.oss.region", "");
        if (signature.equals("v4")) {
            checkArgument(!region.isEmpty(), "fs.oss.region is required for V4 signatures.");
        }
        String host = endpoint.getHost().toLowerCase(Locale.ROOT);
        boolean pathStyle = conf.getBoolean("fs.oss.sld.enabled", false);
        boolean cname =
                !pathStyle
                        && conf.getBoolean("fs.oss.cname.enabled", true)
                        && Arrays.asList("aliyuncs.com", "aliyun-inc.com", "aliyuncs-inc.com")
                                .stream()
                                .noneMatch(host::endsWith);
        Apache5HttpClient http = transport.build();
        try {
            OSSClientBuilder builder =
                    OSSClient.newBuilder()
                            .endpoint(endpoint.toString())
                            .region(region)
                            .credentialsProvider(credentials)
                            .signatureVersion(signature)
                            .usePathStyle(pathStyle)
                            .useCName(cname)
                            .retryMaxAttempts(conf.getInt("fs.oss.attempts.maximum", 10) + 1)
                            .httpClient(http);
            if (signature.equals("v1")) {
                builder.signer(v1Signer());
            }
            String userAgent = conf.getTrimmed("fs.oss.user.agent.prefix");
            if (userAgent != null) {
                builder.userAgent(userAgent);
            }
            return builder.build();
        } catch (RuntimeException | Error failure) {
            try {
                http.close();
            } catch (Exception e) {
                failure.addSuppressed(e);
            }
            throw failure;
        }
    }

    static Signer v1Signer() {
        return new SignerV1() {
            @Override
            public void sign(SigningContext context) {
                boolean headerAuth =
                        context != null
                                && context.getRequest() != null
                                && context.getCredentials() != null
                                && !context.isAuthMethodQuery();
                if (headerAuth) {
                    String token = context.getCredentials().securityToken();
                    if (token != null && !token.isEmpty()) {
                        context.getRequest().headers().put("x-oss-security-token", token);
                    } else {
                        context.getRequest().headers().remove("x-oss-security-token");
                    }
                }
                // SDK v2 0.6.0 uses the query-token name for header auth. Sign the correct
                // x-oss header, and do not forward the stray unsigned header to OSS.
                super.sign(context);
                if (headerAuth) {
                    context.getRequest().headers().remove("security-token");
                }
            }
        };
    }
}
