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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the real SDK and HTTP transport against a loopback server, never a cloud bucket. */
class OSSClientFactoryTest {
    @Test
    void testFileBodyRetryAndHeadErrorMapping() throws Exception {
        List<byte[]> attempts = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<byte[]> stored = new AtomicReference<>();
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    try {
                        String method = exchange.getRequestMethod();
                        exchange.getResponseHeaders().set("x-oss-request-id", "loopback");
                        if ("PUT".equals(method)) {
                            ByteArrayOutputStream body = new ByteArrayOutputStream();
                            IOUtils.copyBytes(exchange.getRequestBody(), body, 8192, false);
                            attempts.add(body.toByteArray());
                            assertThat(
                                            exchange.getRequestHeaders()
                                                    .getFirst("x-oss-security-token"))
                                    .isEqualTo("test-token");
                            if (attempts.size() == 1) {
                                byte[] error =
                                        "<Error><Code>ServiceUnavailable</Code><Message>retry</Message></Error>"
                                                .getBytes(StandardCharsets.UTF_8);
                                exchange.sendResponseHeaders(503, error.length);
                                exchange.getResponseBody().write(error);
                            } else {
                                stored.set(body.toByteArray());
                                exchange.getResponseHeaders().set("ETag", "test-etag");
                                exchange.sendResponseHeaders(200, -1);
                            }
                        } else if ("HEAD".equals(method)) {
                            if (stored.get() == null) {
                                // A HEAD response has no XML body; the SDK may only have the HTTP
                                // status.
                                exchange.sendResponseHeaders(404, -1);
                            } else {
                                exchange.getResponseHeaders()
                                        .set("Content-Length", String.valueOf(stored.get().length));
                                exchange.getResponseHeaders()
                                        .set("Last-Modified", "Wed, 09 Sep 2026 00:00:00 GMT");
                                exchange.sendResponseHeaders(200, -1);
                            }
                        } else if (exchange.getRequestURI().getQuery() != null) {
                            byte[] xml =
                                    "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
                                            .getBytes(StandardCharsets.UTF_8);
                            exchange.sendResponseHeaders(200, xml.length);
                            exchange.getResponseBody().write(xml);
                        } else {
                            byte[] bytes = stored.get();
                            String range = exchange.getRequestHeaders().getFirst("Range");
                            if (range != null) {
                                String[] bounds = range.substring(6).split("-");
                                int from = Integer.parseInt(bounds[0]);
                                int to = Integer.parseInt(bounds[1]) + 1;
                                exchange.getResponseHeaders()
                                        .set(
                                                "Content-Range",
                                                "bytes "
                                                        + from
                                                        + "-"
                                                        + (to - 1)
                                                        + "/"
                                                        + bytes.length);
                                bytes = Arrays.copyOfRange(bytes, from, to);
                            }
                            exchange.sendResponseHeaders(range == null ? 200 : 206, bytes.length);
                            exchange.getResponseBody().write(bytes);
                        }
                    } catch (Throwable e) {
                        serverFailure.set(e);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        Options options = new Options();
        options.set("fs.oss.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        options.set("fs.oss.sld.enabled", "true");
        options.set("fs.oss.accessKeyId", "test-key");
        options.set("fs.oss.accessKeySecret", "test-secret");
        options.set("fs.oss.securityToken", "test-token");
        options.set("fs.oss.attempts.maximum", "1");
        options.set("fs.oss.connection.timeout", "5000");
        options.set(FILE_IO_ALLOW_CACHE, false);
        try (OSSFileIO io = new OSSFileIO()) {
            io.configure(CatalogContext.create(options));
            Path path = new Path("oss://bucket/file");
            assertThat(io.exists(path)).isFalse();
            byte[] expected = new byte[100_001];
            new java.util.Random(7).nextBytes(expected);
            try (PositionOutputStream out = io.newOutputStream(path, false)) {
                out.write(expected);
            }
            assertThat(attempts).hasSize(2);
            assertThat(attempts.get(0)).containsExactly(expected);
            assertThat(attempts.get(1)).containsExactly(expected);
            assertThat(io.getFileStatus(path).getLen()).isEqualTo(expected.length);
            try (SeekableInputStream input = io.newInputStream(path, expected.length)) {
                byte[] actual = new byte[113];
                input.seek(71);
                IOUtils.readFully(input, actual, 0, actual.length);
                assertThat(actual).containsExactly(Arrays.copyOfRange(expected, 71, 184));
            }
            assertThat(serverFailure.get()).isNull();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testCannotRecreatePublicClientAfterClose() throws Exception {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
        conf.set("fs.oss.endpoint", "https://oss-cn-hangzhou-internal.aliyuncs.com");
        conf.set("fs.oss.accessKeyId", "test-key");
        conf.set("fs.oss.accessKeySecret", "test-secret");
        OSSFileSystem fs = new OSSFileSystem();
        fs.initialize(URI.create("oss://bucket"), conf);
        fs.close();
        assertThatThrownBy(fs::publicClient).isInstanceOf(IllegalStateException.class);
    }
}
