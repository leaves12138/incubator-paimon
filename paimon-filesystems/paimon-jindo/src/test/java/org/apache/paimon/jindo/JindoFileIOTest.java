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

package org.apache.paimon.jindo;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.oss.OSSFileIO;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for Jindo configuration and its shared OSS-v2 Blob URL implementation. */
public class JindoFileIOTest {
    private static final String SHOW_DIR_TIMESTAMP = "fs.oss.show-dir-timestamp";

    @Test
    public void testDisableDirectoryTimestampByDefault() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(new Options()));

        assertThat(
                        fileIO.hadoopOptions(new Path("oss://bucket/table"), "meta")
                                .get(SHOW_DIR_TIMESTAMP))
                .isEqualTo("false");
    }

    @Test
    public void testKeepExplicitDirectoryTimestampSetting() {
        Options options = new Options();
        options.set(SHOW_DIR_TIMESTAMP, "true");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        assertThat(
                        fileIO.hadoopOptions(new Path("oss://bucket/table"), "meta")
                                .get(SHOW_DIR_TIMESTAMP))
                .isEqualTo("true");
    }

    @Test
    public void testBlobDelegateUsesConfiguredStsAndOwnsItsClients() throws Exception {
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss.example.com");
        options.set("fs.oss.region", "cn-hangzhou");
        options.set("fs.oss.accessKeyId", "test-key");
        options.set("fs.oss.accessKeySecret", "test-secret");
        options.set("fs.oss.securityToken", "test-token");
        JindoFileIO configured = new JindoFileIO();
        configured.configure(CatalogContext.create(options));
        try (OSSFileIO delegate =
                JindoFileIO.createBlobFileIO(
                        configured.hadoopOptions(new Path("oss://bucket/table"), "meta"))) {
            assertThat(delegate.hadoopOptions().get("fs.oss.securityToken"))
                    .isEqualTo("test-token");
            assertThat(delegate.hadoopOptions().get("fs.oss.region")).isEqualTo("cn-hangzhou");
            assertThat(delegate.hadoopOptions().containsKey("fs.oss.credentials.provider"))
                    .isFalse();
            // An empty known-length stream constructs the real client without making a request.
            try (org.apache.paimon.fs.SeekableInputStream in =
                    delegate.newInputStream(new Path("oss://bucket/table/file"), 0)) {
                assertThat(in.read()).isEqualTo(-1);
            }
        }
        assertThat(options.containsKey(FILE_IO_ALLOW_CACHE.key())).isFalse();
    }

    @Test
    public void testCreateBlobPresignedUrlDelegatesAndCloses() throws Exception {
        OSSFileIO delegate = mock(OSSFileIO.class);
        Path root = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 10, 20);
        Duration validity = Duration.ofHours(1);
        when(delegate.createBlobPresignedUrl(root, descriptor, validity))
                .thenReturn("https://bucket.oss.example.com/materialized");
        JindoFileIO fileIO = new JindoFileIO(delegate);
        assertThat(fileIO.createBlobPresignedUrl(root, descriptor, validity))
                .isEqualTo("https://bucket.oss.example.com/materialized");
        fileIO.close();
        verify(delegate).close();
    }
}
