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

package org.apache.paimon.format.aliorc;

import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.aliorc.jni.AliOrcNativeWriter;
import org.apache.paimon.format.common.writer.ArrowBundleWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Supplier;

/** A factory to create AliOrc {@link FormatWriter}. */
public class AliOrcWriterFactory implements FormatWriterFactory {

    private final String schema;
    private final int writeBatchSize;
    private final int zstdLevel;
    private final Supplier<ArrowFormatCWriter> arrowFormatWriterSupplier;

    public AliOrcWriterFactory(
            String schema,
            Supplier<ArrowFormatCWriter> arrowFormatWriterSupplier,
            int writeBatchSize,
            int zstdLevel) {
        this.schema = schema;
        this.arrowFormatWriterSupplier = arrowFormatWriterSupplier;
        this.writeBatchSize = writeBatchSize;
        this.zstdLevel = zstdLevel;
    }

    @Override
    public FormatWriter create(
            PositionOutputStream positionOutputStream, @Nullable String compression)
            throws IOException {
        ArrowFormatCWriter arrowFormatCWriter = arrowFormatWriterSupplier.get();
        AliOrcNativeWriter nativeWriter =
                new AliOrcNativeWriter(
                        positionOutputStream,
                        schema.toLowerCase(),
                        StringUtils.isNullOrWhitespaceOnly(compression) ? "zstd" : compression,
                        writeBatchSize,
                        zstdLevel,
                        128 * 1024 * 1024);
        return new ArrowBundleWriter(positionOutputStream, arrowFormatCWriter, nativeWriter);
    }
}
