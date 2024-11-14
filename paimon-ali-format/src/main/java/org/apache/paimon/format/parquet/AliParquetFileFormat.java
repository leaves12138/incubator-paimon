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

package org.apache.paimon.format.parquet;

import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.paimon.types.DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.paimon.types.DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

/** Parquet {@link FileFormat}. */
public class AliParquetFileFormat extends ParquetFileFormat {

    private static final String USE_NATIVE = "parquet.use-native";
    private static final String MULTI_THRAD = "parquet.use-multi-thread";

    private final FileFormatFactory.FormatContext formatContext;

    public AliParquetFileFormat(FileFormatFactory.FormatContext formatContext) {
        super(formatContext);
        this.formatContext = formatContext;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType projectedRowType, @Nullable List<Predicate> list) {
        if (formatContext.options().getBoolean(USE_NATIVE, false)) {
            return new AliParquetReaderFactory(
                    projectedRowType,
                    formatContext.readBatchSize(),
                    formatContext.options().getBoolean(MULTI_THRAD, false));
        } else {
            return super.createReaderFactory(projectedRowType, list);
        }
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        if (supportNativeWrite(type) && formatContext.options().getBoolean(USE_NATIVE, true)) {
            return new AliParquetWriterFactory(
                    type,
                    () -> new ArrowFormatCWriter(type, formatContext.writeBatchSize(), true),
                    formatContext.writeBatchSize(),
                    formatContext.zstdLevel());
        } else {
            return super.createWriterFactory(type);
        }
    }

    private boolean supportNativeWrite(RowType writeRowType) {
        for (DataType fieldType : writeRowType.getFieldTypes()) {
            switch (fieldType.getTypeRoot()) {
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    int precision = DataTypeChecks.getPrecision(fieldType);
                    if (precision > 6) {
                        return false;
                    }
            }
        }
        return true;
    }
}
