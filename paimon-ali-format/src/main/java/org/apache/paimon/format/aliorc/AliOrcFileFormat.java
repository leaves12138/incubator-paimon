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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.filter.OrcSimpleStatsExtractor;
import org.apache.paimon.format.orc.reader.OrcSplitReaderUtil;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.orc.TypeDescription;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** AliOrc {@link FileFormat}. */
public class AliOrcFileFormat extends FileFormat {

    private final FileFormatFactory.FormatContext formatContext;

    public AliOrcFileFormat(FileFormatFactory.FormatContext formatContext, String identifier) {
        super(identifier);
        this.formatContext = formatContext;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType projectedRowType, @Nullable List<Predicate> list) {
        // TODO: support predicate push down.
        return new AliOrcReaderFactory(projectedRowType, formatContext.readBatchSize());
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType rowType) {
        DataType refinedType = OrcFileFormat.refineDataType(rowType);
        TypeDescription typeDescription = OrcSplitReaderUtil.toOrcType(refinedType);
        return new AliOrcWriterFactory(
                AliOrcSchemaUtil.suitAliorc(typeDescription.toString()),
                () -> new ArrowFormatCWriter(rowType, formatContext.writeBatchSize(), false),
                formatContext.writeBatchSize(),
                formatContext.zstdLevel());
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new OrcSimpleStatsExtractor(type, statsCollectors));
    }

    @Override
    public void validateDataFields(RowType rowType) {}
}
