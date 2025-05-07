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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.aliorc.reader.AliOrcReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;

import java.io.IOException;

/** A factory to create Aliorc Reader. */
public class AliOrcReaderFactory implements FormatReaderFactory {

    private final RowType projectedRowType;
    private final int batchSize;

    public AliOrcReaderFactory(RowType projectedRowType, int batchSize) {
        this.projectedRowType = projectedRowType;
        this.batchSize = batchSize;
    }

    @Override
    public RecordReader<InternalRow> createReader(Context context) throws IOException {
        return new AliOrcReader(
                context.fileIO().newInputStream(context.filePath()),
                context.fileSize(),
                projectedRowType,
                batchSize);
    }
}
