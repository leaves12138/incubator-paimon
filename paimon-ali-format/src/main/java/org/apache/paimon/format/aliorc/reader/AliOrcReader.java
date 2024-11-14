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

package org.apache.paimon.format.aliorc.reader;

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.aliorc.jni.AliOrcNativeReader;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

/** AliOrc reader. */
public class AliOrcReader implements RecordReader<InternalRow> {

    private final RootAllocator rootAllocator;
    private final ArrowBatchReader arrowBatchReader;
    private final AliOrcNativeReader reader;

    public AliOrcReader(
            SeekableInputStream inputStream, long size, RowType projectedRowType, int batchSize) {
        this.rootAllocator = new RootAllocator();
        this.reader =
                new AliOrcNativeReader(
                        inputStream,
                        size,
                        String.join(",", projectedRowType.getFieldNames()).toLowerCase(),
                        batchSize);
        this.arrowBatchReader = new ArrowBatchReader(projectedRowType, false);
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        try (ArrowArray array = ArrowArray.allocateNew(rootAllocator);
                ArrowSchema schema = ArrowSchema.allocateNew(rootAllocator)) {
            reader.readBatch(array.memoryAddress(), schema.memoryAddress());
            VectorSchemaRoot vectorSchemaRoot =
                    Data.importVectorSchemaRoot(rootAllocator, array, schema, null);
            Iterator<InternalRow> rows = arrowBatchReader.readBatch(vectorSchemaRoot).iterator();
            return new RecordIterator<InternalRow>() {
                @Nullable
                @Override
                public InternalRow next() {
                    return rows.hasNext() ? rows.next() : null;
                }

                @Override
                public void releaseBatch() {
                    vectorSchemaRoot.close();
                }
            };
        } catch (EOFException e) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
        this.rootAllocator.close();
    }
}