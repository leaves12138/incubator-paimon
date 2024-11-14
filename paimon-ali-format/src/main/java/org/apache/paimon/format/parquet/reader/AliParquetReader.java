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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.parquet.jni.AliParquetNativeReader;
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

/** Parquet reader for arrow. */
public class AliParquetReader implements RecordReader<InternalRow> {

    private final RootAllocator rootAllocator;
    private final ArrowBatchReader arrowBatchReader;
    private final AliParquetNativeReader reader;
    public static long cost = 0;

    public AliParquetReader(
            SeekableInputStream inputStream,
            long size,
            RowType projectedRowType,
            int batchSize,
            boolean multiThread) {
        this.rootAllocator = new RootAllocator();
        this.reader =
                new AliParquetNativeReader(
                        inputStream,
                        size,
                        String.join(",", projectedRowType.getFieldNames()),
                        batchSize,
                        multiThread);
        this.arrowBatchReader = new ArrowBatchReader(projectedRowType, true);
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        try (ArrowArray array = ArrowArray.allocateNew(rootAllocator);
                ArrowSchema schema = ArrowSchema.allocateNew(rootAllocator)) {
            long time0 = System.currentTimeMillis();
            reader.readBatch(array.memoryAddress(), schema.memoryAddress());
            cost += (System.currentTimeMillis() - time0);
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
