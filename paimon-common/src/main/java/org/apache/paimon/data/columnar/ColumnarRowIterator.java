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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.RecyclableIterator;
import org.apache.paimon.utils.VectorMappingUtils;

import javax.annotation.Nullable;

/**
 * A {@link RecordReader.RecordIterator} that returns {@link InternalRow}s. The next row is set by
 * {@link ColumnarRow#setRowId}.
 */
public class ColumnarRowIterator extends RecyclableIterator<InternalRow>
        implements FileRecordIterator<InternalRow> {

    protected final Path filePath;
    protected final ColumnarRow row;
    protected final Runnable recycler;

    protected int num;
    protected int nextPos;
    protected long[] positions;

    public ColumnarRowIterator(Path filePath, ColumnarRow row, @Nullable Runnable recycler) {
        super(recycler);
        this.filePath = filePath;
        this.row = row;
        this.recycler = recycler;
    }

    public void reset(long nextFilePos) {
        long[] positions = new long[row.batch().getNumRows()];
        for (int i = 0; i < row.batch().getNumRows(); i++) {
            positions[i] = nextFilePos++;
        }
        reset(positions);
    }

    public void reset(long[] positions) {
        assert positions.length == row.batch().getNumRows();
        this.positions = positions;
        this.num = row.batch().getNumRows();
        this.nextPos = 0;
    }

    @Nullable
    @Override
    public InternalRow next() {
        if (nextPos < num) {
            row.setRowId(nextPos++);
            return row;
        } else {
            return null;
        }
    }

    @Override
    public long returnedPosition() {
        if (nextPos == 0) {
            return positions[0] - 1;
        }
        return positions[nextPos - 1];
    }

    @Override
    public Path filePath() {
        return this.filePath;
    }

    protected ColumnarRowIterator copy(ColumnVector[] vectors) {
        ColumnarRowIterator newIterator =
                new ColumnarRowIterator(filePath, row.copy(vectors), recycler);
        newIterator.reset(positions);
        return newIterator;
    }

    public ColumnarRowIterator mapping(
            @Nullable PartitionInfo partitionInfo, @Nullable int[] indexMapping) {
        if (partitionInfo != null || indexMapping != null) {
            VectorizedColumnBatch vectorizedColumnBatch = row.batch();
            ColumnVector[] vectors = vectorizedColumnBatch.columns;
            if (partitionInfo != null) {
                vectors = VectorMappingUtils.createPartitionMappedVectors(partitionInfo, vectors);
            }
            if (indexMapping != null) {
                vectors = VectorMappingUtils.createMappedVectors(indexMapping, vectors);
            }
            return copy(vectors);
        }
        return this;
    }
}
