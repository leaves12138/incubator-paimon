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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;

import java.util.Arrays;

/** * Abstract class for vectors that have children. */
public abstract class AbstractStructVector extends AbstractHeapVector
        implements WritableColumnVector {

    protected ColumnVector[] children;
    protected long[] offsets;
    protected long[] lengths;

    public AbstractStructVector(int len, ColumnVector[] children) {
        super(len);
        this.offsets = new long[len];
        this.lengths = new long[len];
        this.children = children;
    }

    public void putOffsetLength(int index, long offset, long length) {
        offsets[index] = offset;
        lengths[index] = length;
    }

    public long[] getOffsets() {
        return offsets;
    }

    public void setOffsets(long[] offsets) {
        this.offsets = offsets;
    }

    public long[] getLengths() {
        return lengths;
    }

    public void setLengths(long[] lengths) {
        this.lengths = lengths;
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (offsets.length < newCapacity) {
            offsets = Arrays.copyOf(offsets, newCapacity);
            lengths = Arrays.copyOf(lengths, newCapacity);
        }
    }

    @Override
    public void reset() {
        super.reset();
        if (offsets.length != initialCapacity) {
            offsets = new long[initialCapacity];
        } else {
            Arrays.fill(offsets, 0);
        }
        if (lengths.length != initialCapacity) {
            lengths = new long[initialCapacity];
        } else {
            Arrays.fill(lengths, 0);
        }
        for (ColumnVector child : children) {
            if (child instanceof WritableColumnVector) {
                ((WritableColumnVector) child).reset();
            }
        }
    }

    @Override
    public ColumnVector[] getChildren() {
        return children;
    }
}
