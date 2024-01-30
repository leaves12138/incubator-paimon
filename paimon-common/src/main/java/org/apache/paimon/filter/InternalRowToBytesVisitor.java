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

package org.apache.paimon.filter;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.util.Arrays;
import java.util.function.BiFunction;

/** Temp. */
public class InternalRowToBytesVisitor
        implements DataTypeVisitor<BiFunction<DataGetters, Integer, byte[]>> {

    public static final byte[] NULL_BYTES = new byte[1];

    static {
        Arrays.fill(NULL_BYTES, (byte) 0x00);
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(CharType charType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return row.getString(index).toBytes();
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(VarCharType varCharType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return row.getString(index).toBytes();
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(BooleanType booleanType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return row.getBoolean(index) ? new byte[] {0x01} : new byte[] {0x00};
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(BinaryType binaryType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return row.getBinary(index);
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(VarBinaryType varBinaryType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return row.getBinary(index);
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(DecimalType decimalType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale())
                        .toUnscaledBytes();
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(TinyIntType tinyIntType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return new byte[] {row.getByte(index)};
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(SmallIntType smallIntType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                short x = row.getShort(index);
                return new byte[] {(byte) (x & 0xff), (byte) (x >> 8 & 0xff)};
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(IntType intType) {
        return (row, index) -> row.isNullAt(index) ? NULL_BYTES : intToBytes(row.getInt(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(BigIntType bigIntType) {
        return (row, index) -> row.isNullAt(index) ? NULL_BYTES : longToBytes(row.getLong(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(FloatType floatType) {
        return (row, index) ->
                row.isNullAt(index)
                        ? NULL_BYTES
                        : intToBytes(Float.floatToIntBits(row.getFloat(index)));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(DoubleType doubleType) {
        return (row, index) ->
                row.isNullAt(index)
                        ? NULL_BYTES
                        : longToBytes(Double.doubleToLongBits(row.getDouble(index)));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(DateType dateType) {
        return (row, index) -> row.isNullAt(index) ? NULL_BYTES : intToBytes(row.getInt(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(TimeType timeType) {
        return (row, index) -> row.isNullAt(index) ? NULL_BYTES : intToBytes(row.getInt(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(TimestampType timestampType) {
        return (row, index) ->
                row.isNullAt(index)
                        ? NULL_BYTES
                        : longToBytes(
                                row.getTimestamp(index, timestampType.getPrecision())
                                        .getMillisecond());
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(
            LocalZonedTimestampType localZonedTimestampType) {
        return (row, index) ->
                row.isNullAt(index)
                        ? NULL_BYTES
                        : longToBytes(
                                row.getTimestamp(index, localZonedTimestampType.getPrecision())
                                        .getMillisecond());
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(ArrayType arrayType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {

                BiFunction<DataGetters, Integer, byte[]> function =
                        arrayType.getElementType().accept(this);
                InternalArray internalArray = row.getArray(index);

                int count = 0;
                byte[][] bytes = new byte[internalArray.size()][];
                for (int i = 0; i < internalArray.size(); i++) {
                    bytes[i] = function.apply(internalArray, i);
                    count += bytes[i].length;
                }

                byte[] result = new byte[count];
                int position = 0;
                for (int i = 0; i < internalArray.size(); i++) {
                    System.arraycopy(bytes[i], 0, result, position, bytes[i].length);
                    position += bytes[i].length;
                }
                return result;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(MultisetType multisetType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {

                BiFunction<DataGetters, Integer, byte[]> function =
                        multisetType.getElementType().accept(this);
                InternalMap map = row.getMap(index);

                int count = 0;
                byte[][] bytes = new byte[map.size()][];
                for (int i = 0; i < map.size(); i++) {
                    bytes[i] = function.apply(map.keyArray(), i);
                    count += bytes[i].length;
                }

                byte[] result = new byte[count];
                int position = 0;
                for (int i = 0; i < map.size(); i++) {
                    System.arraycopy(bytes[i], 0, result, position, bytes[i].length);
                    position += bytes[i].length;
                }
                return result;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(MapType mapType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {

                BiFunction<DataGetters, Integer, byte[]> keyFunction =
                        mapType.getKeyType().accept(this);
                BiFunction<DataGetters, Integer, byte[]> valueFunction =
                        mapType.getValueType().accept(this);

                InternalMap map = row.getMap(index);

                int count = 0;
                byte[][] keyBytes = new byte[map.size()][];
                for (int i = 0; i < map.size(); i++) {
                    keyBytes[i] = keyFunction.apply(map.keyArray(), i);
                    count += keyBytes[i].length;
                }

                byte[][] valueBytes = new byte[map.size()][];
                for (int i = 0; i < map.size(); i++) {
                    valueBytes[i] = valueFunction.apply(map.valueArray(), i);
                    count += valueBytes[i].length;
                }

                byte[] result = new byte[count];
                int position = 0;
                for (int i = 0; i < map.size(); i++) {
                    System.arraycopy(keyBytes[i], 0, result, position, keyBytes[i].length);
                    position += keyBytes[i].length;
                    System.arraycopy(valueBytes[i], 0, result, position, valueBytes[i].length);
                    position += valueBytes[i].length;
                }
                return result;
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(RowType rowType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {

                // error here
                BiFunction<DataGetters, Integer, byte[]> function = rowType.accept(this);
                InternalRow secondRow = row.getRow(index, rowType.getFieldCount());

                int count = 0;
                byte[][] bytes = new byte[rowType.getFieldCount()][];
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    bytes[i] = function.apply(secondRow, i);
                    count += bytes[i].length;
                }

                byte[] result = new byte[count];
                int position = 0;
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    System.arraycopy(bytes[i], 0, result, position, bytes[i].length);
                    position += bytes[i].length;
                }
                return result;
            }
        };
    }

    private byte[] longToBytes(long x) {
        return new byte[] {
            (byte) (x & 0xff),
            (byte) (x >> 8 & 0xff),
            (byte) (x >> 16 & 0xff),
            (byte) (x >> 24 & 0xff),
            (byte) (x >> 32 & 0xff),
            (byte) (x >> 40 & 0xff),
            (byte) (x >> 48 & 0xff),
            (byte) (x >> 56 & 0xff)
        };
    }

    private byte[] intToBytes(int x) {
        return new byte[] {
            (byte) (x & 0xff),
            (byte) (x >> 8 & 0xff),
            (byte) (x >> 16 & 0xff),
            (byte) (x >> 24 & 0xff)
        };
    }
}
