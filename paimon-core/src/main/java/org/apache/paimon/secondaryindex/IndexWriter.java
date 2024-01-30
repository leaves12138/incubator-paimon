/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.paimon.secondaryindex;
//
// import org.apache.paimon.CoreOptions;
// import org.apache.paimon.data.DataGetters;
// import org.apache.paimon.data.InternalRow;
// import org.apache.paimon.filter.FilterInterface;
// import org.apache.paimon.types.RowType;
//
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.function.BiFunction;
//
/// ** Index file writer. */
// public final class IndexWriter {
//
//    private CoreOptions.IndexType indexType;
//
//    InternalRowToBytesVisitor internalRowToBytesVisitor = new InternalRowToBytesVisitor();
//
//    List<FilterInterface> filters = new ArrayList<>();
//
//    List<BiFunction<DataGetters, Integer, byte[]>> getters = new ArrayList<>();
//
//    private int[] indexes;
//
//    public IndexWriter(
//            RowType rowType, List<String> indexColumns, CoreOptions.IndexType indexType) {
//
//    }
//
//    public void write(InternalRow row) {
//        for (int i = 0; i < indexes.length; i++) {
//            int index = indexes[i];
//            FilterInterface filterInterface = filters.get(i);
//            BiFunction<DataGetters, Integer, byte[]> getter = getters.get(i);
//            byte[] b = getter.apply(row, index);
//            filterInterface.add(b);
//        }
//    }
//
//    public String getIndexType() {
//        return indexType.name();
//    }
//
//    public String getSerializedString() {
//        Map<String, byte[]> indexMap = new HashMap<>();
//
//        return "";
//    }
//
//    private class IndexMaintainer {
//
//        private String columnName;
//        private int columnIndex;
//        private FilterInterface filter;
//        BiFunction<DataGetters, Integer, byte[]> getter;
//
//        public void write(InternalRow row) {
//            filter.add(getter.apply(row, columnIndex));
//        }
//
//        public String getColumnName() {
//            return columnName;
//        }
//
//        public String getSerializedString() {
//            return filter.serializeToString();
//        }
//    }
// }
