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

package org.apache.paimon.format.parquet.jni;

import org.apache.paimon.format.common.reader.NativeReader;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;

/** Parquet native reader. */
public class AliParquetNativeReader extends NativeReader {

    public AliParquetNativeReader(
            SeekableInputStream inputStream,
            long fileLength,
            String selectedFields,
            int batchSize,
            boolean multiThread) {
        this.create(inputStream, fileLength, selectedFields, batchSize, multiThread);
    }

    public native void create(
            SeekableInputStream seekableInputStream,
            long fileLength,
            String selectedFields,
            int batchSize,
            boolean multiThread);

    public native void readBatch(long arrayAddress, long schemaAddress) throws IOException;

    public native void close();
}
