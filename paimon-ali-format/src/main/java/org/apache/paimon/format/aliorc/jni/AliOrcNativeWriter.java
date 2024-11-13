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

package org.apache.paimon.format.aliorc.jni;

import org.apache.paimon.format.common.writer.NativeWriter;
import org.apache.paimon.fs.PositionOutputStream;

/** Wrapper for Native AliOrc Writer. */
public class AliOrcNativeWriter extends NativeWriter {

    public AliOrcNativeWriter(
            PositionOutputStream stream,
            String orcSchema,
            String compressionType,
            int writeBatchSize,
            int compressionLevel,
            long stripeSize) {
        create(stream, orcSchema, compressionType, writeBatchSize, compressionLevel, stripeSize);
    }

    public native void create(
            PositionOutputStream outputStream,
            String orcSchema,
            String compressionType,
            int writeBatchSize,
            int compressionLevel,
            long stripeSize);

    public native void writeIpcBytes(long arrayAddress, long schemaAddress);

    public native void close();
}
