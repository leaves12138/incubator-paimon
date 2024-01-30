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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.IteratorResultIterator;
import org.apache.paimon.utils.Pool;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/** Provides a {@link FormatReaderFactory} for Avro records. */
public class AvroBulkFormat implements FormatReaderFactory {

    private static final long serialVersionUID = 1L;

    protected final RowType projectedRowType;

    private final List<Predicate> filters;

    public AvroBulkFormat(RowType projectedRowType, List<Predicate> filters) {
        this.projectedRowType = projectedRowType;
        this.filters = filters;
    }

    @Override
    public RecordReader<InternalRow> createReader(FileIO fileIO, Path file) throws IOException {
        return createAvroReader(fileIO, file);
    }

    @Override
    public RecordReader<InternalRow> createReader(FileIO fileIO, Path file, int poolSize)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    private RecordReader<InternalRow> createAvroReader(FileIO fileIO, Path path)
            throws IOException {
        DatumReader<InternalRow> datumReader = new AvroRowDatumReader(projectedRowType);
        SeekableInput in =
                new SeekableInputStreamWrapper(
                        fileIO.newInputStream(path), fileIO.getFileSize(path));

        try {
            DataFileReader<InternalRow> avroReader =
                    (DataFileReader<InternalRow>) DataFileReader.openReader(in, datumReader);
            //            if (filters != null && !filters.isEmpty()) {
            //                String serializedString = avroReader.getMetaString("index.bytes");
            //                if (!PredicateFilterUtil.checkPredicate(serializedString, filters)) {
            //                    IOUtils.closeQuietly(in);
            //                    return EmptyReader.EMPTY_READER;
            //                }
            //            }
            return new AvroReader(fileIO, path, avroReader);
        } catch (Throwable e) {
            IOUtils.closeQuietly(in);
            throw e;
        }
    }

    private class AvroReader implements RecordReader<InternalRow> {

        private final DataFileReader<InternalRow> reader;

        private final long end;
        private final Pool<Object> pool;

        private AvroReader(FileIO fileIO, Path path, DataFileReader<InternalRow> reader)
                throws IOException {
            this.reader = reader;
            this.reader.sync(0);
            this.end = fileIO.getFileSize(path);
            this.pool = new Pool<>(1);
            this.pool.add(new Object());
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            Object ticket;
            try {
                ticket = pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        "Interrupted while waiting for the previous batch to be consumed", e);
            }

            if (!readNextBlock()) {
                pool.recycler().recycle(ticket);
                return null;
            }

            Iterator<InternalRow> iterator = new AvroBlockIterator(reader.getBlockCount(), reader);
            return new IteratorResultIterator<>(iterator, () -> pool.recycler().recycle(ticket));
        }

        private boolean readNextBlock() throws IOException {
            // read the next block with reader,
            // returns true if a block is read and false if we reach the end of this split
            return reader.hasNext() && !reader.pastSync(end);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static class AvroBlockIterator implements Iterator<InternalRow> {

        private long numRecordsRemaining;
        private final DataFileReader<InternalRow> reader;

        private AvroBlockIterator(long numRecordsRemaining, DataFileReader<InternalRow> reader) {
            this.numRecordsRemaining = numRecordsRemaining;
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            return numRecordsRemaining > 0;
        }

        @Override
        public InternalRow next() {
            try {
                numRecordsRemaining--;
                // reader.next merely deserialize bytes in memory to java objects
                // and will not read from file
                // Do not reuse object, manifest file assumes no object reuse
                return reader.next(null);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Encountered exception when reading from avro format file", e);
            }
        }
    }
}
