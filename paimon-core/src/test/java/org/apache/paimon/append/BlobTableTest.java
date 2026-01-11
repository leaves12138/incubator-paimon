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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for table with blob. */
public class BlobTableTest extends TableTestBase {

    private final byte[] blobBytes = randomBytes();

    @Test
    public void testBasic() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(1000, 1));

        AtomicInteger integer = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                DataEvolutionSplitRead.splitFieldBunches(filesMetas, key -> 0);

        assertThat(fieldGroups.size()).isEqualTo(2);
        assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
        assertThat(fieldGroups.get(1).files().size()).isEqualTo(10);

        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    }
                });

        assertThat(integer.get()).isEqualTo(1000);
    }

    @Test
    public void testWriteByInputStream() throws Exception {
        createTableDefault();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(blobBytes);

        GenericRow genericRow = new GenericRow(3);
        genericRow.setField(0, 0);
        genericRow.setField(1, BinaryString.fromString("nice"));
        genericRow.setField(
                2, Blob.fromInputStream(() -> SeekableInputStream.wrap(byteArrayInputStream)));

        writeDataDefault(Collections.singletonList(genericRow));

        readDefault(row -> assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes));
    }

    @Test
    public void testMultiBatch() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(1000, 2));

        AtomicInteger integer = new AtomicInteger(0);

        List<DataFileMeta> filesMetas =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        List<List<DataFileMeta>> batches = DataEvolutionSplitRead.mergeRangesAndSort(filesMetas);
        assertThat(batches.size()).isEqualTo(2);
        for (List<DataFileMeta> batch : batches) {
            List<DataEvolutionSplitRead.FieldBunch> fieldGroups =
                    DataEvolutionSplitRead.splitFieldBunches(batch, file -> 0);
            assertThat(fieldGroups.size()).isEqualTo(2);
            assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
            assertThat(fieldGroups.get(1).files().size()).isEqualTo(10);
        }

        readDefault(
                row -> {
                    if (integer.getAndIncrement() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    }
                });
        assertThat(integer.get()).isEqualTo(2000);
    }

    @Test
    public void testRowIdPushDown() throws Exception {
        createTableDefault();
        writeDataDefault(
                new Iterable<InternalRow>() {
                    @Nonnull
                    @Override
                    public Iterator<InternalRow> iterator() {
                        return new Iterator<InternalRow>() {
                            int i = 0;

                            @Override
                            public boolean hasNext() {
                                return i < 200;
                            }

                            @Override
                            public InternalRow next() {
                                i++;
                                return (i - 1) == 100
                                        ? GenericRow.of(
                                                i,
                                                BinaryString.fromString("nice"),
                                                new BlobData(
                                                        "This is the specified message".getBytes()))
                                        : dataDefault(0, 0);
                            }
                        };
                    }
                });

        Table table = getTableDefault();
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withRowRanges(Collections.singletonList(new Range(100L, 100L)));
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        AtomicInteger i = new AtomicInteger(0);
        reader.forEachRemaining(
                row -> {
                    i.getAndIncrement();
                    assertThat(row.getBlob(2).toData())
                            .isEqualTo("This is the specified message".getBytes());
                });
        assertThat(i.get()).isEqualTo(1);
    }

    @Test
    public void testRolling() throws Exception {
        createTableDefault();
        commitDefault(writeDataDefault(1025, 1));
        AtomicInteger integer = new AtomicInteger(0);
        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes);
                    }
                });
        assertThat(integer.get()).isEqualTo(1025);
    }

    @Test
    public void testOverwriteFailed() throws Exception {

        createTableDefault();
        FileStoreTable table = getTableDefault();

        commitDefault(writeDataDefault(1, 1));

        TableWriteImpl tableWrite = table.newWrite("test");

        tableWrite.write(
                GenericRow.of(0, BinaryString.fromString("nice"), new BlobData(blobBytes)));

        List<CommitMessage> commitMessages = tableWrite.prepareCommit();

        FileStoreCommitImpl fileStoreCommit =
                (FileStoreCommitImpl) table.store().newCommit("test", table);
        ManifestCommittable committable = new ManifestCommittable(Long.MAX_VALUE);
        commitMessages.forEach(committable::addFileCommittable);
        FileStoreCommitImpl.tryAgain = true;
        fileStoreCommit.overwritePartition(
                Collections.singletonMap("f0", "0"), committable, Collections.emptyMap());
        List<Snapshot> snapshots = new ArrayList<>();
        table.snapshotManager().snapshots().forEachRemaining(snapshots::add);

        Snapshot latestSnapshot = table.store().snapshotManager().latestSnapshot();

        List<ManifestFileMeta> metas =
                table.newSnapshotReader()
                        .manifestsReader()
                        .read(latestSnapshot, ScanMode.ALL)
                        .allManifests;

        for (ManifestFileMeta meta : metas) {
            List<ManifestEntry> entries = new ArrayList<>();
            entries.addAll(table.store().newScan().readManifest(meta));

            FileEntry.mergeEntries(entries);
        }

        List<Partition> partitions = new PartitionsTable.PartitionsRead(table).listPartitions();

        return;
    }

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.partitionKeys("f0");
        return schemaBuilder.build();
    }

    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(
                RANDOM.nextInt(), BinaryString.fromBytes(randomBytes()), new BlobData(blobBytes));
    }

    @Override
    protected byte[] randomBytes() {
        byte[] binary = new byte[2 * 1024 * 124];
        RANDOM.nextBytes(binary);
        return binary;
    }
}
