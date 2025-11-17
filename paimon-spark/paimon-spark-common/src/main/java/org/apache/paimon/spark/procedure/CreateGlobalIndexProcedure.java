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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.globalindex.GlobaIndexBuilder;
import org.apache.paimon.index.globalindex.GlobalIndexFileHelper;
import org.apache.paimon.index.globalindex.GlobalIndexer;
import org.apache.paimon.index.globalindex.GlobalIndexerFactory;
import org.apache.paimon.index.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Procedure to build global index files via Spark. */
public class CreateGlobalIndexProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CreateGlobalIndexProcedure.class);

    private static final String BITMAP = "bitmap";

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.required("column", DataTypes.StringType),
                ProcedureParameter.optional("index_type", DataTypes.StringType),
                ProcedureParameter.optional("options", DataTypes.StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "written_partitions",
                                DataTypes.IntegerType,
                                false,
                                Metadata.empty()),
                        new StructField(
                                "written_shards", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField(
                                "written_index_files",
                                DataTypes.IntegerType,
                                false,
                                Metadata.empty()),
                        new StructField("indexed_rows", DataTypes.LongType, false, Metadata.empty())
                    });

    private static final Map<String, GlobalIndexTopologyBuilder> TOPOLOGY_BUILDERS =
            initTopologyBuilders();

    protected CreateGlobalIndexProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public String description() {
        return "Create global index files for a given column.";
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String column = args.getString(1);
        String indexType =
                args.isNullAt(2) ? BITMAP : args.getString(2).toLowerCase(Locale.ROOT).trim();
        String optionString = args.isNullAt(3) ? null : args.getString(3);

        GlobalIndexTopologyBuilder topologyBuilder = getTopologyBuilder(indexType);

        return modifyPaimonTable(
                tableIdent,
                t -> {
                    checkArgument(
                            t instanceof FileStoreTable,
                            "Only FileStoreTable supports global index creation.");
                    FileStoreTable table = (FileStoreTable) t;
                    checkArgument(
                            table.coreOptions().rowTrackingEnabled(),
                            "Table '%s' must enable 'row-tracking.enabled=true' before creating global index.",
                            tableIdent);

                    RowType rowType = table.rowType();
                    checkArgument(
                            rowType.containsField(column),
                            "Column '%s' does not exist in table '%s'.",
                            column,
                            tableIdent);

                    DataField indexField = rowType.getField(column);
                    RowType projectedRowType = rowType.project(Collections.singletonList(column));
                    RowType readRowType = SpecialFields.rowTypeWithRowTracking(projectedRowType);

                    HashMap<String, String> parsedOptions = new HashMap<>();
                    ProcedureUtils.putAllOptions(parsedOptions, optionString);
                    Options userOptions = Options.fromMap(parsedOptions);

                    Options tableOptions = new Options(table.options());
                    long rowsPerShard =
                            tableOptions.getLong("global-index.row_count_per_shard", 100_000L);
                    checkArgument(
                            rowsPerShard > 0,
                            "Option 'global-index.row_count_per_shard' must be greater than 0.");

                    GlobalIndexBuildContext context =
                            new GlobalIndexBuildContext(
                                    spark(),
                                    JavaSparkContext.fromSparkContext(spark().sparkContext()),
                                    table,
                                    indexField,
                                    projectedRowType,
                                    readRowType,
                                    rowsPerShard,
                                    userOptions,
                                    indexType,
                                    tableOptions);

                    GlobalIndexBuildResult result = topologyBuilder.build(context);
                    commit(table, result.serializedMessages);

                    return new InternalRow[] {
                        newInternalRow(
                                result.writtenPartitions,
                                result.writtenShards,
                                result.indexFileCount,
                                result.indexedRowCount)
                    };
                });
    }

    private void commit(FileStoreTable table, List<byte[]> serializedMessages) {
        if (serializedMessages.isEmpty()) {
            return;
        }
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> messages = new ArrayList<>(serializedMessages.size());
        try {
            for (byte[] serializedMessage : serializedMessages) {
                messages.add(serializer.deserialize(serializer.getVersion(), serializedMessage));
            }
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to commit global index files.", e);
        }
    }

    private static Map<String, GlobalIndexTopologyBuilder> initTopologyBuilders() {
        Map<String, GlobalIndexTopologyBuilder> builders = new HashMap<>();
        builders.put(BITMAP, new BitmapGlobalIndexTopologyBuilder());
        return builders;
    }

    private GlobalIndexTopologyBuilder getTopologyBuilder(String indexType) {
        GlobalIndexTopologyBuilder builder = TOPOLOGY_BUILDERS.get(indexType);
        if (builder == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported global index type '%s'. Supported types are %s.",
                            indexType, TOPOLOGY_BUILDERS.keySet()));
        }
        return builder;
    }

    public static ProcedureBuilder builder() {
        return new Builder<CreateGlobalIndexProcedure>() {
            @Override
            protected CreateGlobalIndexProcedure doBuild() {
                return new CreateGlobalIndexProcedure(tableCatalog());
            }
        };
    }

    // -------------------------------------------------------------
    // Context & Result
    // -------------------------------------------------------------

    private static class GlobalIndexBuildContext {
        private final SparkSession spark;
        private final JavaSparkContext javaSparkContext;
        private final FileStoreTable table;
        private final DataField indexField;
        private final RowType projectedRowType;
        private final RowType readRowType;
        private final long rowsPerShard;
        private final Options userOptions;
        private final String indexType;
        private final Options tableOptions;

        private GlobalIndexBuildContext(
                SparkSession spark,
                JavaSparkContext javaSparkContext,
                FileStoreTable table,
                DataField indexField,
                RowType projectedRowType,
                RowType readRowType,
                long rowsPerShard,
                Options userOptions,
                String indexType,
                Options tableOptions) {
            this.spark = spark;
            this.javaSparkContext = javaSparkContext;
            this.table = table;
            this.indexField = indexField;
            this.projectedRowType = projectedRowType;
            this.readRowType = readRowType;
            this.rowsPerShard = rowsPerShard;
            this.userOptions = userOptions;
            this.indexType = indexType;
            this.tableOptions = tableOptions;
        }
    }

    private static class GlobalIndexBuildResult {
        private final List<byte[]> serializedMessages;
        private final int writtenPartitions;
        private final int writtenShards;
        private final int indexFileCount;
        private final long indexedRowCount;

        private GlobalIndexBuildResult(
                List<byte[]> serializedMessages,
                int writtenPartitions,
                int writtenShards,
                int indexFileCount,
                long indexedRowCount) {
            this.serializedMessages = serializedMessages;
            this.writtenPartitions = writtenPartitions;
            this.writtenShards = writtenShards;
            this.indexFileCount = indexFileCount;
            this.indexedRowCount = indexedRowCount;
        }
    }

    // -------------------------------------------------------------
    // Topology builders
    // -------------------------------------------------------------

    private interface GlobalIndexTopologyBuilder {
        String indexType();

        GlobalIndexBuildResult build(GlobalIndexBuildContext context);
    }

    private static class BitmapGlobalIndexTopologyBuilder implements GlobalIndexTopologyBuilder {

        @Override
        public String indexType() {
            return BITMAP;
        }

        @Override
        public GlobalIndexBuildResult build(GlobalIndexBuildContext context) {
            // Read all data with _ROW_ID
            ReadBuilder readBuilder =
                    context.table.newReadBuilder().withReadType(context.readRowType);
            TableScan.Plan plan = readBuilder.newScan().plan();
            if (plan.splits().isEmpty()) {
                LOG.info("No data found for global index building.");
                return new GlobalIndexBuildResult(Collections.emptyList(), 0, 0, 0, 0L);
            }

            // Create RDD from splits
            JavaRDD<Split> splitsRDD =
                    context.javaSparkContext.parallelize(new ArrayList<>(plan.splits()));

            // Read and shuffle data by shard
            JavaRDD<RowWithShard> rowsWithShard = splitsRDD.flatMap(new ReadSplitFunction(context));

            // Group by shard and build index
            JavaRDD<ShardResult> shardResults =
                    rowsWithShard
                            .groupBy(RowWithShard::getShardId)
                            .map(new BuildShardIndexFunction(context));

            List<ShardResult> collected = shardResults.collect();
            List<byte[]> serializedMessages = new ArrayList<>();
            Set<BinaryRow> touchedPartitions = new java.util.HashSet<>();
            int writtenShards = 0;
            int indexFiles = 0;
            long indexedRows = 0L;
            for (ShardResult result : collected) {
                if (result == null || result.serializedMessage == null) {
                    continue;
                }
                serializedMessages.add(result.serializedMessage);
                if (result.partition != null) {
                    touchedPartitions.add(result.partition);
                }
                writtenShards++;
                indexFiles += result.indexFiles;
                indexedRows += result.indexedRows;
            }

            return new GlobalIndexBuildResult(
                    serializedMessages,
                    touchedPartitions.size(),
                    writtenShards,
                    indexFiles,
                    indexedRows);
        }
    }

    private static class RowWithShard implements Serializable {
        private final Object columnValue;
        private final long rowId;
        private final int shardId;
        private final BinaryRow partition;

        private RowWithShard(Object columnValue, long rowId, int shardId, BinaryRow partition) {
            this.columnValue = columnValue;
            this.rowId = rowId;
            this.shardId = shardId;
            this.partition = partition;
        }

        public int getShardId() {
            return shardId;
        }
    }

    private static class ShardResult implements Serializable {
        @Nullable private final byte[] serializedMessage;
        @Nullable private final BinaryRow partition;
        private final int indexFiles;
        private final long indexedRows;

        private ShardResult(
                @Nullable byte[] serializedMessage,
                @Nullable BinaryRow partition,
                int indexFiles,
                long indexedRows) {
            this.serializedMessage = serializedMessage;
            this.partition = partition;
            this.indexFiles = indexFiles;
            this.indexedRows = indexedRows;
        }
    }

    private static class ReadSplitFunction
            implements org.apache.spark.api.java.function.FlatMapFunction<Split, RowWithShard> {

        private final FileStoreTable table;
        private final RowType readRowType;
        private final long rowsPerShard;

        private ReadSplitFunction(GlobalIndexBuildContext context) {
            this.table = context.table;
            this.readRowType = context.readRowType;
            this.rowsPerShard = context.rowsPerShard;
        }

        @Override
        public java.util.Iterator<RowWithShard> call(Split split) throws Exception {
            TableRead read = table.newReadBuilder().withReadType(readRowType).newRead();
            RecordReader<org.apache.paimon.data.InternalRow> reader =
                    read.createReader(Collections.singletonList(split));

            List<RowWithShard> rows = new ArrayList<>();
            org.apache.paimon.data.InternalRow.FieldGetter valueGetter =
                    org.apache.paimon.data.InternalRow.createFieldGetter(
                            readRowType.getTypeAt(0), 0);
            int rowIdIndex = readRowType.getFieldCount() - 2;

            try {
                reader.forEachRemaining(
                        row -> {
                            Object key = valueGetter.getFieldOrNull(row);
                            long rowId = row.getLong(rowIdIndex);
                            int shardId = (int) (rowId / rowsPerShard);
                            BinaryRow partition =
                                    split instanceof DataSplit
                                            ? ((DataSplit) split).partition()
                                            : BinaryRow.EMPTY_ROW;
                            rows.add(new RowWithShard(key, rowId, shardId, partition));
                        });
            } finally {
                reader.close();
            }

            return rows.iterator();
        }
    }

    private static class BuildShardIndexFunction
            implements Function<scala.Tuple2<Integer, Iterable<RowWithShard>>, ShardResult> {

        private final DataField indexField;
        private final FileStoreTable table;
        private final Options userOptions;
        private final String indexType;

        private BuildShardIndexFunction(GlobalIndexBuildContext context) {
            this.indexField = context.indexField;
            this.table = context.table;
            Options merged = new Options(context.tableOptions.toMap());
            context.userOptions.toMap().forEach(merged::setString);
            this.userOptions = merged;
            this.indexType = context.indexType;
        }

        @Override
        public ShardResult call(scala.Tuple2<Integer, Iterable<RowWithShard>> shardData)
                throws Exception {
            int shardId = shardData._1;
            Iterable<RowWithShard> rows = shardData._2;

            GlobalIndexerFactory factory = GlobalIndexerFactoryUtils.load(indexType);
            GlobalIndexer globalIndexer =
                    factory.create(indexField.type(), new Options(userOptions.toMap()));

            // Determine partition (use the first row's partition)
            BinaryRow partition = null;
            for (RowWithShard row : rows) {
                if (partition == null) {
                    partition = row.partition;
                }
                break;
            }
            if (partition == null) {
                partition = BinaryRow.EMPTY_ROW;
            }

            FileIO fileIO = table.fileIO();
            GlobalIndexFileHelper fileHelper =
                    new GlobalIndexFileHelper(
                            fileIO, table.store().pathFactory().indexFileFactory(partition, 0));
            GlobaIndexBuilder builder = globalIndexer.createBuilder(fileHelper);

            long rowCount = 0;
            for (RowWithShard row : rows) {
                builder.indexTo(row.columnValue, row.rowId);
                rowCount++;
            }

            List<Pair<String, byte[]>> files = builder.end();
            if (files.isEmpty()) {
                return null;
            }

            List<IndexFileMeta> metas = new ArrayList<>();
            for (Pair<String, byte[]> file : files) {
                String fileName = file.getLeft();
                long fileSize = fileIO.getFileSize(fileHelper.filePath(fileName));
                metas.add(
                        new IndexFileMeta(
                                indexType,
                                fileName,
                                fileSize,
                                rowCount,
                                shardId,
                                indexField.id(),
                                file.getRight()));
            }

            CommitMessageImpl commitMessage =
                    new CommitMessageImpl(
                            partition,
                            0,
                            null,
                            org.apache.paimon.io.DataIncrement.indexIncrement(metas),
                            org.apache.paimon.io.CompactIncrement.emptyIncrement());

            CommitMessageSerializer serializer = new CommitMessageSerializer();
            byte[] serialized = serializer.serialize(commitMessage);
            return new ShardResult(serialized, partition.copy(), metas.size(), rowCount);
        }
    }
}
