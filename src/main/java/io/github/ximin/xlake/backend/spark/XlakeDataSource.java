/*-
 * #%L
 * xlake-demo
 * %%
 * Copyright (C) 2026 ximin1024
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.github.ximin.xlake.backend.spark;

import io.github.ximin.xlake.backend.read.DataBlockListProvider;
import io.github.ximin.xlake.backend.read.DriverBlockListProvider;
import io.github.ximin.xlake.backend.read.LocalTableStoreBlockListProvider;
import io.github.ximin.xlake.backend.read.ReadPruning;
import io.github.ximin.xlake.backend.routing.*;
import io.github.ximin.xlake.backend.spark.routing.*;
import io.github.ximin.xlake.storage.DynamicMmapStore;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.table.DynamicTableInfo;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.table.Snapshot;
import io.github.ximin.xlake.table.TableMeta;
import io.github.ximin.xlake.table.op.KvScan;
import io.github.ximin.xlake.table.op.Scan;
import io.github.ximin.xlake.table.record.RecordView;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class XlakeDataSource implements TableProvider, DataSourceRegister {
    public static final String OPTION_PATH = "path";
    public static final String OPTION_TABLE = "table";
    public static final String OPTION_STORE_ID = "storeId";
    public static final String KEY_COLUMN = "key";
    public static final String VALUE_COLUMN = "value";
    static final boolean ASSUME_PARTITION_IS_SHARD_DEFAULT = false;

    private static final StructType DEFAULT_SCHEMA = new StructType(new StructField[]{
            new StructField(KEY_COLUMN, DataTypes.StringType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN, DataTypes.StringType, true, Metadata.empty())
    });

    public static boolean isXlakeTable(Table table) {
        return table instanceof XlakeSparkTable;
    }

    public static boolean isXlakeWrite(Write write) {
        return write instanceof XlakeBatchWrite;
    }

    public static int resolveShardCount(SparkConf conf) {
        Objects.requireNonNull(conf, "conf must not be null");
        return conf.getInt("spark.xlake.routing.shardCount", 1);
    }

    @Override
    public String shortName() {
        return "xlake";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return DEFAULT_SCHEMA;
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitions, Map<String, String> properties) {
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
        String basePath = required(options, OPTION_PATH);
        String tableIdentifier = required(options, OPTION_TABLE);
        String storeId = options.getOrDefault(OPTION_STORE_ID, "spark");
        StructType effectiveSchema = schema != null ? schema : DEFAULT_SCHEMA;
        return new XlakeSparkTable(basePath, storeId, tableIdentifier, effectiveSchema);
    }

    private static String required(CaseInsensitiveStringMap options, String key) {
        String value = options.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required option: " + key);
        }
        return value;
    }

    private record XlakeSparkTable(String basePath, String storeId, String tableIdentifier,
                                      StructType schema)
            implements Table, SupportsRead, SupportsWrite {

        @Override
        public String name() {
            return tableIdentifier;
        }

        @Override
        public Set<TableCapability> capabilities() {
            return Set.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
        }

        @Override
        public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
            return new XlakeScanBuilder(basePath, storeId, tableIdentifier, schema);
        }

        @Override
        public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
            return new XlakeWriteBuilder(basePath, storeId, tableIdentifier, info.schema());
        }
    }

    private static final class XlakeScanBuilder implements ScanBuilder,
            org.apache.spark.sql.connector.read.Scan,
            Batch,
            SupportsPushDownFilters {
        private final String basePath;
        private final String storeId;
        private final String tableIdentifier;
        private final StructType schema;

        private final DataBlockListProvider blockListProvider;
        private org.apache.spark.sql.sources.Filter[] filtersForPruning = new org.apache.spark.sql.sources.Filter[0];

        private XlakeScanBuilder(String basePath, String storeId, String tableIdentifier, StructType schema) {
            this(basePath, storeId, tableIdentifier, schema, resolveBlockListProvider());
        }

        private static DataBlockListProvider resolveBlockListProvider() {
            // planInputPartitions() runs on driver; executors use local provider for reader-side access.
            if (SparkEnv.get() != null && "driver".equals(SparkEnv.get().executorId())) {
                return new DriverBlockListProvider();
            }
            return new LocalTableStoreBlockListProvider();
        }

        private XlakeScanBuilder(String basePath, String storeId, String tableIdentifier, StructType schema,
                                    DataBlockListProvider blockListProvider) {
            this.basePath = basePath;
            this.storeId = storeId;
            this.tableIdentifier = tableIdentifier;
            this.schema = schema;
            this.blockListProvider = blockListProvider;
        }

        @Override
        public org.apache.spark.sql.connector.read.Scan build() {
            return this;
        }

        @Override
        public StructType readSchema() {
            return schema;
        }

        @Override
        public Batch toBatch() {
            return this;
        }

        @Override
        public InputPartition[] planInputPartitions() {
            List<DataBlock> allBlocks = blockListProvider.listBlocks(basePath, storeId, tableIdentifier);
            ReadPruning.KeyPruningSpec pruning = ReadPruning.extractKeyPruningSpec(filtersForPruning, KEY_COLUMN);
            List<DataBlock> hitBlocks = ReadPruning.pruneBlocks(allBlocks, pruning);
            if (hitBlocks.isEmpty()) {
                return new InputPartition[0];
            }

            // 按 DataBlock.host 分组，每个 host 一个 InputPartition，确保 block 不会被重复读取。
            LinkedHashMap<String, List<DataBlock>> byHost = new LinkedHashMap<>();
            for (DataBlock block : hitBlocks) {
                String host = ReadPruning.preferredHost(block);
                byHost.computeIfAbsent(host, ignored -> new ArrayList<>()).add(block);
            }

            ArrayList<InputPartition> partitions = new ArrayList<>(byHost.size());
            for (Map.Entry<String, List<DataBlock>> e : byHost.entrySet()) {
                String host = e.getKey();
                String[] preferred = host == null ? new String[0] : new String[]{host};
                partitions.add(new XlakeInputPartition(preferred, e.getValue()));
            }
            return partitions.toArray(InputPartition[]::new);
        }

        @Override
        public PartitionReaderFactory createReaderFactory() {
            ReadPruning.KeyPruningSpec pruning = ReadPruning.extractKeyPruningSpec(filtersForPruning, KEY_COLUMN);
            return new XlakePartitionReaderFactory(basePath, storeId, tableIdentifier, schema, pruning.keyRange());
        }

        @Override
        public org.apache.spark.sql.sources.Filter[] pushFilters(org.apache.spark.sql.sources.Filter[] filters) {
            // 仅用于 block 选择/范围裁剪；不声明 pushdown，让 Spark 继续在上层执行过滤，保证语义正确。
            this.filtersForPruning = filters == null ? new org.apache.spark.sql.sources.Filter[0] : filters;
            return filters;
        }

        @Override
        public org.apache.spark.sql.sources.Filter[] pushedFilters() {
            return new org.apache.spark.sql.sources.Filter[0];
        }
    }

    private record XlakeInputPartition(String[] preferredHosts,
                                          List<DataBlock> blocks) implements InputPartition {
        @Override
        public String[] preferredLocations() {
            return preferredHosts == null ? new String[0] : preferredHosts;
        }
    }

    private record XlakePartitionReaderFactory(String basePath, String storeId, String tableIdentifier,
                                                  StructType schema,
                                                  DataBlock.KeyRange keyRange)
            implements PartitionReaderFactory {

        @Override
        public PartitionReader<InternalRow> createReader(InputPartition partition) {
            XlakeInputPartition p = (XlakeInputPartition) partition;
            // 对 LMDB block：只允许在 preferred host 上读取（避免跨节点重复/失败读取）。
            enforceLocalLmdbRead(p);
            DynamicMmapStore store = DynamicMmapStore.getInstance(basePath, storeId);
            TableStore tableStore = store.tableStore(tableIdentifier);
            List<InternalRow> rows = readAll(tableStore, p.blocks());
            return new XlakePartitionReader(rows);
        }

        private void enforceLocalLmdbRead(XlakeInputPartition partition) {
            if (partition == null || partition.preferredHosts() == null || partition.preferredHosts().length == 0) {
                return;
            }
            List<DataBlock> blocks = partition.blocks();
            if (blocks == null || blocks.isEmpty()) {
                return;
            }
            boolean hasLmdb = blocks.stream().anyMatch(b -> b != null && b.format() == DataBlock.Format.LMDB);
            if (!hasLmdb) {
                return;
            }
            String expectedHost = partition.preferredHosts()[0];
            String actualHost = null;
            try {
                if (SparkEnv.get() != null && SparkEnv.get().rpcEnv() != null && SparkEnv.get().rpcEnv().address() != null) {
                    actualHost = SparkEnv.get().rpcEnv().address().host();
                }
            } catch (Throwable ignored) {
            }
            if (actualHost == null) {
                return;
            }
            if (!expectedHost.equals(actualHost)) {
                throw new IllegalStateException("LMDB blocks must be read on preferred host. expected=" + expectedHost + ", actual=" + actualHost);
            }
        }

        private List<InternalRow> readAll(TableStore tableStore, List<DataBlock> blocks) {
            XlakeTable tableRef = new SparkXlakeTableRef(tableIdentifier);
            KvScan scan = KvScan.builder()
                    .withTable(tableRef)
                    .withDataBlocks(blocks == null ? List.of() : blocks)
                    .withKeyRange(keyRange)
                    .withProjections(List.of())
                    .build();
            try {
                Scan.Result result = (Scan.Result) tableStore.read(scan);
                if (!result.success()) {
                    return List.of();
                }
                return result.data().stream()
                        .filter(RecordView.class::isInstance)
                        .map(RecordView.class::cast)
                        .map(this::toRow)
                        .toList();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private InternalRow toRow(RecordView view) {
            Object[] values = new Object[2];
            values[0] = UTF8String.fromBytes(view.key());
            byte[] rawValue = view.value();
            values[1] = rawValue == null ? null : UTF8String.fromBytes(rawValue);
            return new GenericInternalRow(values);
        }
    }

    private static final class XlakePartitionReader implements PartitionReader<InternalRow> {
        private final List<InternalRow> rows;
        private int index = -1;

        private XlakePartitionReader(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public boolean next() {
            index++;
            return index < rows.size();
        }

        @Override
        public InternalRow get() {
            return rows.get(index);
        }

        @Override
        public void close() {
        }
    }

    private record XlakeWriteBuilder(String basePath, String storeId, String tableIdentifier,
                                        StructType schema) implements WriteBuilder {

        @Override
        public BatchWrite buildForBatch() {
            return new XlakeBatchWrite(basePath, storeId, tableIdentifier, schema);
        }
    }

    private record XlakeBatchWrite(String basePath, String storeId, String tableIdentifier,
                                      StructType schema) implements BatchWrite, RequiresDistributionAndOrdering {

        @Override
        public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
            return new XlakeDataWriterFactory(basePath, storeId, tableIdentifier, schema);
        }

        @Override
        public Distribution requiredDistribution() {
            int shardCount = SparkEnv.get().conf().getInt("spark.xlake.routing.shardCount", 1);
            if (shardCount <= 1) {
                return Distributions.unspecified();
            }
            // Let Spark shuffle into shardCount buckets on key to make each task handle (mostly) one shard.
            return Distributions.clustered(new org.apache.spark.sql.connector.expressions.Expression[]{
                    Expressions.bucket(shardCount, "key")
            });
        }

        @Override
        public boolean distributionStrictlyRequired() {
            return true;
        }

        @Override
        public int requiredNumPartitions() {
            return SparkEnv.get().conf().getInt("spark.xlake.routing.shardCount", 1);
        }

        @Override
        public SortOrder[] requiredOrdering() {
            return new SortOrder[0];
        }

        @Override
        public void commit(WriterCommitMessage[] messages) {
        }

        @Override
        public void abort(WriterCommitMessage[] messages) {
        }
    }

    private record XlakeDataWriterFactory(String basePath, String storeId, String tableIdentifier,
                                             StructType schema) implements DataWriterFactory {

        @Override
        public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
            DynamicMmapStore store = DynamicMmapStore.getInstance(basePath, storeId);
            return new XlakeDataWriter(store, basePath, storeId, tableIdentifier, partitionId);
        }
    }

    private static final class XlakeDataWriter implements DataWriter<InternalRow> {
        private static final Logger log = LoggerFactory.getLogger(XlakeDataWriter.class);
        private final DynamicMmapStore store;
        private final String basePath;
        private final String storeId;
        private final String tableIdentifier;
        private final int partitionId;
        private long count;
        private final int shardCount;
        private final boolean assumePartitionIsShard;
        private final int maxBatchRows;
        private final SparkMurmur3ShardResolver shardResolver;
        private final RoutingWriteClient routingWriteClient;
        private final ShardOwnerCache shardOwnerCache;
        private final String currentExecutorId;
        private final String currentNodeId;
        // Stable within one logical task attempt sequence so Spark task retry/speculation can
        // regenerate the same batchId for the same logical flush boundary. The write path below
        // routes every successful record through an executor endpoint deduper using this id.
        private long flushSequence = 0L;

        // Buffer by shardId so we can do driver lookup/forward in batch.
        private final HashMap<Integer, KvBatch> buffers = new HashMap<>();
        private int bufferedRows = 0;

        private XlakeDataWriter(
                DynamicMmapStore store,
                String basePath,
                String storeId,
                String tableIdentifier,
                int partitionId
        ) {
            this.store = store;
            this.basePath = basePath;
            this.storeId = storeId;
            this.tableIdentifier = tableIdentifier;
            this.partitionId = partitionId;
            this.shardCount = SparkEnv.get().conf().getInt("spark.xlake.routing.shardCount", 1);
            this.assumePartitionIsShard = SparkEnv.get().conf()
                    .getBoolean("spark.xlake.routing.write.assumePartitionIsShard", ASSUME_PARTITION_IS_SHARD_DEFAULT);
            this.maxBatchRows = SparkEnv.get().conf()
                    .getInt("spark.xlake.routing.write.batchRows", 1024);
            this.shardResolver = new SparkMurmur3ShardResolver(shardCount);
            this.routingWriteClient = new RoutingWriteClient();
            this.shardOwnerCache = new ShardOwnerCache(routingWriteClient::lookupOwners);
            this.currentExecutorId = SparkEnv.get().executorId();
            this.currentNodeId = resolveCurrentNodeId();
        }

        @Override
        public void write(InternalRow record) {
            String key = record.getUTF8String(0).toString();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            // Preserve null semantics: do NOT encode null as empty string.
            byte[] valueBytes = record.isNullAt(1) ? null : record.getUTF8String(1).getBytes();
            int shardId = resolveShardId(keyBytes);

            KvBatch batch = buffers.computeIfAbsent(shardId, ignored -> new KvBatch());
            batch.add(keyBytes, valueBytes);
            bufferedRows++;

            if (bufferedRows >= maxBatchRows) {
                flushBatches();
            }
        }

        @Override
        public WriterCommitMessage commit() {
            flushBatches();
            return new XlakeWriterCommitMessage(count);
        }

        @Override
        public void abort() {
        }

        @Override
        public void close() {
            flushBatches();
        }

        private int resolveShardId(byte[] keyBytes) {
            if (assumePartitionIsShard && shardCount > 1 && partitionId >= 0 && partitionId < shardCount) {
                return partitionId;
            }
            return shardResolver.resolve(keyBytes).value();
        }

        private void flushBatches() {
            if (buffers.isEmpty()) {
                return;
            }

            long logicalFlushSequence = flushSequence++;
            // Snapshot & clear first: writer may be called again if Spark retries.
            HashMap<Integer, KvBatch> pending = new HashMap<>(buffers);
            buffers.clear();
            bufferedRows = 0;

            // Lookup routing for involved shards (task-level cache; batch lookup missing shards only).
            Map<Integer, ShardLookupResult> lookups = shardOwnerCache.get(pending.keySet());
            XlakeTable tableRef = new SparkXlakeTableRef(tableIdentifier);
            // Use a deterministic logical batch id instead of a random UUID. We intentionally keep
            // owner-local writes on the executor-endpoint path as well, so every successful write
            // shares the same dedupe contract across same-executor, same-node, and driver fallback.
            String batchId = StableBatchId.forPendingBatches(tableIdentifier, partitionId, logicalFlushSequence, pending);

            ArrayList<Integer> remoteShardIds = new ArrayList<>();
            ArrayList<Long> remoteEpochs = new ArrayList<>();
            ArrayList<byte[]> remoteKeys = new ArrayList<>();
            ArrayList<byte[]> remoteValues = new ArrayList<>();

            for (Map.Entry<Integer, KvBatch> e : pending.entrySet()) {
                int shardId = e.getKey();
                KvBatch batch = e.getValue();
                ShardLookupResult lookup = lookups.get(shardId);
                if (lookup == null || lookup.status() != RoutingStatus.ASSIGNED) {
                    throw new IllegalStateException("Routing not ready for shard " + shardId + ": " + (lookup == null ? "null" : lookup.status()));
                }
                ShardAssignment assignment = lookup.assignment();
                if (assignment == null) {
                    throw new IllegalStateException("Missing assignment for shard " + shardId);
                }

                if (RoutingWriteClient.canFastPath(lookup, currentExecutorId) || isSameNode(assignment)) {
                    RoutingMessages.ExecutorEndpointInfo endpointInfo =
                            routingWriteClient.lookupExecutorEndpoint(assignment.executorId());
                    try {
                        RoutingMessages.WriteAck directAck = routingWriteClient.directForwardToExecutor(
                                endpointInfo,
                                new RoutingMessages.ShardWriteForward(
                                        basePath,
                                        storeId,
                                        tableIdentifier,
                                        shardId,
                                        assignment.epoch().value(),
                                        batchId,
                                        batch.keysSlice(),
                                        batch.valuesSlice()
                                )
                        );
                        if (directAck.status() == RoutingMessages.WriteAckStatus.OK) {
                            count += batch.size;
                            continue;
                        }
                    } catch (Exception ex) {
                        log.debug("fast-path direct forward failed for shard {}, falling back", shardId, ex);
                    }
                    long epoch = assignment.epoch().value();
                    for (int i = 0; i < batch.size; i++) {
                        remoteShardIds.add(shardId);
                        remoteEpochs.add(epoch);
                        remoteKeys.add(batch.keys[i]);
                        remoteValues.add(batch.values[i]);
                    }
                    continue;
                }

                long epoch = assignment.epoch().value();
                for (int i = 0; i < batch.size; i++) {
                    remoteShardIds.add(shardId);
                    remoteEpochs.add(epoch);
                    remoteKeys.add(batch.keys[i]);
                    remoteValues.add(batch.values[i]);
                }
            }

            if (remoteShardIds.isEmpty()) {
                return;
            }

            int[] shardIdsArr = new int[remoteShardIds.size()];
            long[] epochsArr = new long[remoteEpochs.size()];
            byte[][] keysArr = new byte[remoteKeys.size()][];
            byte[][] valuesArr = new byte[remoteValues.size()][];
            for (int i = 0; i < remoteShardIds.size(); i++) {
                shardIdsArr[i] = remoteShardIds.get(i);
                epochsArr[i] = remoteEpochs.get(i);
                keysArr[i] = remoteKeys.get(i);
                valuesArr[i] = remoteValues.get(i);
            }

            RoutingMessages.WriteAck ack = routingWriteClient.forwardMultiShard(
                    basePath,
                    storeId,
                    tableIdentifier,
                    shardIdsArr,
                    epochsArr,
                    batchId,
                    keysArr,
                    valuesArr
            );
            if (ack.status() == RoutingMessages.WriteAckStatus.OK) {
                count += remoteShardIds.size();
                return;
            }
            if (ack.status() == RoutingMessages.WriteAckStatus.ERROR) {
                throw new IllegalStateException("Forward write failed: " + ack.message());
            }

            // RETRY / STALE_EPOCH: keep the same logical batchId so retries that land on the same
            // owner executor can hit the same dedupe window. Owner migration after STALE_EPOCH is
            // still a routing handoff, not a cross-executor global dedupe guarantee.
            for (int attempt = 1; attempt <= 3; attempt++) {
                try {
                    Thread.sleep(50L * attempt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while retrying forward", e);
                }
                Map<Integer, ShardLookupResult> refreshed = shardOwnerCache.refresh(pending.keySet());
                for (int i = 0; i < shardIdsArr.length; i++) {
                    ShardLookupResult refreshedLookup = refreshed.get(shardIdsArr[i]);
                    if (refreshedLookup != null && refreshedLookup.status() == RoutingStatus.ASSIGNED
                            && refreshedLookup.assignment() != null) {
                        epochsArr[i] = refreshedLookup.assignment().epoch().value();
                    }
                }
                RoutingMessages.WriteAck retryAck = routingWriteClient.forwardMultiShard(
                        basePath,
                        storeId,
                        tableIdentifier,
                        shardIdsArr,
                        epochsArr,
                        batchId,
                        keysArr,
                        valuesArr
                );
                if (retryAck.status() == RoutingMessages.WriteAckStatus.OK) {
                    count += remoteShardIds.size();
                    return;
                }
                if (retryAck.status() == RoutingMessages.WriteAckStatus.ERROR) {
                    throw new IllegalStateException("Forward write failed: " + retryAck.message());
                }
            }
            throw new IllegalStateException("Forward write failed after retries: " + ack.status() + ", " + ack.message());
        }

        private boolean isSameNode(ShardAssignment assignment) {
            return assignment != null
                    && assignment.nodeSlot() != null
                    && currentNodeId != null
                    && currentNodeId.equals(assignment.nodeSlot().nodeId());
        }

        private static String resolveCurrentNodeId() {
            if (SparkEnv.get() == null || SparkEnv.get().rpcEnv() == null || SparkEnv.get().rpcEnv().address() == null) {
                return null;
            }
            return SparkEnv.get().rpcEnv().address().host();
        }
    }

    private record XlakeWriterCommitMessage(
            long count) implements WriterCommitMessage {
    }

    private static final class KvBatch implements StableBatchId.BatchLike {
        private byte[][] keys = new byte[128][];
        private byte[][] values = new byte[128][];
        private int size = 0;

        private void add(byte[] key, byte[] value) {
            if (size == keys.length) {
                int newCap = Math.min(Integer.MAX_VALUE - 8, Math.max(16, size * 2));
                keys = java.util.Arrays.copyOf(keys, newCap);
                values = java.util.Arrays.copyOf(values, newCap);
            }
            keys[size] = key;
            values[size] = value;
            size++;
        }

        private byte[][] keysSlice() {
            return java.util.Arrays.copyOf(keys, size);
        }

        private byte[][] valuesSlice() {
            return java.util.Arrays.copyOf(values, size);
        }
        @Override
        public int size() {
            return size;
        }

        @Override
        public byte[] keyAt(int index) {
            return keys[index];
        }

        @Override
        public byte[] valueAt(int index) {
            return values[index];
        }
    }

    private record SparkXlakeTableRef(String uniqId) implements XlakeTable {

        @Override
        public void close() {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void refresh() {
        }

        @Override
        public TableMeta meta() {
            return null;
        }

        @Override
        public DynamicTableInfo dynamicInfo() {
            return null;
        }

        @Override
        public Snapshot currentSnapshot() {
            return null;
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            return null;
        }

        @Override
        public List<Snapshot> snapshots() {
            return List.of();
        }
    }
}
