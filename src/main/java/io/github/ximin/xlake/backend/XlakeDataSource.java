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
package io.github.ximin.xlake.backend;

import io.github.ximin.xlake.storage.DynamicMmapStore;
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.table.DynamicTableInfo;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.table.Snapshot;
import io.github.ximin.xlake.table.TableMeta;
import io.github.ximin.xlake.table.op.KvScan;
import io.github.ximin.xlake.table.op.KvWriteBuilder;
import io.github.ximin.xlake.table.op.Scan;
import io.github.ximin.xlake.table.record.RecordView;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.*;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class XlakeDataSource implements TableProvider, DataSourceRegister {
    public static final String OPTION_PATH = "path";
    public static final String OPTION_TABLE = "table";
    public static final String OPTION_STORE_ID = "storeId";

    private static final StructType DEFAULT_SCHEMA = new StructType(new StructField[]{
            new StructField("key", DataTypes.StringType, false, Metadata.empty()),
            new StructField("value", DataTypes.StringType, true, Metadata.empty())
    });

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

    private record XlakeScanBuilder(String basePath, String storeId, String tableIdentifier,
                                       StructType schema)
            implements ScanBuilder, org.apache.spark.sql.connector.read.Scan, Batch {

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
            return new InputPartition[]{new XlakeInputPartition()};
        }

        @Override
        public PartitionReaderFactory createReaderFactory() {
            return new XlakePartitionReaderFactory(basePath, storeId, tableIdentifier, schema);
        }
    }

    private record XlakeInputPartition() implements InputPartition {
    }

    private record XlakePartitionReaderFactory(String basePath, String storeId, String tableIdentifier,
                                                  StructType schema)
            implements PartitionReaderFactory {

        @Override
        public PartitionReader<InternalRow> createReader(InputPartition partition) {
            DynamicMmapStore store = DynamicMmapStore.getInstance(basePath, storeId);
            TableStore tableStore = store.tableStore(tableIdentifier);
            List<InternalRow> rows = readAll(tableStore);
            return new XlakePartitionReader(rows);
        }

        private List<InternalRow> readAll(TableStore tableStore) {
            XlakeTable tableRef = new SparkXlakeTableRef(tableIdentifier);
            KvScan scan = KvScan.builder()
                    .withTable(tableRef)
                    .withDataBlocks(List.of())
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
            values[1] = UTF8String.fromBytes(view.value());
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
                                      StructType schema) implements BatchWrite {

        @Override
        public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
            return new XlakeDataWriterFactory(basePath, storeId, tableIdentifier, schema);
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
            return new XlakeDataWriter(store, tableIdentifier, partitionId);
        }
    }

    private static final class XlakeDataWriter implements DataWriter<InternalRow> {
        private final DynamicMmapStore store;
        private final String tableIdentifier;
        private final int partitionId;
        private long count;

        private XlakeDataWriter(DynamicMmapStore store, String tableIdentifier, int partitionId) {
            this.store = store;
            this.tableIdentifier = tableIdentifier;
            this.partitionId = partitionId;
        }

        @Override
        public void write(InternalRow record) {
            String key = record.getUTF8String(0).toString();
            String value = record.isNullAt(1) ? "" : record.getUTF8String(1).toString();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            XlakeTable tableRef = new SparkXlakeTableRef(tableIdentifier);
            KvWriteBuilder builder = KvWriteBuilder.builder()
                    .table(tableRef)
                    .key(keyBytes)
                    .value(valueBytes)
                    .partitionHint(partitionId)
                    .build();
            store.write(builder);
            count++;
        }

        @Override
        public WriterCommitMessage commit() {
            return new XlakeWriterCommitMessage(count);
        }

        @Override
        public void abort() {
        }

        @Override
        public void close() {
        }
    }

    private record XlakeWriterCommitMessage(
            long count) implements WriterCommitMessage {
    }

    private record SparkXlakeTableRef(String uniqId) implements XlakeTable {

        @Override
        public void close() {
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
