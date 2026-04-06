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
package io.github.ximin.xlake.metastore.impl;

import io.github.ximin.xlake.backend.query.ExpressionBuilder;
import io.github.ximin.xlake.backend.query.serializer.ProtoExpressionSerializer;
import io.github.ximin.xlake.meta.*;
import io.github.ximin.xlake.metastore.RatisMetastore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RatisMetastoreTest {

    @TempDir
    Path tempDir;

    private RatisMetastore metastore;

    @BeforeEach
    void setUp() {
        // For integration tests, we'll use an in-memory implementation
        // since setting up a full Ratis cluster is complex
        // In a real scenario, this would use embedded Ratis servers
        metastore = new RatisMetastore(null) {
            // Override to use in-memory storage for testing
            private final java.util.Map<byte[], byte[]> storage = new java.util.concurrent.ConcurrentHashMap<>();

            @Override
            protected void kvPut(byte[] key, byte[] value) throws IOException {
                storage.put(key, value);
            }

            @Override
            protected Optional<byte[]> kvGet(byte[] key) throws IOException {
                return Optional.ofNullable(storage.get(key));
            }

            @Override
            protected void kvDelete(byte[] key) throws IOException {
                storage.remove(key);
            }

            @Override
            protected List<byte[]> kvScanByPrefix(byte[] prefix) throws IOException {
                String prefixStr = new String(prefix);
                return storage.entrySet().stream()
                        .filter(entry -> new String(entry.getKey()).startsWith(prefixStr))
                        .map(java.util.Map.Entry::getValue)
                        .collect(java.util.stream.Collectors.toList());
            }
        };
    }

    @AfterEach
    void tearDown() throws IOException {
        if (metastore != null) {
            metastore.close();
        }
    }

    @Test
    void shouldCreateAndGetTable() throws IOException {
        PbFileMetadata metadata = PbFileMetadata.newBuilder()
                .setTableName("test_table")
                .setSchema(Schema.newBuilder()
                        .setStructType(StructType.newBuilder().build())
                        .build())
                .build();

        metastore.createTable(metadata);
        Optional<PbFileMetadata> retrieved = metastore.getTable("test_table");

        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getTableName()).isEqualTo("test_table");
    }

    @Test
    void shouldReturnEmptyForNonExistentTable() throws IOException {
        Optional<PbFileMetadata> result = metastore.getTable("non_existent_table");
        assertThat(result).isEmpty();
    }

    @Test
    void shouldDropTable() throws IOException {
        PbFileMetadata metadata = PbFileMetadata.newBuilder()
                .setTableName("table_to_drop")
                .setSchema(Schema.newBuilder()
                        .setStructType(StructType.newBuilder().build())
                        .build())
                .build();

        metastore.createTable(metadata);
        assertThat(metastore.getTable("table_to_drop")).isPresent();

        metastore.dropTable("table_to_drop");
        assertThat(metastore.getTable("table_to_drop")).isEmpty();
    }

    @Test
    void shouldListTables() throws IOException {
        metastore.createTable(PbFileMetadata.newBuilder()
                .setTableName("table1")
                .setSchema(Schema.newBuilder()
                        .setStructType(StructType.newBuilder().build())
                        .build())
                .build());
        metastore.createTable(PbFileMetadata.newBuilder()
                .setTableName("table2")
                .setSchema(Schema.newBuilder()
                        .setStructType(StructType.newBuilder().build())
                        .build())
                .build());

        List<String> tables = metastore.listTables();

        assertThat(tables).hasSize(2);
        assertThat(tables).contains("table1", "table2");
    }

    @Test
    void shouldHandlePbFileMetadataOperations() throws IOException {
        PbFileMetadata fileMeta = PbFileMetadata.newBuilder()
                .setTableName("test_table")
                .setFilePath("/path/to/file.parquet")
                .setFileSizeBytes(1024)
                .build();

        metastore.putFile(fileMeta);
        Optional<PbFileMetadata> retrieved = metastore.getFile("test_table", "/path/to/file.parquet");

        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getFileSizeBytes()).isEqualTo(1024);
    }

    @Test
    void shouldListFiles() throws IOException {
        metastore.putFile(PbFileMetadata.newBuilder()
                .setTableName("test_table")
                .setFilePath("/path/to/file1.parquet")
                .setFileSizeBytes(1024)
                .build());
        metastore.putFile(PbFileMetadata.newBuilder()
                .setTableName("test_table")
                .setFilePath("/path/to/file2.parquet")
                .setFileSizeBytes(2048)
                .build());

        List<PbFileMetadata> files = metastore.listFiles("test_table");

        assertThat(files).hasSize(2);
        assertThat(files).extracting(PbFileMetadata::getFilePath)
                .contains("/path/to/file1.parquet", "/path/to/file2.parquet");
    }

    @Test
    void shouldHandleUpdateEntries() throws IOException {
        UpdateEntry entry = UpdateEntry.newBuilder()
                .setEntryId("entry-1")
                .setTableName("test_table")
                .setPredicate(ProtoExpressionSerializer.toProto(ExpressionBuilder.eq("id",1)))
                .build();

        metastore.putUpdateEntry(entry);
        Optional<UpdateEntry> retrieved = metastore.getUpdateEntry("entry-1");

        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getTableName()).isEqualTo("test_table");
        assertThat(retrieved.get().getPredicate()).isEqualTo(ExpressionConverter.toProto(ExpressionBuilder.eq("id", 1)));
    }

    @Test
    void shouldGetUpdateEntriesByTable() throws IOException {
        metastore.putUpdateEntry(UpdateEntry.newBuilder()
                .setEntryId("entry-1")
                .setTableName("table1")
                .setPredicate(ExpressionConverter.toProto(ExpressionBuilder.eq("id",1)))
                .build());
        metastore.putUpdateEntry(UpdateEntry.newBuilder()
                .setEntryId("entry-2")
                .setTableName("table1")
                .setPredicate(ExpressionConverter.toProto(ExpressionBuilder.eq("id",2)))
                .build());
        metastore.putUpdateEntry(UpdateEntry.newBuilder()
                .setEntryId("entry-3")
                .setTableName("table2")
                .setPredicate(ExpressionConverter.toProto(ExpressionBuilder.eq("id",3)))
                .build());

        List<UpdateEntry> entries = metastore.getUpdateEntries("table1");

        assertThat(entries).hasSize(2);
        assertThat(entries).extracting(UpdateEntry::getEntryId)
                .contains("entry-1", "entry-2");
    }

    @Test
    void shouldDeleteUpdateEntry() throws IOException {
        UpdateEntry entry = UpdateEntry.newBuilder()
                .setEntryId("entry-to-delete")
                .setTableName("test_table")
                .build();

        metastore.putUpdateEntry(entry);
        assertThat(metastore.getUpdateEntry("entry-to-delete")).isPresent();

        metastore.deleteUpdateEntry("entry-to-delete");
        assertThat(metastore.getUpdateEntry("entry-to-delete")).isEmpty();
    }

    @Test
    void shouldHandleSnapshotOperations() throws IOException {
        long snapshotId = metastore.createSnapshot("test_table", "CREATE", "Initial snapshot");

        assertThat(snapshotId).isGreaterThan(0);

        Optional<Snapshot> snapshot = metastore.getSnapshot("test_table", snapshotId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getOperation()).isEqualTo("CREATE");
        assertThat(snapshot.get().getSummary()).isEqualTo("Initial snapshot");
    }

    @Test
    void shouldListSnapshots() throws IOException {
        metastore.createSnapshot("test_table", "CREATE", "Snapshot 1");
        metastore.createSnapshot("test_table", "UPDATE", "Snapshot 2");

        List<Snapshot> snapshots = metastore.getSnapshots("test_table");

        assertThat(snapshots).hasSize(2);
        assertThat(snapshots).extracting(Snapshot::getOperation)
                .contains("CREATE", "UPDATE");
    }

    @Test
    void shouldHandleTransactionOperations() throws IOException {
        TableOperation operation = TableOperation.newBuilder()
                .setTableName("test_table")
                .setOperationType("INSERT")
                .build();

        long commitId = metastore.beginCommit(List.of(operation));

        assertThat(commitId).isGreaterThan(0);

        boolean committed = metastore.commit(commitId);
        assertThat(committed).isTrue();
    }

    @Test
    void shouldHandleAbortTransaction() throws IOException {
        TableOperation operation = TableOperation.newBuilder()
                .setTableName("test_table")
                .setOperationType("INSERT")
                .build();

        long commitId = metastore.beginCommit(List.of(operation));
        boolean aborted = metastore.abortCommit(commitId);

        assertThat(aborted).isTrue();
    }

    @Test
    void shouldThrowExceptionWhenClosed() throws IOException {
        metastore.close();

        assertThatThrownBy(() -> metastore.getTable("test_table"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Metastore is closed");
    }

    @Test
    void shouldHandleEmptyLists() throws IOException {
        List<String> tables = metastore.listTables();
        assertThat(tables).isEmpty();

        List<PbFileMetadata> files = metastore.listFiles("non_existent_table");
        assertThat(files).isEmpty();

        List<UpdateEntry> entries = metastore.getUpdateEntries("non_existent_table");
        assertThat(entries).isEmpty();

        List<Snapshot> snapshots = metastore.getSnapshots("non_existent_table");
        assertThat(snapshots).isEmpty();
    }

    @Test
    void shouldAlterTable() throws IOException {
        Schema originalSchema = Schema.newBuilder()
                .setStructType(StructType.newBuilder()
                        .addFields(StructField.newBuilder()
                                .setFieldName("id").setDataType(DataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.INT32))).build())
                .build();
        PbFileMetadata metadata = PbFileMetadata.newBuilder()
                .setTableName("test_table")
                .setSchema(originalSchema)
                .setCurrentSnapshot(Snapshot.newBuilder().setSnapshotId(0).build())
                .build();
        metastore.createTable(metadata);

        Schema newSchema = Schema.newBuilder()
                .setStructType(StructType.newBuilder()
                        .addFields(StructField.newBuilder()
                                .setFieldName("id").setDataType(DataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.INT32)))
                        .addFields(StructField.newBuilder()
                                .setFieldName("name").setDataType(DataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.STRING)))
                        .addFields(StructField.newBuilder()
                                .setFieldName("age").setDataType(DataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.INT32)))
                        .build())
                .build();
        metastore.alterTable("test_table", newSchema);

        Optional<PbFileMetadata> updated = metastore.getTable("test_table");
        assertThat(updated).isPresent();
        assertThat(updated.get().getSchema().getStructType().getFieldsList()).hasSize(3);
        assertThat(updated.get().getSchema().getStructType().getFields(1).getFieldName()).isEqualTo("name");
        assertThat(updated.get().getSchema().getStructType().getFields(2).getFieldName()).isEqualTo("age");
    }

    @Test
    void shouldThrowWhenAlteringNonExistentTable() {
        assertThatThrownBy(() -> metastore.alterTable("non_existent",
                PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table not found");
    }

    @Test
    void shouldThrowWhenCreatingDuplicateTable() throws IOException {
        PbFileMetadata metadata = PbFileMetadata.newBuilder()
                .setTableName("dup_table")
                .setSchema(PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build())
                .build();
        metastore.createTable(metadata);

        assertThatThrownBy(() -> metastore.createTable(metadata))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Table already exists");
    }

    @Test
    void shouldThrowWhenDroppingNonExistentTable() {
        assertThatThrownBy(() -> metastore.dropTable("non_existent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table not found");
    }

    @Test
    void shouldRemoveFile() throws IOException {
        PbFileMetadata fileMeta = PbFileMetadata.newBuilder()
                .setTableName("test_table")
                .setFilePath("/path/to/file.parquet")
                .setFileSizeBytes(1024)
                .build();
        metastore.putFile(fileMeta);
        assertThat(metastore.getFile("test_table", "/path/to/file.parquet")).isPresent();

        metastore.removeFile("test_table", "/path/to/file.parquet");
        assertThat(metastore.getFile("test_table", "/path/to/file.parquet")).isEmpty();
    }

    @Test
    void shouldReturnEmptyForNonExistentFile() throws IOException {
        Optional<PbFileMetadata> result = metastore.getFile("test_table", "/non/existent/file.parquet");
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnEmptyForNonExistentUpdateEntry() throws IOException {
        Optional<PbUpdateEntry> result = metastore.getUpdateEntry("non_existent_entry");
        assertThat(result).isEmpty();
    }

    @Test
    void shouldCommitNonExistentTransaction() throws IOException {
        boolean result = metastore.commit(999999L);
        assertThat(result).isFalse();
    }

    @Test
    void shouldAbortNonExistentTransaction() throws IOException {
        boolean result = metastore.abortCommit(999999L);
        assertThat(result).isFalse();
    }
}
