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
    private static final String CATALOG = "default";
    private static final String DATABASE = "default";

    @BeforeEach
    void setUp() {
        // For integration tests, we'll use an in-memory implementation
        // since setting up a full Ratis cluster is complex
        // In a real scenario, this would use embedded Ratis servers
        metastore = new RatisMetastore(null) {
            // Override to use in-memory storage for testing
            private final java.util.Map<String, byte[]> storage = new java.util.concurrent.ConcurrentHashMap<>();
            private volatile boolean open = true;

            @Override
            protected void kvPut(byte[] key, byte[] value) throws IOException {
                if (!open) {
                    throw new IOException("Metastore is closed");
                }
                storage.put(new String(key, java.nio.charset.StandardCharsets.UTF_8), value);
            }

            @Override
            protected Optional<byte[]> kvGet(byte[] key) throws IOException {
                if (!open) {
                    throw new IOException("Metastore is closed");
                }
                return Optional.ofNullable(storage.get(new String(key, java.nio.charset.StandardCharsets.UTF_8)));
            }

            @Override
            protected void kvDelete(byte[] key) throws IOException {
                if (!open) {
                    throw new IOException("Metastore is closed");
                }
                storage.remove(new String(key, java.nio.charset.StandardCharsets.UTF_8));
            }

            @Override
            protected List<byte[]> kvScanByPrefix(byte[] prefix) throws IOException {
                if (!open) {
                    throw new IOException("Metastore is closed");
                }
                String prefixStr = new String(prefix, java.nio.charset.StandardCharsets.UTF_8);
                return storage.entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith(prefixStr))
                        .map(java.util.Map.Entry::getValue)
                        .toList();
            }

            @Override
            public void close() throws IOException {
                open = false;
                super.close();
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
        PbTableMetadata metadata = PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(CATALOG)
                        .setDatabase(DATABASE)
                        .setTable("test_table")
                        .build())
                .setSchema(PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build())
                .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                .build();

        metastore.createTable(metadata);
        Optional<PbTableMetadata> retrieved = metastore.getTable(CATALOG, DATABASE, "test_table");

        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getIdentifier().getTable()).isEqualTo("test_table");
    }

    @Test
    void shouldReturnEmptyForNonExistentTable() throws IOException {
        Optional<PbTableMetadata> result = metastore.getTable(CATALOG, DATABASE, "non_existent_table");
        assertThat(result).isEmpty();
    }

    @Test
    void shouldDropTable() throws IOException {
        PbTableMetadata metadata = PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(CATALOG)
                        .setDatabase(DATABASE)
                        .setTable("table_to_drop")
                        .build())
                .setSchema(PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build())
                .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                .build();

        metastore.createTable(metadata);
        assertThat(metastore.getTable(CATALOG, DATABASE, "table_to_drop")).isPresent();

        metastore.dropTable(CATALOG, DATABASE, "table_to_drop");
        assertThat(metastore.getTable(CATALOG, DATABASE, "table_to_drop")).isEmpty();
    }

    @Test
    void shouldListTables() throws IOException {
        metastore.createTable(PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(CATALOG)
                        .setDatabase(DATABASE)
                        .setTable("table1")
                        .build())
                .setSchema(PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build())
                .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                .build());
        metastore.createTable(PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(CATALOG)
                        .setDatabase(DATABASE)
                        .setTable("table2")
                        .build())
                .setSchema(PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build())
                .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                .build());

        List<String> tables = metastore.listTables(CATALOG, DATABASE);

        assertThat(tables).hasSize(2);
        assertThat(tables).contains("table1", "table2");
    }

    @Test
    void shouldHandleFileMetadataOperations() throws IOException {
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
        PbUpdateEntry entry = PbUpdateEntry.newBuilder()
                .setEntryId("entry-1")
                .setTableName("test_table")
                .setPredicate(ProtoExpressionSerializer.toProto(ExpressionBuilder.eq("id",1)))
                .build();

        metastore.putUpdateEntry(entry);
        Optional<PbUpdateEntry> retrieved = metastore.getUpdateEntry("entry-1");

        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getTableName()).isEqualTo("test_table");
        assertThat(retrieved.get().getPredicate()).isEqualTo(ProtoExpressionSerializer.toProto(ExpressionBuilder.eq("id", 1)));
    }

    @Test
    void shouldGetUpdateEntriesByTable() throws IOException {
        metastore.putUpdateEntry(PbUpdateEntry.newBuilder()
                .setEntryId("entry-1")
                .setTableName("table1")
                .setPredicate(ProtoExpressionSerializer.toProto(ExpressionBuilder.eq("id", 1)))
                .build());
        metastore.putUpdateEntry(PbUpdateEntry.newBuilder()
                .setEntryId("entry-2")
                .setTableName("table1")
                .setPredicate(ProtoExpressionSerializer.toProto(ExpressionBuilder.eq("id", 2)))
                .build());
        metastore.putUpdateEntry(PbUpdateEntry.newBuilder()
                .setEntryId("entry-3")
                .setTableName("table2")
                .setPredicate(ProtoExpressionSerializer.toProto(ExpressionBuilder.eq("id", 3)))
                .build());

        List<PbUpdateEntry> entries = metastore.getUpdateEntries("table1");

        assertThat(entries).hasSize(2);
        assertThat(entries).extracting(PbUpdateEntry::getEntryId)
                .contains("entry-1", "entry-2");
    }

    @Test
    void shouldDeleteUpdateEntry() throws IOException {
        PbUpdateEntry entry = PbUpdateEntry.newBuilder()
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
        metastore.createTable(PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(CATALOG)
                        .setDatabase(DATABASE)
                        .setTable("test_table")
                        .build())
                .setSchema(PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build())
                .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                .build());

        long snapshotId = metastore.createSnapshot(CATALOG, DATABASE, "test_table", "CREATE", "Initial snapshot");

        assertThat(snapshotId).isGreaterThan(0);

        Optional<PbSnapshot> snapshot = metastore.getSnapshot(CATALOG, DATABASE, "test_table", snapshotId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getOperation()).isEqualTo("CREATE");
        assertThat(snapshot.get().getSummary()).isEqualTo("Initial snapshot");
    }

    @Test
    void shouldListSnapshots() throws IOException {
        metastore.createTable(PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(CATALOG)
                        .setDatabase(DATABASE)
                        .setTable("test_table")
                        .build())
                .setSchema(PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build())
                .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                .build());

        metastore.createSnapshot(CATALOG, DATABASE, "test_table", "CREATE", "Snapshot 1");
        metastore.createSnapshot(CATALOG, DATABASE, "test_table", "UPDATE", "Snapshot 2");

        List<PbSnapshot> snapshots = metastore.getSnapshots(CATALOG, DATABASE, "test_table");

        assertThat(snapshots).hasSize(2);
        assertThat(snapshots).extracting(PbSnapshot::getOperation)
                .contains("CREATE", "UPDATE");
    }

    @Test
    void shouldHandleTransactionOperations() throws IOException {
        PbTableOperation operation = PbTableOperation.newBuilder()
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
        PbTableOperation operation = PbTableOperation.newBuilder()
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

        assertThatThrownBy(() -> metastore.getTable(CATALOG, DATABASE, "test_table"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Metastore is closed");
    }

    @Test
    void shouldHandleEmptyLists() throws IOException {
        List<String> tables = metastore.listTables(CATALOG, DATABASE);
        assertThat(tables).isEmpty();

        List<PbFileMetadata> files = metastore.listFiles("non_existent_table");
        assertThat(files).isEmpty();

        List<PbUpdateEntry> entries = metastore.getUpdateEntries("non_existent_table");
        assertThat(entries).isEmpty();

        List<PbSnapshot> snapshots = metastore.getSnapshots(CATALOG, DATABASE, "non_existent_table");
        assertThat(snapshots).isEmpty();
    }

    @Test
    void shouldAlterTable() throws IOException {
        PbSchema originalSchema = PbSchema.newBuilder()
                .setStructType(PbStructType.newBuilder()
                        .addFields(PbStructField.newBuilder()
                                .setFieldName("id").setDataType(PbDataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.INT32))).build())
                .build();
        PbTableMetadata metadata = PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(CATALOG)
                        .setDatabase(DATABASE)
                        .setTable("test_table")
                        .build())
                .setSchema(originalSchema)
                .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                .build();
        metastore.createTable(metadata);

        PbSchema newSchema = PbSchema.newBuilder()
                .setStructType(PbStructType.newBuilder()
                        .addFields(PbStructField.newBuilder()
                                .setFieldName("id").setDataType(PbDataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.INT32)))
                        .addFields(PbStructField.newBuilder()
                                .setFieldName("name").setDataType(PbDataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.STRING)))
                        .addFields(PbStructField.newBuilder()
                                .setFieldName("age").setDataType(PbDataType.newBuilder()
                                        .setPrimitiveType(PrimitiveType.INT32)))
                        .build())
                .build();
        metastore.alterTable(CATALOG, DATABASE, "test_table", newSchema);

        Optional<PbTableMetadata> updated = metastore.getTable(CATALOG, DATABASE, "test_table");
        assertThat(updated).isPresent();
        assertThat(updated.get().getSchema().getStructType().getFieldsList()).hasSize(3);
        assertThat(updated.get().getSchema().getStructType().getFields(1).getFieldName()).isEqualTo("name");
        assertThat(updated.get().getSchema().getStructType().getFields(2).getFieldName()).isEqualTo("age");
    }

    @Test
    void shouldThrowWhenAlteringNonExistentTable() {
        assertThatThrownBy(() -> metastore.alterTable(CATALOG, DATABASE, "non_existent",
                PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table not found");
    }

    @Test
    void shouldThrowWhenCreatingDuplicateTable() throws IOException {
        PbTableMetadata metadata = PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(CATALOG)
                        .setDatabase(DATABASE)
                        .setTable("dup_table")
                        .build())
                .setSchema(PbSchema.newBuilder()
                        .setStructType(PbStructType.newBuilder().build())
                        .build())
                .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                .build();
        metastore.createTable(metadata);

        assertThatThrownBy(() -> metastore.createTable(metadata))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Table already exists");
    }

    @Test
    void shouldThrowWhenDroppingNonExistentTable() {
        assertThatThrownBy(() -> metastore.dropTable(CATALOG, DATABASE, "non_existent"))
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
