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
package io.github.ximin.xlake.metastore;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.ximin.xlake.meta.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

@Slf4j
public abstract class AbstractMetastore implements Metastore {

    private static final String PREFIX_TABLE = "table:";
    private static final String PREFIX_SNAPSHOT = "snapshot:";
    private static final String PREFIX_FILE = "file:";
    private static final String PREFIX_UPDATE = "update:";
    private static final String PREFIX_COMMIT = "commit:";

    protected byte[] tableKey(String catalog, String database, String table) {
        return (PREFIX_TABLE + catalog + ":" + database + ":" + table).getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] snapshotKey(String catalog, String database, String table, long snapshotId) {
        return (PREFIX_SNAPSHOT + catalog + ":" + database + ":" + table + ":" + snapshotId).getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] fileKey(String catalog, String database, String table, String filePath) {
        return (PREFIX_FILE + catalog + ":" + database + ":" + table + ":" + filePath).getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] updateEntryKey(String entryId) {
        return (PREFIX_UPDATE + entryId).getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] commitKey(long commitId) {
        return (PREFIX_COMMIT + commitId).getBytes(StandardCharsets.UTF_8);
    }

    protected abstract void kvPut(byte[] key, byte[] value) throws IOException;

    protected abstract Optional<byte[]> kvGet(byte[] key) throws IOException;

    protected abstract void kvDelete(byte[] key) throws IOException;

    protected abstract List<byte[]> kvScanByPrefix(byte[] prefix) throws IOException;

    @Override
    public void createTable(PbTableMetadata metadata) throws IOException {
        byte[] key = tableKey(
                metadata.getIdentifier().getCatalog(),
                metadata.getIdentifier().getDatabase(),
                metadata.getIdentifier().getTable()
        );
        Optional<byte[]> existing = kvGet(key);
        if (existing.isPresent()) {
            throw new IllegalStateException("Table already exists: " + metadata.getIdentifier().getTable());
        }
        kvPut(key, metadata.toByteArray());
        log.info("Created table: {}", metadata.getIdentifier().getTable());
    }

    @Override
    public void dropTable(String catalog, String database, String table) throws IOException {
        byte[] key = tableKey(catalog, database, table);
        Optional<byte[]> existing = kvGet(key);
        if (existing.isEmpty()) {
            throw new IllegalArgumentException("Table not found: " + table);
        }
        kvDelete(key);
        log.info("Dropped table: {}", table);
    }

    @Override
    public Optional<PbTableMetadata> getTable(String catalog, String database, String table) throws IOException {
        return kvGet(tableKey(catalog, database, table))
                .map(bytes -> {
                    try {
                        return PbTableMetadata.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to parse PbTableMetadata for: " + table, e);
                    }
                });
    }

    @Override
    public void alterTable(String catalog, String database, String table, PbSchema newSchema) throws IOException {
        Optional<PbTableMetadata> existingOpt = getTable(catalog, database, table);
        if (existingOpt.isEmpty()) {
            throw new IllegalArgumentException("Table not found: " + table);
        }
        PbTableMetadata updated = existingOpt.get().toBuilder()
                .setSchema(newSchema)
                .build();
        kvPut(tableKey(catalog, database, table), updated.toByteArray());
        log.info("Altered table schema: {}", table);
    }

    @Override
    public List<String> listTables(String catalog, String database) throws IOException {
        byte[] prefix = (PREFIX_TABLE + catalog + ":" + database + ":").getBytes(StandardCharsets.UTF_8);
        return kvScanByPrefix(prefix).stream()
                .map(bytes -> {
                    try {
                        return PbTableMetadata.parseFrom(bytes).getIdentifier().getTable();
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse PbTableMetadata during listTables", e);
                        return (String) null;
                    }
                })
                .filter(s -> s != null)
                .toList();
    }

    @Override
    public Optional<PbSnapshot> getSnapshot(String catalog, String database, String table, long snapshotId) throws IOException {
        return kvGet(snapshotKey(catalog, database, table, snapshotId))
                .map(bytes -> {
                    try {
                        return PbSnapshot.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to parse PbSnapshot", e);
                    }
                });
    }

    @Override
    public List<PbSnapshot> getSnapshots(String catalog, String database, String table) throws IOException {
        byte[] prefix = (PREFIX_SNAPSHOT + catalog + ":" + database + ":" + table + ":").getBytes(StandardCharsets.UTF_8);
        return kvScanByPrefix(prefix).stream()
                .map(bytes -> {
                    try {
                        return PbSnapshot.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse PbSnapshot for table: {}", table, e);
                        return null;
                    }
                })
                .toList();
    }

    @Override
    public long createSnapshot(String catalog, String database, String table, String operation, String summary) throws IOException {
        Optional<PbTableMetadata> metaOpt = getTable(catalog, database, table);
        if (metaOpt.isEmpty()) {
            throw new IllegalArgumentException("Table not found: " + table);
        }
        PbTableMetadata meta = metaOpt.get();

        long newSnapshotId = meta.getCurrentSnapshot().getSnapshotId() + 1;
        PbSnapshot newSnapshot = PbSnapshot.newBuilder()
                .setSnapshotId(newSnapshotId)
                .setTimestamp(System.currentTimeMillis())
                .setOperation(operation)
                .setSummary(summary != null ? summary : "")
                .setSchema(meta.getSchema())
                .build();

        kvPut(snapshotKey(catalog, database, table, newSnapshotId), newSnapshot.toByteArray());

        PbTableMetadata updatedMeta = meta.toBuilder()
                .setCurrentSnapshot(newSnapshot)
                .build();
        kvPut(tableKey(catalog, database, table), updatedMeta.toByteArray());

        log.info("Created snapshot {} for table {}: {}", newSnapshotId, table, operation);
        return newSnapshotId;
    }

    @Override
    public long beginCommit(List<PbTableOperation> operations) throws IOException {
        long commitId = System.currentTimeMillis();
        PbCommitRequest request = PbCommitRequest.newBuilder()
                .setCommitId(commitId)
                .addAllOperations(operations)
                .build();
        kvPut(commitKey(commitId), request.toByteArray());
        log.info("Began commit: {}", commitId);
        return commitId;
    }

    @Override
    public boolean commit(long commitId) throws IOException {
        Optional<byte[]> data = kvGet(commitKey(commitId));
        if (data.isEmpty()) {
            log.warn("Commit not found: {}", commitId);
            return false;
        }
        try {
            PbCommitRequest request = PbCommitRequest.parseFrom(data.get());

            for (PbTableOperation op : request.getOperationsList()) {
                applyOperation(op);
            }

            kvDelete(commitKey(commitId));
            log.info("Committed: {}", commitId);
            return true;
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse PbCommitRequest for: {}", commitId, e);
            return false;
        }
    }

    @Override
    public boolean abortCommit(long commitId) throws IOException {
        Optional<byte[]> data = kvGet(commitKey(commitId));
        if (data.isEmpty()) {
            return false;
        }
        kvDelete(commitKey(commitId));
        log.info("Aborted commit: {}", commitId);
        return true;
    }

    private void applyOperation(PbTableOperation op) throws IOException {
        switch (op.getOperationType()) {
            case "CREATE_TABLE" -> {
                if (op.hasSchema()) {
                    PbTableMetadata meta = PbTableMetadata.newBuilder()
                            .setIdentifier(PbTableIdentifier.newBuilder()
                                    .setCatalog("default")
                                    .setDatabase("default")
                                    .setTable(op.getTableName())
                                    .build())
                            .setSchema(op.getSchema())
                            .setCurrentSnapshot(PbSnapshot.newBuilder().setSnapshotId(0).build())
                            .build();
                    kvPut(tableKey("default", "default", op.getTableName()), meta.toByteArray());
                }
            }
            case "DROP_TABLE" -> kvDelete(tableKey("default", "default", op.getTableName()));
            case "ALTER_SCHEMA" -> {
                if (op.hasSchema()) {
                    alterTable("default", "default", op.getTableName(), op.getSchema());
                }
            }
            default -> log.warn("Unknown operation type: {}", op.getOperationType());
        }
    }

    @Override
    public void putFile(PbFileMetadata fileMeta) throws IOException {
        kvPut(fileKey(fileMeta.getTableName(), "", "", fileMeta.getFilePath()), fileMeta.toByteArray());
    }

    @Override
    public Optional<PbFileMetadata> getFile(String tableName, String filePath) throws IOException {
        return kvGet(fileKey(tableName, "", "", filePath))
                .map(bytes -> {
                    try {
                        return PbFileMetadata.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to parse PbFileMetadata", e);
                    }
                });
    }

    @Override
    public List<PbFileMetadata> listFiles(String tableName) throws IOException {
        byte[] prefix = (PREFIX_FILE + tableName + ":").getBytes(StandardCharsets.UTF_8);
        return kvScanByPrefix(prefix).stream()
                .map(bytes -> {
                    try {
                        return PbFileMetadata.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse PbFileMetadata for table: {}", tableName, e);
                        return null;
                    }
                })
                .toList();
    }

    @Override
    public List<PbFileMetadata> listFiles(String tableName, int level) throws IOException {
        return listFiles(tableName).stream()
                .filter(f -> f.getLevel() == level)
                .toList();
    }

    @Override
    public void removeFile(String tableName, String filePath) throws IOException {
        kvDelete(fileKey(tableName, "", "", filePath));
    }

    @Override
    public void putUpdateEntry(PbUpdateEntry entry) throws IOException {
        kvPut(updateEntryKey(entry.getEntryId()), entry.toByteArray());
    }

    @Override
    public Optional<PbUpdateEntry> getUpdateEntry(String entryId) throws IOException {
        return kvGet(updateEntryKey(entryId))
                .map(bytes -> {
                    try {
                        return PbUpdateEntry.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to parse PbUpdateEntry", e);
                    }
                });
    }

    @Override
    public List<PbUpdateEntry> getUpdateEntries(String tableName) throws IOException {
        return kvScanByPrefix(PREFIX_UPDATE.getBytes(StandardCharsets.UTF_8)).stream()
                .map(bytes -> {
                    try {
                        PbUpdateEntry entry = PbUpdateEntry.parseFrom(bytes);
                        if (entry.getTableName().equals(tableName)) {
                            return entry;
                        }
                        return null;
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse PbUpdateEntry", e);
                        return null;
                    }
                })
                .filter(e -> e != null)
                .toList();
    }

    @Override
    public void deleteUpdateEntry(String entryId) throws IOException {
        kvDelete(updateEntryKey(entryId));
    }
}
