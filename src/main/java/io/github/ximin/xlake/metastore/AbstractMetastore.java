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

    protected byte[] tableKey(String tableName) {
        return (PREFIX_TABLE + tableName).getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] snapshotKey(String tableName, long snapshotId) {
        return (PREFIX_SNAPSHOT + tableName + ":" + snapshotId).getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] fileKey(String tableName, String filePath) {
        return (PREFIX_FILE + tableName + ":" + filePath).getBytes(StandardCharsets.UTF_8);
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
    public void createTable(TableMetadata metadata) throws IOException {
        byte[] key = tableKey(metadata.getTableName());
        Optional<byte[]> existing = kvGet(key);
        if (existing.isPresent()) {
            throw new IllegalStateException("Table already exists: " + metadata.getTableName());
        }
        kvPut(key, metadata.toByteArray());
        log.info("Created table: {}", metadata.getTableName());
    }

    @Override
    public void dropTable(String tableName) throws IOException {
        byte[] key = tableKey(tableName);
        Optional<byte[]> existing = kvGet(key);
        if (existing.isEmpty()) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        kvDelete(key);
        log.info("Dropped table: {}", tableName);
    }

    @Override
    public Optional<TableMetadata> getTable(String tableName) throws IOException {
        return kvGet(tableKey(tableName))
                .map(bytes -> {
                    try {
                        return TableMetadata.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to parse TableMetadata for: " + tableName, e);
                    }
                });
    }

    @Override
    public void alterTable(String tableName, Schema newSchema) throws IOException {
        Optional<TableMetadata> existingOpt = getTable(tableName);
        if (existingOpt.isEmpty()) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        TableMetadata updated = existingOpt.get().toBuilder()
                .setSchema(newSchema)
                .build();
        kvPut(tableKey(tableName), updated.toByteArray());
        log.info("Altered table schema: {}", tableName);
    }

    @Override
    public List<String> listTables() throws IOException {
        return kvScanByPrefix(PREFIX_TABLE.getBytes(StandardCharsets.UTF_8)).stream()
                .map(bytes -> {
                    try {
                        return TableMetadata.parseFrom(bytes).getTableName();
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse TableMetadata during listTables", e);
                        return null;
                    }
                })
                .toList();
    }

    @Override
    public Optional<Snapshot> getSnapshot(String tableName, long snapshotId) throws IOException {
        return kvGet(snapshotKey(tableName, snapshotId))
                .map(bytes -> {
                    try {
                        return Snapshot.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to parse Snapshot", e);
                    }
                });
    }

    @Override
    public List<Snapshot> getSnapshots(String tableName) throws IOException {
        byte[] prefix = (PREFIX_SNAPSHOT + tableName + ":").getBytes(StandardCharsets.UTF_8);
        return kvScanByPrefix(prefix).stream()
                .map(bytes -> {
                    try {
                        return Snapshot.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse Snapshot for table: {}", tableName, e);
                        return null;
                    }
                })
                .toList();
    }

    @Override
    public long createSnapshot(String tableName, String operation, String summary) throws IOException {
        Optional<TableMetadata> metaOpt = getTable(tableName);
        if (metaOpt.isEmpty()) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        TableMetadata meta = metaOpt.get();

        long newSnapshotId = meta.getCurrentSnapshot().getSnapshotId() + 1;
        Snapshot newSnapshot = Snapshot.newBuilder()
                .setSnapshotId(newSnapshotId)
                .setTimestamp(System.currentTimeMillis())
                .setOperation(operation)
                .setSummary(summary)
                .setSchema(meta.getSchema())
                .build();

        kvPut(snapshotKey(tableName, newSnapshotId), newSnapshot.toByteArray());

        TableMetadata updatedMeta = meta.toBuilder()
                .setCurrentSnapshot(newSnapshot)
                .build();
        kvPut(tableKey(tableName), updatedMeta.toByteArray());

        log.info("Created snapshot {} for table {}: {}", newSnapshotId, tableName, operation);
        return newSnapshotId;
    }

    @Override
    public long beginCommit(List<TableOperation> operations) throws IOException {
        long commitId = System.currentTimeMillis();
        CommitRequest request = CommitRequest.newBuilder()
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
            CommitRequest request = CommitRequest.parseFrom(data.get());

            for (TableOperation op : request.getOperationsList()) {
                applyOperation(op);
            }

            kvDelete(commitKey(commitId));
            log.info("Committed: {}", commitId);
            return true;
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse CommitRequest for: {}", commitId, e);
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

    private void applyOperation(TableOperation op) throws IOException {
        switch (op.getOperationType()) {
            case "CREATE_TABLE" -> {
                if (op.hasSchema()) {
                    TableMetadata meta = TableMetadata.newBuilder()
                            .setTableName(op.getTableName())
                            .setSchema(op.getSchema())
                            .setCurrentSnapshot(Snapshot.newBuilder().setSnapshotId(0).build())
                            .build();
                    kvPut(tableKey(op.getTableName()), meta.toByteArray());
                }
            }
            case "DROP_TABLE" -> kvDelete(tableKey(op.getTableName()));
            case "ALTER_SCHEMA" -> {
                if (op.hasSchema()) {
                    alterTable(op.getTableName(), op.getSchema());
                }
            }
            default -> log.warn("Unknown operation type: {}", op.getOperationType());
        }
    }

    @Override
    public void putFile(FileMetadata fileMeta) throws IOException {
        kvPut(fileKey(fileMeta.getTableName(), fileMeta.getFilePath()), fileMeta.toByteArray());
    }

    @Override
    public Optional<FileMetadata> getFile(String tableName, String filePath) throws IOException {
        return kvGet(fileKey(tableName, filePath))
                .map(bytes -> {
                    try {
                        return FileMetadata.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to parse FileMetadata", e);
                    }
                });
    }

    @Override
    public List<FileMetadata> listFiles(String tableName) throws IOException {
        byte[] prefix = (PREFIX_FILE + tableName + ":").getBytes(StandardCharsets.UTF_8);
        return kvScanByPrefix(prefix).stream()
                .map(bytes -> {
                    try {
                        return FileMetadata.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse FileMetadata for table: {}", tableName, e);
                        return null;
                    }
                })
                .toList();
    }

    @Override
    public List<FileMetadata> listFiles(String tableName, int level) throws IOException {
        return listFiles(tableName).stream()
                .filter(f -> f.getLevel() == level)
                .toList();
    }

    @Override
    public void removeFile(String tableName, String filePath) throws IOException {
        kvDelete(fileKey(tableName, filePath));
    }

    @Override
    public void putUpdateEntry(UpdateEntry entry) throws IOException {
        kvPut(updateEntryKey(entry.getEntryId()), entry.toByteArray());
    }

    @Override
    public Optional<UpdateEntry> getUpdateEntry(String entryId) throws IOException {
        return kvGet(updateEntryKey(entryId))
                .map(bytes -> {
                    try {
                        return UpdateEntry.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to parse UpdateEntry", e);
                    }
                });
    }

    @Override
    public List<UpdateEntry> getUpdateEntries(String tableName) throws IOException {
        return kvScanByPrefix(PREFIX_UPDATE.getBytes(StandardCharsets.UTF_8)).stream()
                .map(bytes -> {
                    try {
                        UpdateEntry entry = UpdateEntry.parseFrom(bytes);
                        if (entry.getTableName().equals(tableName)) {
                            return entry;
                        }
                        return null;
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Failed to parse UpdateEntry", e);
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
