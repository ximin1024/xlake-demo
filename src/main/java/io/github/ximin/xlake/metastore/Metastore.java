package io.github.ximin.xlake.metastore;

import io.github.ximin.xlake.meta.*;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface Metastore {

    void createTable(TableMetadata metadata) throws IOException;

    void dropTable(String tableName) throws IOException;

    Optional<TableMetadata> getTable(String tableName) throws IOException;

    void alterTable(String tableName, Schema newSchema) throws IOException;

    List<String> listTables() throws IOException;

    Optional<Snapshot> getSnapshot(String tableName, long snapshotId) throws IOException;

    List<Snapshot> getSnapshots(String tableName) throws IOException;

    long createSnapshot(String tableName, String operation, String summary) throws IOException;

    long beginCommit(List<TableOperation> operations) throws IOException;

    boolean commit(long commitId) throws IOException;

    boolean abortCommit(long commitId) throws IOException;

    void putFile(FileMetadata fileMeta) throws IOException;

    Optional<FileMetadata> getFile(String tableName, String filePath) throws IOException;

    List<FileMetadata> listFiles(String tableName) throws IOException;

    List<FileMetadata> listFiles(String tableName, int level) throws IOException;

    void removeFile(String tableName, String filePath) throws IOException;

    void putUpdateEntry(UpdateEntry entry) throws IOException;

    Optional<UpdateEntry> getUpdateEntry(String entryId) throws IOException;

    List<UpdateEntry> getUpdateEntries(String tableName) throws IOException;

    void deleteUpdateEntry(String entryId) throws IOException;

    void close() throws IOException;
}
