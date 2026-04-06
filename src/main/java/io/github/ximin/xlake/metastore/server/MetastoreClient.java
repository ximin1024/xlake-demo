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
package io.github.ximin.xlake.metastore.server;

import io.github.ximin.xlake.meta.*;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MetastoreClient implements AutoCloseable {

    private static final ConcurrentHashMap<String, MetastoreClient> INSTANCES = new ConcurrentHashMap<>();

    private final ManagedChannel channel;
    private final MetastoreServiceGrpc.MetastoreServiceBlockingStub stub;
    private final String target;

    private MetastoreClient(String target) {
        this.target = target;
        this.channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .maxInboundMessageSize(16 * 1024 * 1024)
                .build();
        this.stub = MetastoreServiceGrpc.newBlockingStub(channel);
    }

    public static MetastoreClient getInstance() {
        return getInstance("localhost:9001");
    }

    public static MetastoreClient getInstance(String target) {
        return INSTANCES.computeIfAbsent(target, MetastoreClient::new);
    }

    public Channel getChannel() {
        return channel;
    }

    public void createTable(PbTableMetadata metadata) {
        try {
            var request = PbCreateTableRequest.newBuilder().setMetadata(metadata).build();
            var result = stub.createTable(request);
            if (!result.getSuccess()) {
                throw new RuntimeException("CreateTable failed: " + result.getMessage());
            }
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in createTable", e);
        }
    }

    public void dropTable(String tableName) {
        try {
            var request = PbDropTableRequest.newBuilder().setTableName(tableName).build();
            var result = stub.dropTable(request);
            if (!result.getSuccess()) {
                throw new RuntimeException("DropTable failed: " + result.getMessage());
            }
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in dropTable", e);
        }
    }

    public Optional<PbTableMetadata> getTableMetadata(String tableName) {
        try {
            var request = PbGetTableRequest.newBuilder().setTableName(tableName).build();
            return Optional.of(stub.getTable(request));
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == io.grpc.Status.NOT_FOUND.getCode()) {
                return Optional.empty();
            }
            throw new RuntimeException("gRPC error in getTableMetadata", e);
        }
    }

    public void alterTable(String tableName, PbSchema newSchema) {
        try {
            var request = PbAlterTableRequest.newBuilder()
                    .setTableName(tableName)
                    .setNewSchema(newSchema)
                    .build();
            var result = stub.alterTable(request);
            if (!result.getSuccess()) {
                throw new RuntimeException("AlterTable failed: " + result.getMessage());
            }
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in alterTable", e);
        }
    }

    public List<PbSnapshot> getTableSnapshots(String tableName) {
        try {
            var request = PbGetSnapshotsRequest.newBuilder().setTableName(tableName).build();
            return stub.getSnapshots(request).getSnapshotsList();
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in getTableSnapshots", e);
        }
    }

    public Optional<PbSnapshot> getSnapshot(String tableName, long snapshotId) {
        try {
            var request = PbGetSnapshotRequest.newBuilder()
                    .setTableName(tableName)
                    .setSnapshotId(snapshotId)
                    .build();
            return Optional.of(stub.getSnapshot(request));
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == io.grpc.Status.NOT_FOUND.getCode()) {
                return Optional.empty();
            }
            throw new RuntimeException("gRPC error in getSnapshot", e);
        }
    }

    public long beginCommit(List<PbTableOperation> operations) {
        try {
            var request = PbCommitRequest.newBuilder()
                    .setCommitId(System.currentTimeMillis())
                    .addAllOperations(operations)
                    .build();
            var result = stub.beginCommit(request);
            if (!result.getSuccess()) {
                throw new RuntimeException("BeginCommit failed: " + result.getMessage());
            }
            return result.getCommitId();
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in beginCommit", e);
        }
    }

    public boolean commit(long commitId) {
        try {
            var result = stub.commit(PbCommitId.newBuilder().setValue(commitId).build());
            return result.getSuccess();
        } catch (StatusRuntimeException e) {
            log.error("gRPC error in commit: {}", commitId, e);
            return false;
        }
    }

    public boolean abortCommit(long commitId) {
        try {
            var result = stub.abortCommit(PbCommitId.newBuilder().setValue(commitId).build());
            return result.getSuccess();
        } catch (StatusRuntimeException e) {
            log.error("gRPC error in abortCommit: {}", commitId, e);
            return false;
        }
    }

    public PbOperationResult commitOperations(String tableName, long commitId, List<PbTableOperation> operations) {
        try {
            var request = PbCommitRequest.newBuilder()
                    .setCommitId(commitId)
                    .addAllOperations(operations)
                    .build();
            var result = stub.beginCommit(request);

            if (!result.getSuccess()) {
                return PbOperationResult.newBuilder()
                        .setSuccess(false)
                        .setMessage("beginCommit failed")
                        .build();
            }

            var commitResult = stub.commit(PbCommitId.newBuilder().setValue(commitId).build());
            return commitResult;
        } catch (Exception e) {
            return PbOperationResult.newBuilder()
                    .setSuccess(false)
                    .setMessage(e.getMessage())
                    .build();
        }
    }

    public void putFile(PbFileMetadata fileMeta) {
        try {
            var result = stub.putFile(fileMeta);
            if (!result.getSuccess()) {
                throw new RuntimeException("PutFile failed: " + result.getMessage());
            }
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in putFile", e);
        }
    }

    public List<PbFileMetadata> listFiles(String tableName) {
        try {
            var request = PbListFilesRequest.newBuilder().setTableName(tableName).build();
            return stub.listFiles(request).getFilesList();
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in listFiles", e);
        }
    }

    public List<PbFileMetadata> listFiles(String tableName, int level) {
        try {
            var request = PbListFilesRequest.newBuilder()
                    .setTableName(tableName)
                    .setLevel(level)
                    .build();
            return stub.listFiles(request).getFilesList();
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in listFiles by level", e);
        }
    }

    public void removeFile(String tableName, String filePath) {
        try {
            var meta = PbFileMetadata.newBuilder()
                    .setTableName(tableName)
                    .setFilePath(filePath)
                    .build();
            var result = stub.removeFile(meta);
            if (!result.getSuccess()) {
                throw new RuntimeException("RemoveFile failed: " + result.getMessage());
            }
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in removeFile", e);
        }
    }

    public void putUpdateEntry(PbUpdateEntry entry) {
        try {
            var request = PbPutUpdateEntryRequest.newBuilder().setEntry(entry).build();
            var result = stub.putUpdateEntry(request);
            if (!result.getSuccess()) {
                throw new RuntimeException("PutUpdateEntry failed: " + result.getMessage());
            }
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in putUpdateEntry", e);
        }
    }

    public List<PbUpdateEntry> getUpdateEntries(String tableName) {
        try {
            var request = PbGetUpdateEntriesRequest.newBuilder().setTableName(tableName).build();
            return stub.getUpdateEntries(request).getEntriesList();
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in getUpdateEntries", e);
        }
    }

    public void deleteUpdateEntry(String entryId) {
        try {
            var request = PbDeleteUpdateEntryRequest.newBuilder().setEntryId(entryId).build();
            var result = stub.deleteUpdateEntry(request);
            if (!result.getSuccess()) {
                throw new RuntimeException("DeleteUpdateEntry failed: " + result.getMessage());
            }
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC error in deleteUpdateEntry", e);
        }
    }

    @Override
    public void close() {
        INSTANCES.remove(target);
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
