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
import io.github.ximin.xlake.metastore.Metastore;
import io.github.ximin.xlake.metastore.RatisMetastore;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GrpcServer {

    private static final String DEFAULT_CATALOG = "default";
    private static final String DEFAULT_DATABASE = "default";

    private final Server grpcServer;
    private final Metastore metastore;

    public GrpcServer(int port, Metastore metastore) {
        this.metastore = metastore;
        this.grpcServer = ServerBuilder.forPort(port)
                .maxInboundMessageSize(16 * 1024 * 1024)
                .addService(new MetastoreGrpcServiceImpl(metastore))
                .build();
    }

    public static GrpcServer createWithRatis(int port, List<String> ratisPeers) {
        var ratisMeta = io.github.ximin.xlake.metastore.RatisMetastore.create(ratisPeers);
        return new GrpcServer(port, ratisMeta);
    }

    public void start() throws IOException {
        grpcServer.start();
        log.info("[gRPC] Metastore server started on port {}", grpcServer.getPort());
        if (metastore instanceof io.github.ximin.xlake.metastore.RatisMetastore) {
            log.info("[gRPC] Backend: Ratis + RocksDB (distributed)");
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    public void shutdown() {
        grpcServer.shutdown();
        try {
            if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
            }
        } catch (InterruptedException e) {
            grpcServer.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (metastore instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.error("Error closing metastore backend", e);
            }
        }
        log.info("[gRPC] Server shutdown complete");
    }

    @Slf4j
    static class MetastoreGrpcServiceImpl extends MetastoreServiceGrpc.MetastoreServiceImplBase {

        private final Metastore metastore;

        MetastoreGrpcServiceImpl(Metastore metastore) {
            this.metastore = metastore;
        }

        @Override
        public void createTable(PbCreateTableRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                metastore.createTable(request.getMetadata());
                respondOk(responseObserver, "Table created: " + request.getMetadata().getIdentifier().getTable());
            } catch (Exception e) {
                log.error("[gRPC] createTable failed", e);
                respondError(responseObserver, e);
            }
        }

        @Override
        public void dropTable(PbDropTableRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                metastore.dropTable(DEFAULT_CATALOG, DEFAULT_DATABASE, request.getTableName());
                respondOk(responseObserver, "Table dropped: " + request.getTableName());
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void getTable(PbGetTableRequest request, StreamObserver<PbTableMetadata> responseObserver) {
            try {
                var meta = metastore.getTable(DEFAULT_CATALOG, DEFAULT_DATABASE, request.getTableName())
                        .orElseThrow(() -> new IllegalArgumentException(
                                "Table not found: " + request.getTableName()));
                responseObserver.onNext(meta);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void alterTable(PbAlterTableRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                metastore.alterTable(DEFAULT_CATALOG, DEFAULT_DATABASE, request.getTableName(), request.getNewSchema());
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void getSnapshot(PbGetSnapshotRequest request, StreamObserver<PbSnapshot> responseObserver) {
            try {
                var snapshot = metastore.getSnapshot(DEFAULT_CATALOG, DEFAULT_DATABASE, request.getTableName(), request.getSnapshotId())
                        .orElseThrow(() -> new IllegalArgumentException("Snapshot not found"));
                responseObserver.onNext(snapshot);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void getSnapshots(PbGetSnapshotsRequest request, StreamObserver<PbSnapshotList> responseObserver) {
            try {
                var snapshots = metastore.getSnapshots(DEFAULT_CATALOG, DEFAULT_DATABASE, request.getTableName());
                responseObserver.onNext(PbSnapshotList.newBuilder()
                        .addAllSnapshots(snapshots)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void beginCommit(PbCommitRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                long commitId = metastore.beginCommit(request.getOperationsList());
                responseObserver.onNext(PbOperationResult.newBuilder()
                        .setSuccess(true)
                        .setCommitId(commitId)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void commit(PbCommitId request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                boolean success = metastore.commit(request.getValue());
                responseObserver.onNext(PbOperationResult.newBuilder()
                        .setSuccess(success)
                        .setCommitId(success ? request.getValue() : 0)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void abortCommit(PbCommitId request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                boolean success = metastore.abortCommit(request.getValue());
                respondOk(responseObserver, null, success ? request.getValue() : 0);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void putFile(PbFileMetadata request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                metastore.putFile(request);
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void listFiles(PbListFilesRequest request, StreamObserver<PbListFilesResponse> responseObserver) {
            try {
                List<PbFileMetadata> files = request.hasLevel()
                        ? metastore.listFiles(request.getTableName(), request.getLevel())
                        : metastore.listFiles(request.getTableName());
                responseObserver.onNext(PbListFilesResponse.newBuilder()
                        .addAllFiles(files)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void removeFile(PbFileMetadata request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                metastore.removeFile(request.getTableName(), request.getFilePath());
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void putUpdateEntry(PbPutUpdateEntryRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                metastore.putUpdateEntry(request.getEntry());
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void getUpdateEntries(PbGetUpdateEntriesRequest request, StreamObserver<PbGetUpdateEntriesResponse> responseObserver) {
            try {
                var entries = metastore.getUpdateEntries(request.getTableName());
                responseObserver.onNext(PbGetUpdateEntriesResponse.newBuilder()
                        .addAllEntries(entries)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void deleteUpdateEntry(PbDeleteUpdateEntryRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                metastore.deleteUpdateEntry(request.getEntryId());
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        private void respondOk(StreamObserver<PbOperationResult> observer) {
            respondOk(observer, null, 0);
        }

        private void respondOk(StreamObserver<PbOperationResult> observer, String message) {
            respondOk(observer, message, 0);
        }

        private void respondOk(StreamObserver<PbOperationResult> observer, String message, long commitId) {
            var builder = PbOperationResult.newBuilder().setSuccess(true);
            if (message != null) builder.setMessage(message);
            if (commitId > 0) builder.setCommitId(commitId);
            observer.onNext(builder.build());
            observer.onCompleted();
        }

        private void respondError(StreamObserver<PbOperationResult> observer, Exception e) {
            observer.onNext(PbOperationResult.newBuilder()
                    .setSuccess(false)
                    .setMessage(e.getMessage())
                    .build());
            observer.onCompleted();
        }
    }
}
