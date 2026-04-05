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
        var ratisMeta = RatisMetastore.create(ratisPeers);
        return new GrpcServer(port, ratisMeta);
    }

    public void start() throws IOException {
        grpcServer.start();
        log.info("[gRPC] Metastore server started on port {}", grpcServer.getPort());
        if (metastore instanceof RatisMetastore) {
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
        public void createTable(CreateTableRequest request, StreamObserver<OperationResult> responseObserver) {
            try {
                metastore.createTable(request.getMetadata());
                respondOk(responseObserver, "Table created: " + request.getMetadata().getTableName());
            } catch (Exception e) {
                log.error("[gRPC] createTable failed", e);
                respondError(responseObserver, e);
            }
        }

        @Override
        public void dropTable(DropTableRequest request, StreamObserver<OperationResult> responseObserver) {
            try {
                metastore.dropTable(request.getTableName());
                respondOk(responseObserver, "Table dropped: " + request.getTableName());
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void getTable(GetTableRequest request, StreamObserver<TableMetadata> responseObserver) {
            try {
                var meta = metastore.getTable(request.getTableName())
                        .orElseThrow(() -> new IllegalArgumentException(
                                "Table not found: " + request.getTableName()));
                responseObserver.onNext(meta);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void alterTable(AlterTableRequest request, StreamObserver<OperationResult> responseObserver) {
            try {
                metastore.alterTable(request.getTableName(), request.getNewSchema());
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void getSnapshot(GetSnapshotRequest request, StreamObserver<Snapshot> responseObserver) {
            try {
                var snapshot = metastore.getSnapshot(request.getTableName(), request.getSnapshotId())
                        .orElseThrow(() -> new IllegalArgumentException("Snapshot not found"));
                responseObserver.onNext(snapshot);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void getSnapshots(GetSnapshotsRequest request, StreamObserver<SnapshotList> responseObserver) {
            try {
                var snapshots = metastore.getSnapshots(request.getTableName());
                responseObserver.onNext(SnapshotList.newBuilder()
                        .addAllSnapshots(snapshots)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void beginCommit(CommitRequest request, StreamObserver<OperationResult> responseObserver) {
            try {
                long commitId = metastore.beginCommit(request.getOperationsList());
                responseObserver.onNext(OperationResult.newBuilder()
                        .setSuccess(true)
                        .setCommitId(commitId)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void commit(CommitId request, StreamObserver<OperationResult> responseObserver) {
            try {
                boolean success = metastore.commit(request.getValue());
                responseObserver.onNext(OperationResult.newBuilder()
                        .setSuccess(success)
                        .setCommitId(success ? request.getValue() : 0)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void abortCommit(CommitId request, StreamObserver<OperationResult> responseObserver) {
            try {
                boolean success = metastore.abortCommit(request.getValue());
                respondOk(responseObserver, null, success ? request.getValue() : 0);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void putFile(FileMetadata request, StreamObserver<OperationResult> responseObserver) {
            try {
                metastore.putFile(request);
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void listFiles(ListFilesRequest request, StreamObserver<ListFilesResponse> responseObserver) {
            try {
                List<FileMetadata> files = request.hasLevel()
                        ? metastore.listFiles(request.getTableName(), request.getLevel())
                        : metastore.listFiles(request.getTableName());
                responseObserver.onNext(ListFilesResponse.newBuilder()
                        .addAllFiles(files)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void removeFile(FileMetadata request, StreamObserver<OperationResult> responseObserver) {
            try {
                metastore.removeFile(request.getTableName(), request.getFilePath());
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void putUpdateEntry(PutUpdateEntryRequest request, StreamObserver<OperationResult> responseObserver) {
            try {
                metastore.putUpdateEntry(request.getEntry());
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        @Override
        public void getUpdateEntries(GetUpdateEntriesRequest request, StreamObserver<GetUpdateEntriesResponse> responseObserver) {
            try {
                var entries = metastore.getUpdateEntries(request.getTableName());
                responseObserver.onNext(GetUpdateEntriesResponse.newBuilder()
                        .addAllEntries(entries)
                        .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void deleteUpdateEntry(DeleteUpdateEntryRequest request, StreamObserver<OperationResult> responseObserver) {
            try {
                metastore.deleteUpdateEntry(request.getEntryId());
                respondOk(responseObserver);
            } catch (Exception e) {
                respondError(responseObserver, e);
            }
        }

        private void respondOk(StreamObserver<OperationResult> observer) {
            respondOk(observer, null, 0);
        }

        private void respondOk(StreamObserver<OperationResult> observer, String message) {
            respondOk(observer, message, 0);
        }

        private void respondOk(StreamObserver<OperationResult> observer, String message, long commitId) {
            var builder = OperationResult.newBuilder().setSuccess(true);
            if (message != null) builder.setMessage(message);
            if (commitId > 0) builder.setCommitId(commitId);
            observer.onNext(builder.build());
            observer.onCompleted();
        }

        private void respondError(StreamObserver<OperationResult> observer, Exception e) {
            observer.onNext(OperationResult.newBuilder()
                    .setSuccess(false)
                    .setMessage(e.getMessage())
                    .build());
            observer.onCompleted();
        }
    }
}
