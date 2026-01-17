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

import io.github.ximin.xlake.metastore.MetastoreProto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class MetastoreGrpcServer extends MetastoreServiceGrpc.MetastoreServiceImplBase {
    private final MetastoreClient ratisClient;
    private final Server grpcServer;

    public MetastoreGrpcServer(int port, MetastoreClient ratisClient) {
        this.ratisClient = ratisClient;
        this.grpcServer = ServerBuilder.forPort(port)
                .addService(this)
                .build();
    }

    @Override
    public void write(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            ratisClient.write(request);
            responseObserver.onNext(PutResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Write failed", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            byte[] value = ratisClient.get(request.getKey().toByteArray());
            if (value == null) {
                responseObserver.onNext(GetResponse.getDefaultInstance());
            } else {
                responseObserver.onNext(GetResponse.newBuilder()
                        .setValue(com.google.protobuf.ByteString.copyFrom(value))
                        .build());
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Get failed", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void listFiles(ListFilesRequest request, StreamObserver<ListFilesResponse> responseObserver) {
        try {
            // TODO: 实现基于辅助索引的范围查询
            // 简化版：从 metastore 中扫描所有文件（实际需用 I#LEVEL#... 索引）
            var response = ListFilesResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("ListFiles failed", e);
            responseObserver.onError(e);
        }
    }

    public void start() throws IOException {
        grpcServer.start();
        log.info("gRPC server started on port {}", grpcServer.getPort());
    }

    public void blockUntilShutdown() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    public void shutdown() {
        grpcServer.shutdown();
        log.info("gRPC server shutdown on port {}", grpcServer.getPort());
    }
}
