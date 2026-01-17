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
package io.github.ximin.xlake.backend.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RpcServer {
    private Server server;
    private final ExecutorService executor;
    private final int port;

    public RpcServer(int port) {
        this.port = port;
        this.executor = Executors.newFixedThreadPool(10);
    }

    public void start() throws Exception {
        // 创建gRPC服务器
//        server = ServerBuilder.forPort(port)
//                .addService(new QueryServiceImpl())
//                .addService(new JobManagementServiceImpl())
//                .executor(executor)
//                .build()
//                .start();

        System.out.println("RPC Server started on port " + port);

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            shutdown();
            System.err.println("*** server shut down");
        }));

        server.awaitTermination();
    }

    public void shutdown() {
        if (server != null) {
            server.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }
}
