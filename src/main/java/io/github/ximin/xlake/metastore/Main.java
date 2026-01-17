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

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;

import java.util.List;

@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        int grpcPort = Integer.parseInt(System.getProperty("grpc.port", "9001"));

        RaftProperties properties = new RaftProperties();
        RaftServer server = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf("node1")) // 必须指定 ID
                .setProperties(properties)
                .setStateMachineRegistry(gid -> new MetastoreStateMachine(
                        new ShardedRocksStore("/tmp/metastore/data", 100)
                ))
                .build();

        server.start();

        MetastoreClient ratisClient = new MetastoreClient(List.of("node1:9002", "node2:9002", "node3:9002")
        );
        MetastoreGrpcServer grpcServer = new MetastoreGrpcServer(grpcPort, ratisClient);
        grpcServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                grpcServer.shutdown();
                server.close();
                ratisClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        grpcServer.blockUntilShutdown();
    }
}
