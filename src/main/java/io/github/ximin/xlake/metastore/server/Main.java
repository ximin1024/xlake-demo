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

import io.github.ximin.xlake.metastore.RocksdbMetastore;
import io.github.ximin.xlake.metastore.ratis.MetastoreStateMachine;
import io.github.ximin.xlake.metastore.storage.ShardedRocksStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        int grpcPort = Integer.parseInt(System.getProperty("grpc.port", "9001"));
        int ratisPort = Integer.parseInt(System.getProperty("ratis.port", "9876"));
        String dataPath = System.getProperty("metastore.data.path", "/tmp/xlake/metastore/data");
        int numShards = Integer.parseInt(System.getProperty("metastore.shards", "16"));
        String nodeId = System.getProperty("node.id", "node1");

        log.info("=== Xlake Metastore Starting ===");
        log.info("  Node ID:      {}", nodeId);
        log.info("  gRPC Port:    {}", grpcPort);
        log.info("  Ratis Port:   {}", ratisPort);
        log.info("  Data Path:    {}", dataPath);
        log.info("  Shards:       {} (consistent hashing)", numShards);

        RocksdbMetastore metastore = new RocksdbMetastore(dataPath, numShards);
        ShardedRocksStore rocksStore = metastore.getStore();
        MetastoreStateMachine stateMachine = new MetastoreStateMachine(rocksStore);

        RaftPeer localPeer = RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(nodeId))
                .setAddress(InetSocketAddress.createUnresolved("localhost", ratisPort))
                .build();

        RaftGroupId groupId = RaftGroupId.valueOf(
                ByteString.copyFrom("xlake-meta-group".getBytes(StandardCharsets.UTF_8)));
        RaftGroup raftGroup = RaftGroup.valueOf(groupId, Collections.singletonList(localPeer));

        RaftProperties properties = new RaftProperties();

        RaftServer ratisServer = RaftServer.newBuilder()
                .setGroup(raftGroup)
                .setProperties(properties)
                .setStateMachine(stateMachine)
                .setServerId(localPeer.getId())
                .build();

        ratisServer.start();
        log.info("[Ratis] Server started on port {} (node={})", ratisPort, nodeId);

        GrpcServer grpcServer = new GrpcServer(grpcPort, metastore);
        grpcServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("=== Shutting down Xlake Metastore ===");
            try {
                grpcServer.shutdown();
            } catch (Exception e) {
                log.error("Error shutting down gRPC", e);
            }
            try {
                ratisServer.close();
            } catch (IOException e) {
                log.error("Error shutting down Ratis", e);
            }
            try {
                metastore.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

        log.info("=== Xlake Metastore Ready ===");
        log.info("  gRPC endpoint:  localhost:{}", grpcPort);
        log.info("  Ratis endpoint: localhost:{}", ratisPort);
        log.info("  Backend:       Ratis + RocksDB (distributed, consistent hashing)");

        grpcServer.blockUntilShutdown();
    }
}
