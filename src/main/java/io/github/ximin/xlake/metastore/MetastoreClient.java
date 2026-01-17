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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class MetastoreClient implements AutoCloseable {
    private final RaftClient client;

    public MetastoreClient(List<String> peerHostPorts) {

        // 1. 配置 Server Peer（ID 和地址必须匹配 Server）
//        RaftPeer serverPeer = RaftPeer.newBuilder()
//                .setId(RaftPeerId.valueOf("node1"))
//                .setAddress(String.valueOf(HostAndPort.fromParts("localhost", 9876)))
//                .build();

        List<RaftPeer> peers = peerHostPorts.stream()
                .map(addr -> {
                    String[] parts = addr.split(":");
                    return RaftPeer.newBuilder()
                            .setId(RaftPeerId.valueOf(parts[0]))
                            .setAddress(addr)
                            .build();
                })
                .collect(Collectors.toList());

        RaftGroupId groupId = RaftGroupId.valueOf(ByteString.copyFrom("xlake-meta".getBytes(StandardCharsets.UTF_8)));
        RaftGroup group = RaftGroup.valueOf(groupId, peers);

        // 2. 创建 Client
        try (RaftClient client = RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(group)
                .build()) {
            this.client = client;
            // 3. 发送消息（需你的 StateMachine 支持）
            Message message = Message.valueOf("Hello Raft");
            RaftClientReply reply = client.io().send(message);
            System.out.println("Reply: " + reply);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(MetastoreProto.PutRequest request) throws Exception {
        Message message = Message.valueOf(String.valueOf(request.toByteString()));
        RaftClientReply reply = client.io().send(message);
        if (!reply.isSuccess()) {
            throw new RuntimeException("Write failed: " + reply.getException());
        }
    }

    public byte[] get(byte[] key) throws Exception {
        var request = MetastoreProto.GetRequest.newBuilder()
                .setKey(com.google.protobuf.ByteString.copyFrom(key))
                .build();
        Message message = Message.valueOf(String.valueOf(request.toByteString()));
        RaftClientReply reply = client.io().sendReadOnly(message);
        if (!reply.isSuccess()) return null;
        var response = MetastoreProto.GetResponse.parseFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
        return response.getValue().isEmpty() ? null : response.getValue().toByteArray();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
