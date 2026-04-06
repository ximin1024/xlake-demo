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
package io.github.ximin.xlake.metastore.ratis;

import com.google.protobuf.ByteString;
import io.github.ximin.xlake.meta.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class Client implements AutoCloseable {

    private final RaftClient client;

    public Client(List<String> peerHostPorts) {
        List<RaftPeer> peers = peerHostPorts.stream()
                .map(addr -> {
                    String[] parts = addr.split(":");
                    return RaftPeer.newBuilder()
                            .setId(RaftPeerId.valueOf("peer-" + parts[0].replace(".", "-")))
                            .setAddress(InetSocketAddress.createUnresolved(parts[0], Integer.parseInt(parts[1])))
                            .build();
                })
                .collect(Collectors.toList());

        RaftGroupId groupId = RaftGroupId.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
                        "nebulake-meta-group".getBytes(StandardCharsets.UTF_8)));
        RaftGroup group = RaftGroup.valueOf(groupId, peers);

        this.client = RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(group)
                .build();
    }

    public static Client createDefault() {
        return create(List.of("localhost:9876"));
    }

    public static Client create(List<String> peers) {
        return new Client(peers);
    }

    public void write(byte[] key, byte[] value) throws IOException {
        var envelope = PbRaftOpEnvelope.newBuilder()
                .setOpType("PUT")
                .setPayload(PbPutRequest.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .build()
                        .toByteString())
                .build();

        Message message = Message.valueOf(String.valueOf(envelope.toByteString()));
        RaftClientReply reply = client.io().send(message);
        if (!reply.isSuccess()) {
            throw new IOException("Ratis write failed: " + reply.getException());
        }
    }

    public Optional<byte[]> read(byte[] key) throws IOException {
        var envelope = PbRaftOpEnvelope.newBuilder()
                .setOpType("GET")
                .setPayload(PbGetRequest.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .build()
                        .toByteString())
                .build();

        Message message = Message.valueOf(String.valueOf(envelope.toByteString()));
        RaftClientReply reply = client.io().sendReadOnly(message);
        if (!reply.isSuccess()) {
            return Optional.empty();
        }

        var response = PbGetResponse.parseFrom(
                reply.getMessage().getContent().toByteArray());
        if (response.getValue().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(response.getValue().toByteArray());
    }

    public void delete(byte[] key) throws IOException {
        var envelope = PbRaftOpEnvelope.newBuilder()
                .setOpType("DELETE")
                .setPayload(PbDeleteRequest.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .build()
                        .toByteString())
                .build();

        Message message = Message.valueOf(String.valueOf(envelope.toByteString()));
        RaftClientReply reply = client.io().send(message);
        if (!reply.isSuccess()) {
            throw new IOException("Ratis delete failed: " + reply.getException());
        }
    }

    public List<byte[]> scanByPrefix(byte[] prefix) throws IOException {
        var envelope = PbRaftOpEnvelope.newBuilder()
                .setOpType("SCAN_PREFIX")
                .setPayload(PbScanPrefixRequest.newBuilder()
                        .setPrefix(ByteString.copyFrom(prefix))
                        .build()
                        .toByteString())
                .build();

        Message message = Message.valueOf(String.valueOf(envelope.toByteString()));
        RaftClientReply reply = client.io().sendReadOnly(message);
        if (!reply.isSuccess()) {
            return List.of();
        }

        var response = PbScanPrefixResponse.parseFrom(
                reply.getMessage().getContent().toByteArray());
        return response.getValuesList().stream()
                .map(com.google.protobuf.ByteString::toByteArray)
                .toList();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
