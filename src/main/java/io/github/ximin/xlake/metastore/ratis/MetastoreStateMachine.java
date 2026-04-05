package io.github.ximin.xlake.metastore.ratis;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.ximin.xlake.meta.*;
import io.github.ximin.xlake.metastore.storage.ShardedRocksStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class MetastoreStateMachine extends BaseStateMachine {

    private final ShardedRocksStore store;

    public MetastoreStateMachine(ShardedRocksStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        try {
            byte[] data = request.getContent().toByteArray();
            String opType = extractOpType(data);

            return switch (opType) {
                case "GET" -> handleGet(data);
                case "SCAN_PREFIX" -> handleScanPrefix(data);
                default -> CompletableFuture.failedFuture(
                        new IllegalArgumentException("Unknown query op: " + opType));
            };
        } catch (Exception e) {
            LOG.error("Query failed", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        try {
            byte[] logData = trx.getLogEntry().getStateMachineLogEntry().getLogData().toByteArray();
            String opType = extractOpType(logData);

            Message result = switch (opType) {
                case "PUT" -> handlePut(logData);
                case "DELETE" -> handleDelete(logData);
                default -> {
                    LOG.warn("Unknown transaction op: {}", opType);
                    yield Message.EMPTY;
                }
            };

            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            LOG.error("ApplyTransaction failed", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void pause() {}

    @Override
    public void reinitialize() {}

    public void kvPut(byte[] key, byte[] value) throws IOException {
        store.put(key, value);
    }

    public byte[] kvGet(byte[] key) throws IOException {
        return store.get(key);
    }

    public void kvDelete(byte[] key) throws IOException {
        store.delete(key);
    }

    public List<byte[]> kvScanByPrefix(byte[] prefix) {
        String prefixStr = new String(prefix, StandardCharsets.UTF_8);
        List<byte[]> results = new ArrayList<>();
        for (int i = 0; i < store.getNumShards(); i++) {
            var iterator = store.newIterator(i);
            iterator.seek(prefix);
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (key.startsWith(prefixStr)) {
                    results.add(iterator.value());
                } else {
                    break;
                }
                iterator.next();
            }
            iterator.close();
        }
        return results;
    }

    private String extractOpType(byte[] data) throws InvalidProtocolBufferException {
        var envelope = RaftOpEnvelope.parseFrom(data);
        return envelope.getOpType();
    }

    private CompletableFuture<Message> handleGet(byte[] data) throws IOException {
        var envelope = RaftOpEnvelope.parseFrom(data);
        var getRequest = GetRequest.parseFrom(envelope.getPayload());
        byte[] value = store.get(getRequest.getKey().toByteArray());

        GetResponse response;
        if (value == null) {
            response = GetResponse.getDefaultInstance();
        } else {
            response = GetResponse.newBuilder()
                    .setValue(com.google.protobuf.ByteString.copyFrom(value))
                    .build();
        }
        return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(response.toByteString())));
    }

    private CompletableFuture<Message> handleScanPrefix(byte[] data) throws InvalidProtocolBufferException {
        var envelope = RaftOpEnvelope.parseFrom(data);
        var scanRequest = ScanPrefixRequest.parseFrom(envelope.getPayload());
        List<byte[]> values = kvScanByPrefix(scanRequest.getPrefix().toByteArray());

        ScanPrefixResponse response = ScanPrefixResponse.newBuilder()
                .addAllValues(values.stream()
                        .map(com.google.protobuf.ByteString::copyFrom)
                        .toList())
                .build();
        return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(response.toByteString())));
    }

    private Message handlePut(byte[] data) throws IOException {
        var envelope = RaftOpEnvelope.parseFrom(data);
        var putRequest = PutRequest.parseFrom(envelope.getPayload());
        store.put(putRequest.getKey().toByteArray(), putRequest.getValue().toByteArray());
        return Message.EMPTY;
    }

    private Message handleDelete(byte[] data) throws IOException {
        var envelope = RaftOpEnvelope.parseFrom(data);
        var deleteRequest = DeleteRequest.parseFrom(envelope.getPayload());
        store.delete(deleteRequest.getKey().toByteArray());
        return Message.EMPTY;
    }
}
