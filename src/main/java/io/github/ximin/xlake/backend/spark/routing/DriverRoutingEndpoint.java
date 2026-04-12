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
package io.github.ximin.xlake.backend.spark.routing;

import io.github.ximin.xlake.backend.routing.*;
import io.github.ximin.xlake.storage.block.DataBlock;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.*;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;

import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DriverRoutingEndpoint implements RpcEndpoint {
    public static final String ENDPOINT_NAME = "xlake-driver-routing-endpoint";

    private final RpcEnv rpcEnv;
    private final RoutingCoordinator routingCoordinator;
    private final SparkConf sparkConf;
    private final Map<String, RpcEndpointRef> executorEndpoints = new ConcurrentHashMap<>();
    private final Map<String, RoutingMessages.ExecutorEndpointInfo> executorEndpointInfos = new ConcurrentHashMap<>();

    public DriverRoutingEndpoint(RpcEnv rpcEnv, RoutingCoordinator routingCoordinator) {
        this(rpcEnv, routingCoordinator, SparkEnv.get().conf());
    }

    DriverRoutingEndpoint(RpcEnv rpcEnv, RoutingCoordinator routingCoordinator, SparkConf sparkConf) {
        this.rpcEnv = Objects.requireNonNull(rpcEnv, "rpcEnv must not be null");
        this.routingCoordinator = Objects.requireNonNull(routingCoordinator, "routingCoordinator must not be null");
        this.sparkConf = Objects.requireNonNull(sparkConf, "sparkConf must not be null");
    }

    @Override
    public RpcEnv rpcEnv() {
        return rpcEnv;
    }

    @Override
    public void onStart() {
    }

    @Override
    public void onStop() {
        executorEndpoints.clear();
        executorEndpointInfos.clear();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        return new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Object message) {
                return message instanceof RoutingMessages.RegisterExecutorEndpointMessage;
            }

            @Override
            public BoxedUnit apply(Object message) {
                if (message instanceof RoutingMessages.RegisterExecutorEndpointMessage(
                        String executorId, String host, int port, String endpointName
                )) {
                    RpcAddress address = RpcAddress.apply(host, port);
                    RpcEndpointRef ref = rpcEnv.setupEndpointRef(address, endpointName);
                    executorEndpoints.put(executorId, ref);
                }
                return BoxedUnit.UNIT;
            }
        };
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveAndReply(RpcCallContext context) {
        return new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Object message) {
                return message instanceof RoutingMessages.ShardWriteRequest
                        || message instanceof RoutingMessages.MultiShardWriteRequest
                        || message instanceof RoutingMessages.RegisterExecutorEndpointMessage
                        || message instanceof RoutingMessages.LookupExecutorEndpointMessage
                        || message instanceof RoutingMessages.LookupShardOwnerMessage
                        || message instanceof RoutingMessages.LookupShardOwnersMessage
                        || message instanceof RoutingMessages.QueryGlobalBlocksMessage;
            }

            @Override
            public BoxedUnit apply(Object message) {
                if (message instanceof RoutingMessages.RegisterExecutorEndpointMessage(
                        String executorId, String host, int port, String endpointName
                )) {
                    RpcAddress address = RpcAddress.apply(host, port);
                    RpcEndpointRef ref = rpcEnv.setupEndpointRef(address, endpointName);
                    executorEndpoints.put(executorId, ref);
                    executorEndpointInfos.put(executorId,
                            new RoutingMessages.ExecutorEndpointInfo(executorId, host, port, endpointName));
                    context.reply(Boolean.TRUE);
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages.ShardWriteRequest request) {
                    context.reply(handleWrite(request));
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages.MultiShardWriteRequest request) {
                    context.reply(handleMultiShardWrite(request));
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages.LookupShardOwnerMessage(int shardId)) {
                    context.reply(routingCoordinator.lookupOwner(new ShardId(shardId)));
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages.LookupExecutorEndpointMessage(String executorId)) {
                    context.reply(executorEndpointInfos.get(executorId));
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages
                        .LookupShardOwnersMessage(java.util.Set<Integer> shardIds)) {
                    context.reply(routingCoordinator.lookupOwners(
                            shardIds.stream()
                                    .map(ShardId::new)
                                    .collect(java.util.stream.Collectors.toSet())
                    ));
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages.QueryGlobalBlocksMessage query) {
                    context.reply(handleQueryGlobalBlocks(query));
                }
                return BoxedUnit.UNIT;
            }
        };
    }

    private RoutingMessages.WriteAck handleWrite(RoutingMessages.ShardWriteRequest request) {
        ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(request.shardId()));
        if (lookup.status() != RoutingStatus.ASSIGNED) {
            return RoutingMessages.WriteAck.retry("routing_status=" + lookup.status());
        }
        ShardAssignment assignment = lookup.assignment();
        if (assignment.epoch().value() != request.epoch()) {
            return RoutingMessages.WriteAck.staleEpoch("epoch_mismatch");
        }
        RpcEndpointRef target = executorEndpoints.get(assignment.executorId());
        if (target == null) {
            return RoutingMessages.WriteAck.retry("executor_endpoint_missing");
        }
        RoutingMessages.ShardWriteForward forward = new RoutingMessages.ShardWriteForward(
                request.basePath(),
                request.storeId(),
                request.tableIdentifier(),
                request.shardId(),
                request.epoch(),
                request.batchId(),
                request.keys(),
                request.values()
        );
        try {
            RpcTimeout timeout = askTimeout();
            Object reply = target.askSync(forward, timeout,
                    scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            if (reply instanceof RoutingMessages.WriteAck ack) {
                return ack;
            }
            return RoutingMessages.WriteAck.error("invalid_reply");
        } catch (Exception e) {
            if (isRpcTimeout(e)) {
                // Timeout is not a proof of executor death. Keep routing stable and let the caller retry.
                return RoutingMessages.WriteAck.retry("forward_timeout");
            }
            if (isHardRpcFailure(e)) {
                routingCoordinator.onExecutorDown(assignment.executorId());
            }
            return RoutingMessages.WriteAck.retry("forward_failed");
        }
    }

    private RoutingMessages.WriteAck handleMultiShardWrite(RoutingMessages.MultiShardWriteRequest request) {
        if (request.keys() == null || request.values() == null || request.shardIds() == null || request.epochs() == null) {
            return RoutingMessages.WriteAck.error("invalid_batch");
        }
        if (request.keys().length != request.values().length
                || request.keys().length != request.shardIds().length
                || request.keys().length != request.epochs().length) {
            return RoutingMessages.WriteAck.error("invalid_batch_len");
        }
        if (request.keys().length == 0) {
            return RoutingMessages.WriteAck.ok();
        }

        HashMap<Integer, ShardBatch> batches = new HashMap<>();
        for (int i = 0; i < request.keys().length; i++) {
            int shardId = request.shardIds()[i];
            long epoch = request.epochs()[i];
            ShardBatch batch = batches.computeIfAbsent(shardId, ignored -> new ShardBatch(epoch));
            if (batch.epoch != epoch) {
                return RoutingMessages.WriteAck.error("mixed_epoch_in_shard");
            }
            batch.add(request.keys()[i], request.values()[i]);
        }

        // Pre-check routing/epochs/endpoints before doing any forwarding to reduce partial success.
        HashMap<Integer, ShardAssignment> assignments = new HashMap<>(batches.size());
        for (Map.Entry<Integer, ShardBatch> e : batches.entrySet()) {
            int shardId = e.getKey();
            ShardBatch batch = e.getValue();
            ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(shardId));
            if (lookup == null || lookup.status() != RoutingStatus.ASSIGNED) {
                return RoutingMessages.WriteAck.retry("routing_status=" + (lookup == null ? "null" : lookup.status()));
            }
            ShardAssignment assignment = lookup.assignment();
            if (assignment == null) {
                return RoutingMessages.WriteAck.retry("missing_assignment");
            }
            if (assignment.epoch().value() != batch.epoch) {
                return RoutingMessages.WriteAck.staleEpoch("epoch_mismatch");
            }
            RpcEndpointRef target = executorEndpoints.get(assignment.executorId());
            if (target == null) {
                return RoutingMessages.WriteAck.retry("executor_endpoint_missing");
            }
            assignments.put(shardId, assignment);
        }

        RpcTimeout timeout = askTimeout();
        for (Map.Entry<Integer, ShardBatch> e : batches.entrySet()) {
            int shardId = e.getKey();
            ShardBatch batch = e.getValue();
            ShardAssignment assignment = assignments.get(shardId);
            RpcEndpointRef target = executorEndpoints.get(assignment.executorId());

            RoutingMessages.ShardWriteForward forward = new RoutingMessages.ShardWriteForward(
                    request.basePath(),
                    request.storeId(),
                    request.tableIdentifier(),
                    shardId,
                    batch.epoch,
                    request.batchId(),
                    batch.keys(),
                    batch.values()
            );
            try {
                Object reply = target.askSync(forward, timeout,
                        scala.reflect.ClassTag$.MODULE$.apply(Object.class));
                if (!(reply instanceof RoutingMessages.WriteAck ack)) {
                    return RoutingMessages.WriteAck.error("invalid_reply");
                }
                if (ack.status() != RoutingMessages.WriteAckStatus.OK) {
                    return ack;
                }
            } catch (Exception ex) {
                if (isRpcTimeout(ex)) {
                    // Same contract as single-shard: timeout is retryable and does NOT mark executor down.
                    return RoutingMessages.WriteAck.retry("forward_timeout");
                }
                if (isHardRpcFailure(ex)) {
                    routingCoordinator.onExecutorDown(assignment.executorId());
                }
                return RoutingMessages.WriteAck.retry("forward_failed");
            }
        }
        return RoutingMessages.WriteAck.ok();
    }

    private RoutingMessages.GlobalBlocksResponse handleQueryGlobalBlocks(RoutingMessages.QueryGlobalBlocksMessage query) {
        RoutingMessages.QueryLocalBlocksMessage localQuery =
                new RoutingMessages.QueryLocalBlocksMessage(query.basePath(), query.storeId(), query.tableIdentifier());
        RpcTimeout timeout = askTimeout();
        ArrayList<DataBlock> aggregated = new ArrayList<>();
        for (Map.Entry<String, RpcEndpointRef> entry : executorEndpoints.entrySet()) {
            try {
                Object reply = entry.getValue().askSync(localQuery, timeout,
                        scala.reflect.ClassTag$.MODULE$.apply(Object.class));
                if (reply instanceof RoutingMessages.LocalBlocksResponse response) {
                    aggregated.addAll(response.blocks());
                }
            } catch (Exception e) {
                // Executor unreachable — skip its blocks. Read planning will work with whatever is available.
            }
        }
        return new RoutingMessages.GlobalBlocksResponse(aggregated);
    }

    private RpcTimeout askTimeout() {
        return RpcTimeout.apply(sparkConf, "spark.rpc.askTimeout", "30s");
    }

    private static boolean isRpcTimeout(Throwable t) {
        Throwable cur = t;
        for (int i = 0; i < 32 && cur != null; i++) {
            if (cur instanceof RpcTimeoutException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    
    private static boolean isHardRpcFailure(Throwable t) {
        Throwable cur = t;
        for (int i = 0; i < 32 && cur != null; i++) {
            if (cur instanceof RpcEndpointNotFoundException) {
                return true;
            }
            if (cur instanceof RpcEnvStoppedException) {
                return true;
            }
            if (cur instanceof ConnectException) {
                return true;
            }
            if (cur instanceof java.nio.channels.ClosedChannelException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    private static final class ShardBatch {
        private final long epoch;
        private byte[][] keys = new byte[128][];
        private byte[][] values = new byte[128][];
        private int size = 0;

        private ShardBatch(long epoch) {
            this.epoch = epoch;
        }

        private void add(byte[] key, byte[] value) {
            if (size == keys.length) {
                int newCap = Math.min(Integer.MAX_VALUE - 8, Math.max(16, size * 2));
                keys = java.util.Arrays.copyOf(keys, newCap);
                values = java.util.Arrays.copyOf(values, newCap);
            }
            keys[size] = key;
            values[size] = value;
            size++;
        }

        private byte[][] keys() {
            return size == keys.length ? keys : java.util.Arrays.copyOf(keys, size);
        }

        private byte[][] values() {
            return size == values.length ? values : java.util.Arrays.copyOf(values, size);
        }
    }
}
