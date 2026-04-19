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
import io.github.ximin.xlake.common.config.XlakeConfig;
import io.github.ximin.xlake.common.config.XlakeOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.*;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class DriverRoutingEndpoint implements RpcEndpoint {
    public static final String ENDPOINT_NAME = "xlake-driver-routing-endpoint";

    private final RpcEnv rpcEnv;
    private final RoutingCoordinator routingCoordinator;
    private final SparkConf sparkConf;
    private final XlakeConfig xlakeConfig;
    private final Map<String, RpcEndpointRef> executorEndpoints = new ConcurrentHashMap<>();
    private final Map<String, RoutingMessages.ExecutorEndpointInfo> executorEndpointInfos = new ConcurrentHashMap<>();
    private final Map<ShardId, Integer> recoveryRetryCount = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ShardId, DispatchEntry> dispatchedRecoveryShards = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "xlake-recovery-scheduler");
        t.setDaemon(true);
        return t;
    });

    public DriverRoutingEndpoint(RpcEnv rpcEnv, RoutingCoordinator routingCoordinator) {
        this(rpcEnv, routingCoordinator, SparkEnv.get().conf(), null);
    }

    public DriverRoutingEndpoint(RpcEnv rpcEnv, RoutingCoordinator routingCoordinator, SparkConf sparkConf) {
        this(rpcEnv, routingCoordinator, sparkConf, null);
    }

    DriverRoutingEndpoint(RpcEnv rpcEnv, RoutingCoordinator routingCoordinator, SparkConf sparkConf, XlakeConfig xlakeConfig) {
        this.rpcEnv = Objects.requireNonNull(rpcEnv, "rpcEnv must not be null");
        this.routingCoordinator = Objects.requireNonNull(routingCoordinator, "routingCoordinator must not be null");
        this.sparkConf = Objects.requireNonNull(sparkConf, "sparkConf must not be null");
        this.xlakeConfig = xlakeConfig;
        if (routingCoordinator instanceof InMemoryRoutingCoordinator coordinator) {
            coordinator.setRecoveryTaskDispatcher(this::submitRecoveryDispatch);
        }
    }

    @Override
    public RpcEnv rpcEnv() {
        return rpcEnv;
    }

    @Override
    public void onStart() {
        long timeoutMs = getRecoveryTimeoutMs();
        long checkIntervalMs = Math.max(timeoutMs / 2, 1000);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                dispatchPendingRecoveryTasks();
            } catch (Exception e) {
                log.error("Periodic recovery dispatch failed", e);
            }
        }, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onStop() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Recovery scheduler did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executorEndpoints.clear();
        executorEndpointInfos.clear();
        dispatchedRecoveryShards.clear();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        return new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Object message) {
                return message instanceof RecoveryCompleteMessage;
            }

            @Override
            public BoxedUnit apply(Object message) {
                if (message instanceof RecoveryCompleteMessage complete) {
                    handleRecoveryComplete(complete);
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
                        || message instanceof RoutingMessages.LookupShardOwnersMessage;
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
                    submitRecoveryDispatch();
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
                cleanupExecutorEndpoint(assignment.executorId());
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
                return RoutingMessages.WriteAck.retry("mixed_epoch_in_shard");
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
                    cleanupExecutorEndpoint(assignment.executorId());
                }
                return RoutingMessages.WriteAck.retry("forward_failed");
            }
        }
        return RoutingMessages.WriteAck.ok();
    }

    private void cleanupExecutorEndpoint(String executorId) {
        executorEndpoints.remove(executorId);
        executorEndpointInfos.remove(executorId);
        dispatchedRecoveryShards.entrySet().removeIf(e -> executorId.equals(e.getValue().executorId));
    }

    public boolean sendRecoveryTask(RecoveryTaskMessage message) {
        RpcEndpointRef target = executorEndpoints.get(message.targetExecutorId());
        if (target == null) {
            return false;
        }
        try {
            RpcTimeout timeout = askTimeout();
            Object reply = target.askSync(message, timeout,
                    scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            return Boolean.TRUE.equals(reply);
        } catch (Exception e) {
            return false;
        }
    }

    private void dispatchPendingRecoveryTasks() {
        if (!(routingCoordinator instanceof InMemoryRoutingCoordinator coordinator)) {
            return;
        }
        String basePath = getStorageMmapPath();
        String storeId = getStorageStoreId();
        long timeoutMs = getRecoveryTimeoutMs();
        long now = System.currentTimeMillis();
        for (ShardLookupResult routing : coordinator.getRecoveringShards()) {
            ShardAssignment assignment = routing.assignment();
            if (assignment == null) {
                continue;
            }
            ShardId shardId = assignment.shardId();
            String targetExecutorId = assignment.executorId();
            RpcEndpointRef targetRef = executorEndpoints.get(targetExecutorId);
            if (targetRef == null) {
                continue;
            }
            java.util.List<ShardRecoveryRecord> records =
                    coordinator.getShardHistory(shardId);
            if (records.isEmpty()) {
                continue;
            }
            RecoveryTaskMessage message = new RecoveryTaskMessage(
                    targetExecutorId, records, basePath, storeId);
            DispatchEntry entry = new DispatchEntry(targetExecutorId, now);
            dispatchedRecoveryShards.compute(shardId, (key, existing) -> {
                if (existing != null
                        && existing.executorId.equals(targetExecutorId)
                        && (now - existing.dispatchTimestamp) < timeoutMs) {
                    return existing;
                }
                return entry;
            });
            if (dispatchedRecoveryShards.get(shardId) != entry) {
                continue;
            }
            boolean sent = sendRecoveryTask(message);
            if (sent) {
                log.info("Dispatched recovery task for shard {} to executor {}", shardId, targetExecutorId);
            } else {
                dispatchedRecoveryShards.remove(shardId);
            }
        }
    }

    private void handleRecoveryComplete(RecoveryCompleteMessage message) {
        if (routingCoordinator instanceof InMemoryRoutingCoordinator coordinator) {
            if (message.success()) {
                boolean cleared = coordinator.onRecoveryComplete(message.shardId(), message.epoch(), true);
                if (cleared) {
                    recoveryRetryCount.remove(message.shardId());
                    dispatchedRecoveryShards.remove(message.shardId());
                }
            } else {
                coordinator.onRecoveryComplete(message.shardId(), message.epoch(), false);
                int maxRetries = getMaxRetries();
                int currentRetries = recoveryRetryCount.getOrDefault(message.shardId(), 0) + 1;
                if (currentRetries < maxRetries) {
                    recoveryRetryCount.put(message.shardId(), currentRetries);
                    log.warn("Recovery failed for shard {} (attempt {}/{}), will retry. Error: {}",
                            message.shardId(), currentRetries, maxRetries, message.errorMessage());
                    scheduleRecoveryRetry(message.shardId(), message.epoch());
                } else {
                    log.error("Recovery failed for shard {} after {} retries, transitioning shard to RECOVERING on new executor after recovery failure. Error: {}",
                            message.shardId(), maxRetries, message.errorMessage());
                    recoveryRetryCount.remove(message.shardId());
                    dispatchedRecoveryShards.remove(message.shardId());
                    coordinator.reassignShardForRecovery(message.shardId());
                    submitRecoveryDispatch();
                }
            }
        }
    }

    private int getMaxRetries() {
        if (xlakeConfig != null) {
            return xlakeConfig.get(XlakeOptions.RECOVERY_MAX_RETRIES);
        }
        return XlakeOptions.RECOVERY_MAX_RETRIES.defaultValue();
    }

    private void submitRecoveryDispatch() {
        scheduler.submit(() -> {
            try {
                dispatchPendingRecoveryTasks();
            } catch (Exception e) {
                log.error("Failed to dispatch pending recovery tasks", e);
            }
        });
    }

    private long getBackoffMs() {
        if (xlakeConfig != null) {
            return xlakeConfig.get(XlakeOptions.RECOVERY_BACKOFF_MS);
        }
        return XlakeOptions.RECOVERY_BACKOFF_MS.defaultValue();
    }

    private String getStorageMmapPath() {
        if (xlakeConfig != null) {
            return xlakeConfig.get(XlakeOptions.STORAGE_MMAP_PATH);
        }
        return XlakeOptions.STORAGE_MMAP_PATH.defaultValue();
    }

    private String getStorageStoreId() {
        if (xlakeConfig != null) {
            return xlakeConfig.get(XlakeOptions.STORAGE_STORE_ID);
        }
        return XlakeOptions.STORAGE_STORE_ID.defaultValue();
    }

    private long getRecoveryTimeoutMs() {
        if (xlakeConfig != null) {
            return xlakeConfig.get(XlakeOptions.RECOVERY_TIMEOUT_MS);
        }
        return XlakeOptions.RECOVERY_TIMEOUT_MS.defaultValue();
    }

    private void scheduleRecoveryRetry(ShardId shardId, RoutingEpoch epoch) {
        long backoffMs = getBackoffMs();
        scheduler.schedule(() -> {
            try {
                ShardLookupResult lookup = routingCoordinator.lookupOwner(shardId);
                if (lookup.assignment() != null) {
                    String targetExecutorId = lookup.assignment().executorId();
                    java.util.List<ShardRecoveryRecord> records =
                            routingCoordinator instanceof InMemoryRoutingCoordinator coordinator
                                    ? coordinator.getShardHistory(shardId)
                                    : java.util.List.of();
                    if (!records.isEmpty()) {
                        long now = System.currentTimeMillis();
                        dispatchedRecoveryShards.compute(shardId, (key, existing) -> new DispatchEntry(targetExecutorId, now));
                        RecoveryTaskMessage retryMessage = new RecoveryTaskMessage(
                                targetExecutorId, records, getStorageMmapPath(), getStorageStoreId());
                        boolean sent = sendRecoveryTask(retryMessage);
                        if (!sent) {
                            dispatchedRecoveryShards.remove(shardId);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Failed to schedule recovery retry for shard {}", shardId, e);
            }
        }, backoffMs, TimeUnit.MILLISECONDS);
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

    private record DispatchEntry(String executorId, long dispatchTimestamp) {
    }
}
