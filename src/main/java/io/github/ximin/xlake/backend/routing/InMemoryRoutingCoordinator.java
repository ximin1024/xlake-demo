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
package io.github.ximin.xlake.backend.routing;

import io.github.ximin.xlake.common.config.XlakeConfig;
import io.github.ximin.xlake.common.config.XlakeOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public final class InMemoryRoutingCoordinator implements RoutingCoordinator {
    private final ShardRoutingTable routingTable;
    private final Map<ShardId, List<ShardRecoveryRecord>> shardHistory = new ConcurrentHashMap<>();
    private final XlakeConfig config;
    private final Map<String, String> executorBasePaths = new ConcurrentHashMap<>();
    private volatile Runnable pendingRecoveryDispatcher;
    private final Map<ShardId, Set<String>> shardTableIdentifiers = new ConcurrentHashMap<>();

    public InMemoryRoutingCoordinator(ShardRoutingTable routingTable) {
        this(routingTable, null);
    }

    public InMemoryRoutingCoordinator(ShardRoutingTable routingTable, XlakeConfig config) {
        this.routingTable = Objects.requireNonNull(routingTable, "routingTable must not be null");
        this.config = config;
    }

    @Override
    public void onExecutorUp(String executorId, NodeSlot nodeSlot) {
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        Objects.requireNonNull(nodeSlot, "nodeSlot must not be null");
        routingTable.registerExecutor(executorId, nodeSlot);
        for (ShardLookupResult routing : routingTable.routings()) {
            ShardAssignment assignment = routing.assignment();
            boolean prefersThisSlot = routingTable.preferredSlotOf(routing.shardId()).filter(nodeSlot::equals).isPresent();
            boolean currentlyOnDifferentSlot = assignment == null || !assignment.nodeSlot().equals(nodeSlot);
            if (routing.status() == RoutingStatus.REASSIGNING
                    || (prefersThisSlot && currentlyOnDifferentSlot && routing.status() != RoutingStatus.RECOVERING)) {
                routingTable.reassignShard(routing.shardId());
            }
        }
    }

    @Override
    public void onExecutorReady(String executorId) {
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        routingTable.markExecutorReady(executorId);
    }

    @Override
    public void onExecutorDown(String executorId) {
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        List<ShardAssignment> assignments = routingTable.unregisterExecutorAndCollectAssignments(executorId);
        if (assignments.isEmpty()) {
            log.warn("onExecutorDown called for executor {} but no assignments found (already unregistered or duplicate trigger)", executorId);
            return;
        }
        buildShardHistory(executorId, assignments);
        executorBasePaths.remove(executorId);
        Runnable dispatcher = pendingRecoveryDispatcher;
        if (dispatcher != null) {
            try {
                dispatcher.run();
            } catch (Exception e) {
                log.error("Recovery task dispatcher failed after executor {} down", executorId, e);
            }
        }
    }

    @Override
    public ShardLookupResult lookupOwner(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId must not be null");
        ShardLookupResult result = routingTable.lookup(shardId);
        if (result.status() == RoutingStatus.UNASSIGNED) {
            routingTable.reassignShard(shardId);
            return routingTable.lookup(shardId);
        }
        return result;
    }

    @Override
    public Map<ShardId, ShardLookupResult> lookupOwners(Set<ShardId> shardIds) {
        Objects.requireNonNull(shardIds, "shardIds must not be null");
        Map<ShardId, ShardLookupResult> result = routingTable.lookupBatch(shardIds);
        for (Map.Entry<ShardId, ShardLookupResult> entry : result.entrySet()) {
            if (entry.getValue().status() == RoutingStatus.UNASSIGNED) {
                routingTable.reassignShard(entry.getKey());
            }
        }
        return routingTable.lookupBatch(shardIds);
    }

    public boolean onRecoveryComplete(ShardId shardId, RoutingEpoch epoch, boolean success) {
        if (routingTable instanceof InMemoryShardRoutingTable table) {
            if (success) {
                boolean cleared = table.clearRecoveringFlag(shardId, epoch, () -> shardHistory.remove(shardId));
                return cleared;
            }
        }
        return false;
    }

    public List<ShardRecoveryRecord> getShardHistory(ShardId shardId) {
        List<ShardRecoveryRecord> records = shardHistory.get(shardId);
        return records == null ? Collections.emptyList() : List.copyOf(records);
    }

    public List<ShardLookupResult> getRecoveringShards() {
        return routingTable.routings().stream()
                .filter(r -> r.status() == RoutingStatus.RECOVERING)
                .toList();
    }

    public void registerExecutorBasePath(String executorId, String basePath) {
        if (basePath != null && !basePath.isBlank()) {
            executorBasePaths.put(executorId, basePath);
        }
    }

    public void setRecoveryTaskDispatcher(Runnable dispatcher) {
        this.pendingRecoveryDispatcher = dispatcher;
    }

    public void registerShardTables(ShardId shardId, Set<String> tableIdentifiers) {
        if (tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
            shardTableIdentifiers.put(shardId, Set.copyOf(tableIdentifiers));
        }
    }

    @Override
    public void reassignShard(ShardId shardId) {
        routingTable.reassignShard(shardId);
    }

    @Override
    public void reassignShardForRecovery(ShardId shardId) {
        routingTable.reassignShardForRecovery(shardId);
    }

    private void buildShardHistory(String executorId, List<ShardAssignment> assignments) {
        long lostTimestamp = System.currentTimeMillis();
        String previousBasePath = executorBasePaths.getOrDefault(executorId, "");
        String walHdfsDir = deriveWalHdfsDir(executorId);
        for (ShardAssignment assignment : assignments) {
            Set<String> tables = shardTableIdentifiers.getOrDefault(assignment.shardId(), Set.of());
            ShardRecoveryRecord record = new ShardRecoveryRecord(
                    assignment.shardId(),
                    assignment.executorId(),
                    assignment.nodeSlot().toString(),
                    previousBasePath,
                    walHdfsDir,
                    tables,
                    assignment.epoch(),
                    lostTimestamp
            );
            shardHistory.computeIfAbsent(assignment.shardId(), k -> new CopyOnWriteArrayList<>()).add(record);
        }
    }

    private String deriveWalHdfsDir(String executorId) {
        if (config == null) {
            return "";
        }
        String walHdfsPath = config.get(XlakeOptions.STORAGE_WAL_HDFS_PATH);
        if (walHdfsPath == null || walHdfsPath.isBlank()) {
            return "";
        }
        String base = walHdfsPath.endsWith("/") ? walHdfsPath.substring(0, walHdfsPath.length() - 1) : walHdfsPath;
        return base + "/" + executorId;
    }
}
