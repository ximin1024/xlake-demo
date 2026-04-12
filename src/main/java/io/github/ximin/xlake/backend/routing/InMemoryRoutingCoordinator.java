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

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class InMemoryRoutingCoordinator implements RoutingCoordinator {
    private final ShardRoutingTable routingTable;

    public InMemoryRoutingCoordinator(ShardRoutingTable routingTable) {
        this.routingTable = Objects.requireNonNull(routingTable);
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
                    || (prefersThisSlot && currentlyOnDifferentSlot)) {
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
        routingTable.unregisterExecutorAndReassign(executorId);
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
}
