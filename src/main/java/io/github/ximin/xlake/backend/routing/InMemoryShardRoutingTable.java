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

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class InMemoryShardRoutingTable implements ShardRoutingTable {
    private final Map<ShardId, ShardLookupResult> routings = new LinkedHashMap<>();
    private final Map<ShardId, NodeSlot> preferredSlots = new LinkedHashMap<>();
    private final Map<ShardId, RoutingEpoch> maxEpochs = new LinkedHashMap<>();
    private final Map<String, ExecutorRegistration> executorToSlot = new LinkedHashMap<>();
    private final Map<NodeSlot, String> slotToExecutor = new LinkedHashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    @Override
    public ShardLookupResult lookup(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId must not be null");
        readLock.lock();
        try {
            return routings.getOrDefault(shardId, ShardLookupResult.unassigned(shardId));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<ShardId, ShardLookupResult> lookupBatch(Set<ShardId> shardIds) {
        validateShardIds(shardIds);
        readLock.lock();
        try {
            Map<ShardId, ShardLookupResult> result = new LinkedHashMap<>();
            for (ShardId shardId : shardIds) {
                result.put(shardId, routings.getOrDefault(shardId, ShardLookupResult.unassigned(shardId)));
            }
            return result;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void registerExecutor(String executorId, NodeSlot nodeSlot) {
        validateExecutorId(executorId);
        Objects.requireNonNull(nodeSlot, "nodeSlot must not be null");
        writeLock.lock();
        try {
            ExecutorRegistration oldRegistration = executorToSlot.get(executorId);
            if (oldRegistration != null && !oldRegistration.nodeSlot().equals(nodeSlot)) {
                throw new IllegalStateException("Executor " + executorId + " is already registered on " + oldRegistration.nodeSlot());
            }
            String previousExecutor = slotToExecutor.get(nodeSlot);
            if (previousExecutor != null && !previousExecutor.equals(executorId)) {
                throw new IllegalStateException("Slot " + nodeSlot + " is already owned by " + previousExecutor);
            }
            boolean ready = oldRegistration != null && oldRegistration.ready();
            executorToSlot.put(executorId, new ExecutorRegistration(nodeSlot, ready));
            slotToExecutor.put(nodeSlot, executorId);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void markExecutorReady(String executorId) {
        validateExecutorId(executorId);
        writeLock.lock();
        try {
            ExecutorRegistration registration = executorToSlot.get(executorId);
            if (registration == null) {
                throw new IllegalStateException("Executor is not registered: " + executorId);
            }
            if (registration.ready()) {
                return;
            }
            executorToSlot.put(executorId, registration.asReady());
            for (Map.Entry<ShardId, ShardLookupResult> entry : routings.entrySet()) {
                ShardLookupResult routing = entry.getValue();
                ShardAssignment assignment = routing.assignment();
                if (routing.status() == RoutingStatus.PENDING_READY
                        && assignment != null
                        && assignment.executorId().equals(executorId)) {
                    entry.setValue(ShardLookupResult.assigned(assignment));
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void unregisterExecutorAndReassign(String executorId) {
        validateExecutorId(executorId);
        writeLock.lock();
        try {
            Set<ShardId> affectedShards = routings.values().stream()
                    .map(ShardLookupResult::assignment)
                    .filter(Objects::nonNull)
                    .filter(assignment -> assignment.executorId().equals(executorId))
                    .map(ShardAssignment::shardId)
                    .collect(java.util.stream.Collectors.toSet());

            removeExecutorMapping(executorId);

            for (ShardId shardId : affectedShards) {
                reassignShardUnderWriteLock(shardId);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void assignShard(ShardAssignment assignment) {
        Objects.requireNonNull(assignment, "assignment must not be null");
        writeLock.lock();
        try {
            validateExecutorPlacement(assignment.executorId(), assignment.nodeSlot());
            RoutingEpoch maxEpoch = maxEpochs.get(assignment.shardId());
            if (maxEpoch != null && assignment.epoch().compareTo(maxEpoch) <= 0) {
                throw new IllegalStateException(
                        "Assignment epoch must advance for shard " + assignment.shardId()
                );
            }
            preferredSlots.put(assignment.shardId(), assignment.nodeSlot());
            maxEpochs.put(assignment.shardId(), assignment.epoch());
            routings.put(assignment.shardId(), resolveLookupResult(assignment));
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void reassignShard(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId must not be null");
        writeLock.lock();
        try {
            reassignShardUnderWriteLock(shardId);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Collection<ShardAssignment> assignments() {
        readLock.lock();
        try {
            return routings.values().stream()
                    .map(ShardLookupResult::assignment)
                    .filter(Objects::nonNull)
                    .toList();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<ShardLookupResult> routings() {
        readLock.lock();
        try {
            return List.copyOf(routings.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<NodeSlot> nodeSlotOf(String executorId) {
        validateExecutorId(executorId);
        readLock.lock();
        try {
            ExecutorRegistration registration = executorToSlot.get(executorId);
            return registration == null ? Optional.empty() : Optional.of(registration.nodeSlot());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<NodeSlot> preferredSlotOf(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId must not be null");
        readLock.lock();
        try {
            return Optional.ofNullable(preferredSlots.get(shardId));
        } finally {
            readLock.unlock();
        }
    }

    private void validateExecutorPlacement(String executorId, NodeSlot nodeSlot) {
        ExecutorRegistration registration = executorToSlot.get(executorId);
        if (registration == null) {
            throw new IllegalStateException("Executor is not registered: " + executorId);
        }
        NodeSlot registeredSlot = registration.nodeSlot();
        if (!registeredSlot.equals(nodeSlot)) {
            throw new IllegalStateException("Executor " + executorId + " is registered on " + registeredSlot + ", not " + nodeSlot);
        }
        String currentExecutor = slotToExecutor.get(nodeSlot);
        if (!executorId.equals(currentExecutor)) {
            throw new IllegalStateException("Slot " + nodeSlot + " is currently owned by " + currentExecutor + ", not " + executorId);
        }
    }

    private NodeSlot selectFallbackSlot(ShardId shardId) {
        List<NodeSlot> liveSlots = new ArrayList<>(slotToExecutor.keySet());
        liveSlots.sort(Comparator.naturalOrder());
        int index = Math.floorMod(shardId.value(), liveSlots.size());
        return liveSlots.get(index);
    }

    private void removeExecutorMapping(String executorId) {
        ExecutorRegistration registration = executorToSlot.remove(executorId);
        if (registration != null) {
            NodeSlot slot = registration.nodeSlot();
            String current = slotToExecutor.get(slot);
            if (executorId.equals(current)) {
                slotToExecutor.remove(slot);
            }
        }
    }

    private void reassignShardUnderWriteLock(ShardId shardId) {
        ShardLookupResult currentRouting = routings.get(shardId);
        ShardAssignment current = currentRouting == null ? null : currentRouting.assignment();
        NodeSlot preferred = preferredSlots.get(shardId);
        if (preferred == null && current != null) {
            preferred = current.nodeSlot();
            preferredSlots.put(shardId, preferred);
        }
        if (slotToExecutor.isEmpty()) {
            routings.put(shardId, ShardLookupResult.reassigning(shardId));
            return;
        }
        NodeSlot target = preferred != null && slotToExecutor.containsKey(preferred)
                ? preferred
                : selectFallbackSlot(shardId);
        if (preferred == null) {
            preferredSlots.put(shardId, target);
        }
        RoutingEpoch maxEpoch = maxEpochs.get(shardId);
        RoutingEpoch nextEpoch = maxEpoch == null ? RoutingEpoch.initial() : maxEpoch.next();
        ShardAssignment assignment = new ShardAssignment(shardId, target, slotToExecutor.get(target), nextEpoch);
        maxEpochs.put(shardId, nextEpoch);
        routings.put(shardId, resolveLookupResult(assignment));
    }

    private ShardLookupResult resolveLookupResult(ShardAssignment assignment) {
        return isExecutorReady(assignment.executorId())
                ? ShardLookupResult.assigned(assignment)
                : ShardLookupResult.pendingReady(assignment);
    }

    private boolean isExecutorReady(String executorId) {
        ExecutorRegistration registration = executorToSlot.get(executorId);
        return registration != null && registration.ready();
    }

    private static void validateExecutorId(String executorId) {
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
    }

    private static void validateShardIds(Set<ShardId> shardIds) {
        if (shardIds == null) {
            throw new IllegalArgumentException("shardIds must not be null");
        }
        if (shardIds.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("shardIds must not contain null");
        }
    }

    private record ExecutorRegistration(NodeSlot nodeSlot, boolean ready) {
        private ExecutorRegistration {
            Objects.requireNonNull(nodeSlot, "nodeSlot must not be null");
        }

        private ExecutorRegistration asReady() {
            return new ExecutorRegistration(nodeSlot, true);
        }
    }
}
