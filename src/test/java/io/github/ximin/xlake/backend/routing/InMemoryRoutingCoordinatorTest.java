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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryRoutingCoordinatorTest {
    private InMemoryShardRoutingTable routingTable;
    private InMemoryRoutingCoordinator coordinator;

    @BeforeEach
    void setUp() {
        routingTable = new InMemoryShardRoutingTable();
        coordinator = new InMemoryRoutingCoordinator(routingTable);
    }

    @Test
    void shouldLookupAssignedShardAndBatch() {
        coordinator.onExecutorUp("exec-1", new NodeSlot("node-a", 0));
        coordinator.onExecutorUp("exec-2", new NodeSlot("node-b", 0));
        coordinator.onExecutorReady("exec-1");
        coordinator.onExecutorReady("exec-2");

        routingTable.assignShard(new ShardAssignment(new ShardId(0), new NodeSlot("node-a", 0), "exec-1", RoutingEpoch.initial()));
        routingTable.assignShard(new ShardAssignment(new ShardId(1), new NodeSlot("node-b", 0), "exec-2", RoutingEpoch.initial()));

        assertThat(coordinator.lookupOwner(new ShardId(0)).assigned()).isTrue();
        assertThat(coordinator.lookupOwner(new ShardId(0)).assignment().executorId()).isEqualTo("exec-1");

        Map<ShardId, ShardLookupResult> batch = coordinator.lookupOwners(Set.of(new ShardId(0), new ShardId(1)));
        assertThat(batch).hasSize(2);
        assertThat(batch.get(new ShardId(0)).assignment().nodeSlot()).isEqualTo(new NodeSlot("node-a", 0));
        assertThat(batch.get(new ShardId(1)).assignment().nodeSlot()).isEqualTo(new NodeSlot("node-b", 0));
    }

    @Test
    void shouldAutoAssignMissingShardInBatchLookup() {
        coordinator.onExecutorUp("exec-1", new NodeSlot("node-a", 0));
        coordinator.onExecutorReady("exec-1");
        routingTable.assignShard(new ShardAssignment(new ShardId(0), new NodeSlot("node-a", 0), "exec-1", RoutingEpoch.initial()));

        Map<ShardId, ShardLookupResult> batch = coordinator.lookupOwners(Set.of(new ShardId(0), new ShardId(7)));
        assertThat(batch.get(new ShardId(0)).status()).isEqualTo(RoutingStatus.ASSIGNED);
        assertThat(batch.get(new ShardId(7)).status()).isEqualTo(RoutingStatus.ASSIGNED);
    }

    @Test
    void shouldReassignShardWhenExecutorGoesDownAndReturnToPreferredSlot() {
        NodeSlot preferred = new NodeSlot("node-a", 1);
        NodeSlot fallback = new NodeSlot("node-b", 0);

        coordinator.onExecutorUp("exec-a1", preferred);
        coordinator.onExecutorUp("exec-b0", fallback);
        coordinator.onExecutorReady("exec-a1");
        coordinator.onExecutorReady("exec-b0");

        routingTable.assignShard(new ShardAssignment(new ShardId(3), preferred, "exec-a1", RoutingEpoch.initial()));

        coordinator.onExecutorDown("exec-a1");

        ShardAssignment drifted = coordinator.lookupOwner(new ShardId(3)).assignment();
        assertThat(drifted.nodeSlot()).isEqualTo(fallback);
        assertThat(drifted.executorId()).isEqualTo("exec-b0");
        assertThat(drifted.epoch()).isEqualTo(new RoutingEpoch(1));
        assertThat(routingTable.preferredSlotOf(new ShardId(3))).contains(preferred);

        coordinator.onExecutorUp("exec-a1-restarted", preferred);

        ShardLookupResult pending = coordinator.lookupOwner(new ShardId(3));
        assertThat(pending.status()).isEqualTo(RoutingStatus.PENDING_READY);
        assertThat(pending.assignment().executorId()).isEqualTo("exec-a1-restarted");

        coordinator.onExecutorReady("exec-a1-restarted");

        ShardAssignment returned = coordinator.lookupOwner(new ShardId(3)).assignment();
        assertThat(returned.nodeSlot()).isEqualTo(preferred);
        assertThat(returned.executorId()).isEqualTo("exec-a1-restarted");
        assertThat(returned.epoch()).isEqualTo(new RoutingEpoch(2));
    }

    @Test
    void shouldRejectAssignmentForUnknownExecutor() {
        assertThatThrownBy(() -> routingTable.assignShard(
                new ShardAssignment(new ShardId(1), new NodeSlot("node-a", 0), "missing", RoutingEpoch.initial())
        )).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not registered");
    }

    @Test
    void shouldRejectNonAdvancingAssignmentEpoch() {
        coordinator.onExecutorUp("exec-a0", new NodeSlot("node-a", 0));
        coordinator.onExecutorReady("exec-a0");
        routingTable.assignShard(new ShardAssignment(new ShardId(1), new NodeSlot("node-a", 0), "exec-a0", RoutingEpoch.initial()));

        assertThatThrownBy(() -> routingTable.assignShard(
                new ShardAssignment(new ShardId(1), new NodeSlot("node-a", 0), "exec-a0", RoutingEpoch.initial())
        )).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("epoch");
    }

    @Test
    void shouldReassignUsingDeterministicFallbackWhenPreferredSlotIsUnavailable() {
        coordinator.onExecutorUp("exec-a0", new NodeSlot("node-a", 0));
        coordinator.onExecutorUp("exec-b1", new NodeSlot("node-b", 1));
        coordinator.onExecutorReady("exec-a0");
        coordinator.onExecutorReady("exec-b1");

        routingTable.reassignShard(new ShardId(5));

        ShardAssignment assignment = coordinator.lookupOwner(new ShardId(5)).assignment();
        assertThat(assignment.nodeSlot()).isEqualTo(new NodeSlot("node-b", 1));
        assertThat(assignment.epoch()).isEqualTo(RoutingEpoch.initial());
    }

    @Test
    void shouldClearOwnerWhenLastExecutorGoesDown() {
        NodeSlot onlySlot = new NodeSlot("node-a", 0);
        coordinator.onExecutorUp("exec-a0", onlySlot);
        coordinator.onExecutorReady("exec-a0");
        routingTable.assignShard(new ShardAssignment(new ShardId(9), onlySlot, "exec-a0", RoutingEpoch.initial()));

        coordinator.onExecutorDown("exec-a0");

        assertThat(coordinator.lookupOwner(new ShardId(9)).status()).isEqualTo(RoutingStatus.REASSIGNING);
        assertThat(routingTable.preferredSlotOf(new ShardId(9))).contains(onlySlot);
    }

    @Test
    void shouldRejectConflictingExecutorRegistration() {
        coordinator.onExecutorUp("exec-a0", new NodeSlot("node-a", 0));

        assertThatThrownBy(() -> coordinator.onExecutorUp("exec-a0", new NodeSlot("node-a", 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already registered");

        assertThatThrownBy(() -> coordinator.onExecutorUp("exec-b0", new NodeSlot("node-a", 0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already owned");
    }

    @Test
    void shouldRejectInvalidInputs() {
        assertThatThrownBy(() -> coordinator.onExecutorUp(" ", new NodeSlot("node-a", 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("executorId");
        assertThatThrownBy(() -> coordinator.lookupOwner(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("shardId");
        Set<ShardId> invalidShardIds = new LinkedHashSet<>();
        invalidShardIds.add(new ShardId(1));
        invalidShardIds.add(null);
        assertThatThrownBy(() -> routingTable.lookupBatch(invalidShardIds))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null");
    }

    @Test
    void shouldRejectStaleAssignmentAfterOwnerDisappears() {
        NodeSlot onlySlot = new NodeSlot("node-a", 0);
        coordinator.onExecutorUp("exec-a0", onlySlot);
        coordinator.onExecutorReady("exec-a0");
        routingTable.assignShard(new ShardAssignment(new ShardId(11), onlySlot, "exec-a0", new RoutingEpoch(5)));

        coordinator.onExecutorDown("exec-a0");
        coordinator.onExecutorUp("exec-a1", onlySlot);

        assertThatThrownBy(() -> routingTable.assignShard(
                new ShardAssignment(new ShardId(11), onlySlot, "exec-a1", new RoutingEpoch(5))
        )).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("epoch");
    }

    @Test
    void shouldExposePendingReadyBeforeExecutorBecomesAvailableForReads() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        coordinator.onExecutorUp("exec-a0", slot);

        routingTable.assignShard(new ShardAssignment(new ShardId(13), slot, "exec-a0", RoutingEpoch.initial()));

        ShardLookupResult pending = coordinator.lookupOwner(new ShardId(13));
        assertThat(pending.status()).isEqualTo(RoutingStatus.PENDING_READY);
        assertThat(pending.assignment().executorId()).isEqualTo("exec-a0");

        coordinator.onExecutorReady("exec-a0");

        ShardLookupResult assigned = coordinator.lookupOwner(new ShardId(13));
        assertThat(assigned.status()).isEqualTo(RoutingStatus.ASSIGNED);
        assertThat(assigned.assignment().epoch()).isEqualTo(RoutingEpoch.initial());
    }
}
