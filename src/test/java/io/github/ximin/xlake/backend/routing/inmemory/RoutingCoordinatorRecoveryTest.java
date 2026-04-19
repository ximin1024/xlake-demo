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
package io.github.ximin.xlake.backend.routing.inmemory;

import io.github.ximin.xlake.backend.routing.*;
import io.github.ximin.xlake.common.config.XlakeConfig;
import io.github.ximin.xlake.common.config.XlakeOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingCoordinatorRecoveryTest {
    private InMemoryShardRoutingTable routingTable;
    private InMemoryRoutingCoordinator coordinator;

    @BeforeEach
    void setUp() {
        routingTable = new InMemoryShardRoutingTable();
        coordinator = new InMemoryRoutingCoordinator(routingTable);
    }

    private void registerAndReady(String executorId, NodeSlot slot) {
        coordinator.onExecutorUp(executorId, slot);
        coordinator.onExecutorReady(executorId);
    }

    @Test
    void onExecutorDownBuildsShardHistoryWithCorrectPreviousBasePathAndWalHdfsDir() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        coordinator.registerExecutorBasePath("exec-1", "/data/xlake/exec-1");
        coordinator.registerShardTables(new ShardId(0), Set.of("db.table_a", "db.table_b"));

        routingTable.assignShard(new ShardAssignment(new ShardId(0), slot, "exec-1", RoutingEpoch.initial()));

        coordinator.onExecutorDown("exec-1");

        List<ShardRecoveryRecord> history = coordinator.getShardHistory(new ShardId(0));
        assertThat(history).hasSize(1);
        ShardRecoveryRecord record = history.get(0);
        assertThat(record.shardId()).isEqualTo(new ShardId(0));
        assertThat(record.previousExecutorId()).isEqualTo("exec-1");
        assertThat(record.previousBasePath()).isEqualTo("/data/xlake/exec-1");
        assertThat(record.walHdfsDir()).isEmpty();
        assertThat(record.tableIdentifiers()).containsExactlyInAnyOrder("db.table_a", "db.table_b");
        assertThat(record.epoch()).isEqualTo(RoutingEpoch.initial());
        assertThat(record.lostTimestamp()).isGreaterThan(0);
    }

    @Test
    void onExecutorDownWithConfigBuildsShardHistoryWithWalHdfsDir() {
        XlakeConfig config = XlakeConfig.empty()
                .with(XlakeOptions.STORAGE_WAL_HDFS_PATH, "hdfs://namenode:8020/wal");
        routingTable = new InMemoryShardRoutingTable();
        coordinator = new InMemoryRoutingCoordinator(routingTable, config);

        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        routingTable.assignShard(new ShardAssignment(new ShardId(0), slot, "exec-1", RoutingEpoch.initial()));

        coordinator.onExecutorDown("exec-1");

        List<ShardRecoveryRecord> history = coordinator.getShardHistory(new ShardId(0));
        assertThat(history).hasSize(1);
        assertThat(history.get(0).walHdfsDir()).isEqualTo("hdfs://namenode:8020/wal/exec-1");
    }

    @Test
    void onRecoveryCompleteWithSuccessClearsRecoveringFlagAndRemovesShardHistory() {
        NodeSlot slotA = new NodeSlot("node-a", 0);
        NodeSlot slotB = new NodeSlot("node-b", 0);
        registerAndReady("exec-1", slotA);
        registerAndReady("exec-2", slotB);

        coordinator.registerExecutorBasePath("exec-1", "/data/exec-1");
        routingTable.assignShard(new ShardAssignment(new ShardId(0), slotA, "exec-1", RoutingEpoch.initial()));

        coordinator.onExecutorDown("exec-1");

        ShardLookupResult recovering = coordinator.lookupOwner(new ShardId(0));
        assertThat(recovering.status()).isEqualTo(RoutingStatus.RECOVERING);
        RoutingEpoch epoch = recovering.assignment().epoch();

        assertThat(coordinator.getShardHistory(new ShardId(0))).isNotEmpty();

        boolean result = coordinator.onRecoveryComplete(new ShardId(0), epoch, true);

        assertThat(result).isTrue();
        ShardLookupResult afterRecovery = coordinator.lookupOwner(new ShardId(0));
        assertThat(afterRecovery.status()).isEqualTo(RoutingStatus.ASSIGNED);
        assertThat(afterRecovery.assignment().recovering()).isFalse();
        assertThat(coordinator.getShardHistory(new ShardId(0))).isEmpty();
    }

    @Test
    void onRecoveryCompleteWithEpochMismatchDoesNotRemoveShardHistory() {
        NodeSlot slotA = new NodeSlot("node-a", 0);
        NodeSlot slotB = new NodeSlot("node-b", 0);
        registerAndReady("exec-1", slotA);
        registerAndReady("exec-2", slotB);

        coordinator.registerExecutorBasePath("exec-1", "/data/exec-1");
        routingTable.assignShard(new ShardAssignment(new ShardId(0), slotA, "exec-1", RoutingEpoch.initial()));

        coordinator.onExecutorDown("exec-1");

        RoutingEpoch mismatchedEpoch = new RoutingEpoch(999);

        boolean result = coordinator.onRecoveryComplete(new ShardId(0), mismatchedEpoch, true);

        assertThat(result).isFalse();
        assertThat(coordinator.getShardHistory(new ShardId(0))).isNotEmpty();
        ShardLookupResult stillRecovering = coordinator.lookupOwner(new ShardId(0));
        assertThat(stillRecovering.status()).isEqualTo(RoutingStatus.RECOVERING);
    }

    @Test
    void reassignShardForRecoveryDelegatesToRoutingTableWithRecoveringTrue() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        coordinator.reassignShardForRecovery(new ShardId(0));

        ShardLookupResult result = coordinator.lookupOwner(new ShardId(0));
        assertThat(result.status()).isEqualTo(RoutingStatus.RECOVERING);
        assertThat(result.assignment().recovering()).isTrue();
    }

    @Test
    void getRecoveringShardsReturnsOnlyRecoveringShards() {
        NodeSlot slotA = new NodeSlot("node-a", 0);
        NodeSlot slotB = new NodeSlot("node-b", 0);
        registerAndReady("exec-1", slotA);
        registerAndReady("exec-2", slotB);

        routingTable.assignShard(new ShardAssignment(new ShardId(0), slotA, "exec-1", RoutingEpoch.initial()));
        routingTable.assignShard(new ShardAssignment(new ShardId(1), slotB, "exec-2", RoutingEpoch.initial()));

        coordinator.reassignShardForRecovery(new ShardId(0));

        List<ShardLookupResult> recovering = coordinator.getRecoveringShards();
        assertThat(recovering).hasSize(1);
        assertThat(recovering.get(0).shardId()).isEqualTo(new ShardId(0));
        assertThat(recovering.get(0).status()).isEqualTo(RoutingStatus.RECOVERING);
    }

    @Test
    void registerExecutorBasePathStoresBasePathForLaterRecovery() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        coordinator.registerExecutorBasePath("exec-1", "/data/xlake/exec-1");
        routingTable.assignShard(new ShardAssignment(new ShardId(0), slot, "exec-1", RoutingEpoch.initial()));

        coordinator.onExecutorDown("exec-1");

        List<ShardRecoveryRecord> history = coordinator.getShardHistory(new ShardId(0));
        assertThat(history).hasSize(1);
        assertThat(history.get(0).previousBasePath()).isEqualTo("/data/xlake/exec-1");
    }

    @Test
    void onExecutorUpDoesNotReassignRecoveringShards() {
        NodeSlot slotA = new NodeSlot("node-a", 0);
        NodeSlot slotB = new NodeSlot("node-b", 0);
        registerAndReady("exec-1", slotA);
        registerAndReady("exec-2", slotB);

        routingTable.assignShard(new ShardAssignment(new ShardId(0), slotA, "exec-1", RoutingEpoch.initial()));

        coordinator.onExecutorDown("exec-1");

        ShardLookupResult recovering = coordinator.lookupOwner(new ShardId(0));
        assertThat(recovering.status()).isEqualTo(RoutingStatus.RECOVERING);
        RoutingEpoch recoveringEpoch = recovering.assignment().epoch();

        coordinator.onExecutorUp("exec-1-restarted", slotA);

        ShardLookupResult afterUp = coordinator.lookupOwner(new ShardId(0));
        assertThat(afterUp.status()).isEqualTo(RoutingStatus.RECOVERING);
        assertThat(afterUp.assignment().epoch()).isEqualTo(recoveringEpoch);
    }
}
