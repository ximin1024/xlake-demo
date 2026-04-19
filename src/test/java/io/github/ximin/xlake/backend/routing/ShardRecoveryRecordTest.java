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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardRecoveryRecordTest {

    @Test
    void recordConstructionWithAllFields() {
        long timestamp = System.currentTimeMillis();
        ShardRecoveryRecord record = new ShardRecoveryRecord(
                new ShardId(5),
                "exec-1",
                "node-a:0",
                "/data/xlake/exec-1",
                "hdfs://namenode:8020/wal/exec-1",
                Set.of("db.table_a", "db.table_b"),
                new RoutingEpoch(3),
                timestamp
        );

        assertThat(record.shardId()).isEqualTo(new ShardId(5));
        assertThat(record.previousExecutorId()).isEqualTo("exec-1");
        assertThat(record.previousNodeSlotId()).isEqualTo("node-a:0");
        assertThat(record.previousBasePath()).isEqualTo("/data/xlake/exec-1");
        assertThat(record.walHdfsDir()).isEqualTo("hdfs://namenode:8020/wal/exec-1");
        assertThat(record.tableIdentifiers()).containsExactlyInAnyOrder("db.table_a", "db.table_b");
        assertThat(record.epoch()).isEqualTo(new RoutingEpoch(3));
        assertThat(record.lostTimestamp()).isEqualTo(timestamp);
    }

    @Test
    void compactConstructorNullShardIdThrows() {
        assertThatThrownBy(() -> new ShardRecoveryRecord(
                null, "exec-1", "node-a:0", "/data", "hdfs://wal", Set.of(), RoutingEpoch.initial(), 0L
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardId");
    }

    @Test
    void compactConstructorBlankPreviousExecutorIdThrows() {
        assertThatThrownBy(() -> new ShardRecoveryRecord(
                new ShardId(0), "", "node-a:0", "/data", "hdfs://wal", Set.of(), RoutingEpoch.initial(), 0L
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("previousExecutorId");
    }

    @Test
    void compactConstructorNullPreviousExecutorIdThrows() {
        assertThatThrownBy(() -> new ShardRecoveryRecord(
                new ShardId(0), null, "node-a:0", "/data", "hdfs://wal", Set.of(), RoutingEpoch.initial(), 0L
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("previousExecutorId");
    }

    @Test
    void compactConstructorBlankPreviousNodeSlotIdThrows() {
        assertThatThrownBy(() -> new ShardRecoveryRecord(
                new ShardId(0), "exec-1", "  ", "/data", "hdfs://wal", Set.of(), RoutingEpoch.initial(), 0L
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("previousNodeSlotId");
    }

    @Test
    void compactConstructorNullPreviousNodeSlotIdThrows() {
        assertThatThrownBy(() -> new ShardRecoveryRecord(
                new ShardId(0), "exec-1", null, "/data", "hdfs://wal", Set.of(), RoutingEpoch.initial(), 0L
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("previousNodeSlotId");
    }

    @Test
    void compactConstructorNullEpochThrows() {
        assertThatThrownBy(() -> new ShardRecoveryRecord(
                new ShardId(0), "exec-1", "node-a:0", "/data", "hdfs://wal", Set.of(), null, 0L
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("epoch");
    }

    @Test
    void compactConstructorNullTableIdentifiersDefaultsToEmpty() {
        ShardRecoveryRecord record = new ShardRecoveryRecord(
                new ShardId(0), "exec-1", "node-a:0", "/data", "hdfs://wal", null, RoutingEpoch.initial(), 0L
        );
        assertThat(record.tableIdentifiers()).isEmpty();
    }

    @Test
    void compactConstructorDefensiveCopyTableIdentifiers() {
        Set<String> tables = new java.util.HashSet<>(Set.of("db.table_a"));
        ShardRecoveryRecord record = new ShardRecoveryRecord(
                new ShardId(0), "exec-1", "node-a:0", "/data", "hdfs://wal", tables, RoutingEpoch.initial(), 0L
        );
        tables.add("db.table_b");
        assertThat(record.tableIdentifiers()).hasSize(1);
        assertThat(record.tableIdentifiers()).containsExactly("db.table_a");
    }
}
