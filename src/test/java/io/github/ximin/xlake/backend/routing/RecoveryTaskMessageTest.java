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

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryTaskMessageTest {

    private ShardRecoveryRecord createRecord() {
        return new ShardRecoveryRecord(
                new ShardId(0),
                "exec-1",
                "node-a:0",
                "/data/xlake/exec-1",
                "hdfs://namenode:8020/wal/exec-1",
                Set.of("db.table_a"),
                new RoutingEpoch(1),
                System.currentTimeMillis()
        );
    }

    @Test
    void messageConstructionWithBasePathAndStoreId() {
        ShardRecoveryRecord record = createRecord();
        RecoveryTaskMessage message = new RecoveryTaskMessage(
                "exec-2",
                List.of(record),
                "/data/xlake/exec-2",
                "store-42"
        );

        assertThat(message.targetExecutorId()).isEqualTo("exec-2");
        assertThat(message.shards()).hasSize(1);
        assertThat(message.shards().get(0)).isEqualTo(record);
        assertThat(message.basePath()).isEqualTo("/data/xlake/exec-2");
        assertThat(message.storeId()).isEqualTo("store-42");
    }

    @Test
    void compactConstructorNullTargetExecutorIdThrows() {
        assertThatThrownBy(() -> new RecoveryTaskMessage(
                null, List.of(createRecord()), "/data", "store-1"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetExecutorId");
    }

    @Test
    void compactConstructorBlankTargetExecutorIdThrows() {
        assertThatThrownBy(() -> new RecoveryTaskMessage(
                "  ", List.of(createRecord()), "/data", "store-1"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetExecutorId");
    }

    @Test
    void compactConstructorNullShardsThrows() {
        assertThatThrownBy(() -> new RecoveryTaskMessage(
                "exec-1", null, "/data", "store-1"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shards");
    }

    @Test
    void compactConstructorEmptyShardsThrows() {
        assertThatThrownBy(() -> new RecoveryTaskMessage(
                "exec-1", List.of(), "/data", "store-1"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shards");
    }

    @Test
    void compactConstructorNullBasePathThrows() {
        assertThatThrownBy(() -> new RecoveryTaskMessage(
                "exec-1", List.of(createRecord()), null, "store-1"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("basePath");
    }

    @Test
    void compactConstructorBlankBasePathThrows() {
        assertThatThrownBy(() -> new RecoveryTaskMessage(
                "exec-1", List.of(createRecord()), "  ", "store-1"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("basePath");
    }

    @Test
    void compactConstructorNullStoreIdThrows() {
        assertThatThrownBy(() -> new RecoveryTaskMessage(
                "exec-1", List.of(createRecord()), "/data", null
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storeId");
    }

    @Test
    void compactConstructorBlankStoreIdThrows() {
        assertThatThrownBy(() -> new RecoveryTaskMessage(
                "exec-1", List.of(createRecord()), "/data", ""
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storeId");
    }

    @Test
    void compactConstructorDefensiveCopyShards() {
        ShardRecoveryRecord record = createRecord();
        List<ShardRecoveryRecord> mutableList = new java.util.ArrayList<>(List.of(record));
        RecoveryTaskMessage message = new RecoveryTaskMessage(
                "exec-2", mutableList, "/data", "store-1"
        );
        mutableList.add(createRecord());
        assertThat(message.shards()).hasSize(1);
    }
}
