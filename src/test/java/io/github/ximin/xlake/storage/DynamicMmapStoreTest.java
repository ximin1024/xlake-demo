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
package io.github.ximin.xlake.storage;

import io.github.ximin.xlake.storage.lmdb.LmdbInstance;
import io.github.ximin.xlake.table.DynamicTableInfo;
import io.github.ximin.xlake.table.Snapshot;
import io.github.ximin.xlake.table.TableMeta;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.table.op.KvWriteBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DynamicMmapStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void writeCreatesTableScopedInstanceAndLease() throws Exception {
        String tableIdentifier = "catalog.db.tbl";
        DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        try {
            KvWriteBuilder builder = KvWriteBuilder.builder()
                    .table(new TestTable(tableIdentifier))
                    .key("k1".getBytes(StandardCharsets.UTF_8))
                    .value("v1".getBytes(StandardCharsets.UTF_8))
                    .partitionHint(0);

            DynamicMmapStore.RoutingOwnerContext owner =
                    new DynamicMmapStore.RoutingOwnerContext("exec-1", 0);
            store.write(builder, owner);

            Path tablePath = tempDir.resolve(tableIdentifier);
            assertThat(Files.exists(tablePath)).isTrue();

            List<Path> children = Files.list(tablePath).toList();
            assertThat(children).isNotEmpty();
            assertThat(children.stream()
                    .filter(Files::isDirectory)
                    .filter(path -> path.getFileName().toString().startsWith("inst-"))
                    .count())
                    .isEqualTo(1);
            assertThat(children.stream().filter(path -> path.getFileName().toString().endsWith(".lease")).count())
                    .isEqualTo(1);
        } finally {
            store.close();
        }
    }

    @Test
    void writeWithoutOwnerContextFailsExplicitly() throws Exception {
        String tableIdentifier = "catalog.db.tbl.noowner";
        DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        try {
            KvWriteBuilder builder = KvWriteBuilder.builder()
                    .table(new TestTable(tableIdentifier))
                    .key("k1".getBytes(StandardCharsets.UTF_8))
                    .value("v1".getBytes(StandardCharsets.UTF_8))
                    .partitionHint(0);

            assertThatThrownBy(() -> store.write(builder))
                    .isInstanceOf(DynamicMmapStore.LeaseUnavailableException.class)
                    .hasMessageContaining("owner lease unavailable");
        } finally {
            store.close();
        }
    }

    @Test
    void ownerEpochSwitchRotatesToNewWritableInstanceInsteadOfWaitingForOldLeaseTtl() throws Exception {
        String tableIdentifier = "catalog.db.tbl.epoch";
        DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        try {
            KvWriteBuilder firstWrite = KvWriteBuilder.builder()
                    .table(new TestTable(tableIdentifier))
                    .key("k1".getBytes(StandardCharsets.UTF_8))
                    .value("v1".getBytes(StandardCharsets.UTF_8))
                    .partitionHint(0);
            store.write(firstWrite, new DynamicMmapStore.RoutingOwnerContext("exec-owner", 1));

            KvWriteBuilder secondWrite = KvWriteBuilder.builder()
                    .table(new TestTable(tableIdentifier))
                    .key("k2".getBytes(StandardCharsets.UTF_8))
                    .value("v2".getBytes(StandardCharsets.UTF_8))
                    .partitionHint(0);
            store.write(secondWrite, new DynamicMmapStore.RoutingOwnerContext("exec-owner", 2));

            List<LmdbInstance> allInstances = store.tableStore(tableIdentifier).getAllInstancesDescending();
            assertThat(allInstances).hasSize(2);
            assertThat(allInstances.stream()
                    .map(LmdbInstance::currentLease)
                    .filter(java.util.Objects::nonNull)
                    .anyMatch(lease -> lease.state() == LmdbInstance.LeaseState.READ_ONLY))
                    .isTrue();
            assertThat(allInstances.stream()
                    .map(LmdbInstance::currentLease)
                    .filter(java.util.Objects::nonNull)
                    .anyMatch(lease -> lease.state() == LmdbInstance.LeaseState.ACTIVE
                            && lease.ownerId().equals(LmdbInstance.routingOwnerId("exec-owner", 2))))
                    .isTrue();
        } finally {
            store.close();
        }
    }

    private record TestTable(String uniqId) implements XlakeTable {
        @Override
        public void close() {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void refresh() {
        }

        @Override
        public TableMeta meta() {
            return null;
        }

        @Override
        public DynamicTableInfo dynamicInfo() {
            return null;
        }

        @Override
        public Snapshot currentSnapshot() {
            return null;
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            return null;
        }

        @Override
        public List<Snapshot> snapshots() {
            return List.of();
        }
    }
}
