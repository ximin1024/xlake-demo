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
package io.github.ximin.xlake.table.impl;

import io.github.ximin.xlake.meta.PbSnapshot;
import io.github.ximin.xlake.metastore.Metastore;
import io.github.ximin.xlake.table.*;
import io.github.ximin.xlake.table.schema.Schema;
import io.github.ximin.xlake.table.schema.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("XlakeTableImpl - 可序列化表生命周期管理测试 (Phase 0)")
class XlakeTableImplTest {

    @TempDir
    Path tempDir;

    @Mock
    private Metastore metastore;

    private TableMeta tableMeta;
    private DynamicTableInfo dynamicInfo;
    private XlakeTableImpl table;

    @BeforeEach
    void setUp() {
        tableMeta = createTestTableMeta();
        dynamicInfo = DynamicTableInfo.builder().build();  // 初始为空
        // ✅ 使用新的3参数构造函数（Phase 0可序列化版本）
        table = new XlakeTableImpl(
                "default.test_db.test_users",
                tableMeta,
                dynamicInfo
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (table != null) {
            try {
                table.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("成功创建可序列化的表实例并验证元数据")
    void testCreateTableSuccessfully() {
        assertThat(table).isNotNull();
        assertThat(table.meta()).isNotNull().isEqualTo(tableMeta);
        assertThat(table.meta().tableName()).isEqualTo("test_users");
        assertThat(table.name()).isEqualTo("default.test_db.test_users");
        assertThat(table.uniqId()).contains("default").contains("test_db").contains("test_users");
        assertThat(table.schema()).isNotNull();
        assertThat(table.tableId()).isEqualTo(1L);
        assertThat(table.isKVTable()).isTrue();
        assertThat(table.isAppendOnlyTable()).isFalse();
        assertThat(table.primaryKey()).isNotNull();
        assertThat(table.primaryKey().fields()).containsExactly("user_id");

        // ✅ 验证表是可序列化的（Phase 0核心特性）
        assertThat(table).isInstanceOf(java.io.Serializable.class);
    }

    @Test
    @DisplayName("currentSnapshot返回有效的快照信息或null（空表场景）")
    void testCurrentSnapshotReturnsValidSnapshot() {
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot != null) {
            assertThat(snapshot.snapshotId()).isGreaterThanOrEqualTo(0);
            assertThat(snapshot.operation()).isNotNull();
        }
    }

    @Test
    @DisplayName("dynamicInfo返回非空的动态表信息对象")
    void testDynamicInfoReturnsNonNull() {
        DynamicTableInfo info = table.dynamicInfo();
        assertThat(info).isNotNull();
        assertThat(info.snapshots()).isNotNull();
    }

    @Test
    @DisplayName("refresh()方法在Phase 0中抛出异常（已废弃，推荐使用Refresh Op）")
    void testDirectRefreshThrowsException() {
        // Phase 0架构改进：直接调用refresh()不再支持，推荐使用Refresh Op
        assertThatThrownBy(() -> table.refresh())
                .isInstanceOf(io.github.ximin.xlake.common.exception.CatalogException.class)
                .hasMessageContaining("deprecated")
                .hasMessageContaining("Refresh op");
    }

    @Test
    @DisplayName("close释放资源且幂等（多次调用不报错）")
    void testCloseReleasesResources() throws Exception {
        table.close();
        // 幂等性：多次close不报错
        assertThatCode(() -> table.close())
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("snapshots方法返回快照列表（可能为空）")
    void testSnapshotsReturnsList() {
        List<Snapshot> snapshots = table.snapshots();
        assertThat(snapshots).isNotNull();
    }

    @Test
    @DisplayName("snapshot根据ID查找特定快照")
    void testSnapshotById() {
        Snapshot snapshot = table.snapshot(-1L);
        assertThat(snapshot).isNull();
    }

    @Test
    @DisplayName("toString包含关键信息且标记为可序列化")
    void testToStringContainsKeyInfo() {
        String str = table.toString();
        assertThat(str).contains("XlakeTableImpl");
        assertThat(str).contains("default.test_db.test_users");
        assertThat(str).contains("test_users");
        assertThat(str).contains("serializable=true");  // Phase 0新特性
    }

    @Test
    @DisplayName("新构造函数参数为null时抛出NullPointerException")
    void testConstructorNullChecks() {
        // ✅ 新的3参数构造函数的null检查
        assertThatNullPointerException()
                .isThrownBy(() -> new XlakeTableImpl(null, tableMeta, dynamicInfo));

        assertThatNullPointerException()
                .isThrownBy(() -> new XlakeTableImpl("id", null, dynamicInfo));

        // dynamicInfo可以为null（会使用默认值）
        assertThatCode(() -> new XlakeTableImpl("id", tableMeta, null))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("updateDynamicInfo正确更新动态信息")
    void testUpdateDynamicInfo() {
        // Given: 初始状态为空
        assertThat(table.dynamicInfo().snapshots()).isEmpty();

        // When: 更新动态信息
        PbSnapshot pbSnapshot = PbSnapshot.newBuilder()
                .setSnapshotId(200L)
                .setTimestamp(System.currentTimeMillis())
                .setOperation("update-test")
                .build();

        Snapshot snapshot = ProtoTableMetaConverter.fromPbSnapshot(pbSnapshot);
        DynamicTableInfo newInfo = DynamicTableInfo.builder()
                .withSnapshots(List.of(snapshot))
                .build();

        table.updateDynamicInfo(newInfo);

        // Then: 验证已更新
        assertThat(table.dynamicInfo().snapshots()).hasSize(1);
        assertThat(table.dynamicInfo().snapshots().get(0).snapshotId()).isEqualTo(200L);
    }

    private TableMeta createTestTableMeta() {
        Schema schema = Schema.builder()
                .field("user_id", Types.string())
                .field("name", Types.string())
                .field("age", Types.int32())
                .primaryKey("user_id")
                .build();

        return TableMeta.builder()
                .withTableId(1L)
                .withCatalogName("default")
                .withDatabaseName("test_db")
                .withTableName("test_users")
                .withTableType(TableMeta.TableType.PRIMARY_KEY)
                .withSchema(schema)
                .withPrimaryKey(PrimaryKey.of("user_id"))
                .withLocation(tempDir.toString() + "/test_db/test_users")
                .build();
    }
}
