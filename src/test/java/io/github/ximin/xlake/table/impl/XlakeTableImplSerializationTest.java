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

import io.github.ximin.xlake.common.exception.CatalogException;
import io.github.ximin.xlake.table.*;
import io.github.ximin.xlake.table.schema.Schema;
import io.github.ximin.xlake.table.schema.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.*;


@ExtendWith(MockitoExtension.class)
@DisplayName("XlakeTableImpl - 序列化/反序列化往返测试 (Blocker-3)")
class XlakeTableImplSerializationTest {

    @org.junit.jupiter.api.io.TempDir
    Path tempDir;

    private TableMeta tableMeta;
    private DynamicTableInfo dynamicInfo;
    private XlakeTableImpl originalTable;

    @BeforeEach
    void setUp() {
        // 创建复杂的测试数据（包含所有字段类型）
        tableMeta = createComplexTableMeta();
        dynamicInfo = createDynamicInfoWithSnapshots();

        // 使用3参数构造函数创建可序列化的Table实例
        originalTable = new XlakeTableImpl(
                "default.test_db.serialization_test",
                tableMeta,
                dynamicInfo
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (originalTable != null) {
            try {
                originalTable.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("基本序列化和反序列化往返测试")
    void testBasicSerializationRoundTrip() throws Exception {
        // When: 序列化为字节流
        byte[] serialized = serialize(originalTable);

        // Then: 反序列化
        XlakeTableImpl deserialized = deserialize(serialized);

        // 验证：基本字段一致性
        assertThat(deserialized).isNotNull();
        assertThat(deserialized.getTableIdentifier())
                .isEqualTo(originalTable.getTableIdentifier());
        assertThat(deserialized.meta()).isNotNull();
        assertThat(deserialized.dynamicInfo()).isNotNull();
    }

    @Test
    @DisplayName("序列化后TableMeta完全一致")
    void testTableMetaConsistencyAfterDeserialization() throws Exception {
        // Given & When
        XlakeTableImpl deserialized = deserialize(serialize(originalTable));

        // Then: 验证TableMeta的所有关键字段
        TableMeta deserializedMeta = deserialized.meta();
        assertThat(deserializedMeta.tableId()).isEqualTo(tableMeta.tableId());
        assertThat(deserializedMeta.catalogName()).isEqualTo(tableMeta.catalogName());
        assertThat(deserializedMeta.databaseName()).isEqualTo(tableMeta.databaseName());
        assertThat(deserializedMeta.tableName()).isEqualTo(tableMeta.tableName());
        assertThat(deserializedMeta.tableType()).isEqualTo(tableMeta.tableType());
        assertThat(deserializedMeta.location()).isEqualTo(tableMeta.location());

        // 验证Schema一致性
        assertThat(deserializedMeta.schema()).isEqualTo(tableMeta.schema());
        assertThat(deserializedMeta.schema().fields()).hasSize(tableMeta.schema().fields().size());

        // 验证主键信息
        if (tableMeta.hasPrimaryKey()) {
            assertThat(deserializedMeta.hasPrimaryKey()).isTrue();
            assertThat(deserializedMeta.primaryKey().fields())
                    .isEqualTo(tableMeta.primaryKey().fields());
        }
    }

    @Test
    @DisplayName("序列化后DynamicTableInfo完全一致")
    void testDynamicTableInfoConsistencyAfterDeserialization() throws Exception {
        // Given & When
        XlakeTableImpl deserialized = deserialize(serialize(originalTable));

        // Then: 验证DynamicTableInfo
        DynamicTableInfo deserializedInfo = deserialized.dynamicInfo();
        assertThat(deserializedInfo).isNotNull();
        assertThat(deserializedInfo.snapshots()).hasSize(dynamicInfo.snapshots().size());

        // 如果有快照，验证快照列表
        if (!dynamicInfo.snapshots().isEmpty()) {
            for (int i = 0; i < dynamicInfo.snapshots().size(); i++) {
                Snapshot originalSnapshot = dynamicInfo.snapshots().get(i);
                Snapshot deserializedSnapshot = deserializedInfo.snapshots().get(i);

                assertThat(deserializedSnapshot.snapshotId())
                        .isEqualTo(originalSnapshot.snapshotId());
                assertThat(deserializedSnapshot.operation())
                        .isEqualTo(originalSnapshot.operation());
                assertThat(deserializedSnapshot.timestampMillis())
                        .isEqualTo(originalSnapshot.timestampMillis());
            }
        }
    }

    @Test
    @DisplayName("反序列化后transient字段已正确重建（不会NPE）")
    void testTransientFieldsReconstructedAfterDeserialization() throws Exception {
        // Given & When: 反序列化
        XlakeTableImpl deserialized = deserialize(serialize(originalTable));

        // Then: 验证transient字段已重建（通过行为测试）
        // 1. closed标志位应该是false（新JVM中的表应该是打开状态）
        // 通过调用方法来验证不会抛出IllegalStateException
        assertThatCode(() -> {
            // 调用需要检查closed状态的方法
            deserialized.meta();  // 这个方法会调用ensureOpen()
        }).doesNotThrowAnyException();

        // 2. lock已重建，可以正常调用需要锁的方法
        assertThatCode(() -> {
            deserialized.currentSnapshot();  // 需要读锁
            deserialized.snapshots();         // 需要读锁
        }).doesNotThrowAnyException();

        // 3. toString应该包含正确的信息（使用lock和closed字段）
        String str = deserialized.toString();
        assertThat(str).contains("XlakeTableImpl");
        assertThat(str).contains("serializable=true");
    }

    @Test
    @DisplayName("反序列化后的表可以正常调用refresh()方法（虽然会抛出UnsupportedOperationException）")
    void testRefreshMethodWorksAfterDeserialization() throws Exception {
        // Given & When
        XlakeTableImpl deserialized = deserialize(serialize(originalTable));

        // Then: refresh()方法可以调用（transient字段已重建）
        // 注意：refresh()会抛出CatalogException（因为这是预期行为）
        assertThatThrownBy(() -> deserialized.refresh())
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("deprecated")
                .hasMessageContaining("Refresh op");
    }

    @Test
    @DisplayName("反序列化后的表可以正常调用close()方法")
    void testCloseMethodWorksAfterDeserialization() throws Exception {
        // Given & When: 反序列化
        XlakeTableImpl deserialized = deserialize(serialize(originalTable));

        // Then: close()应该成功执行（不抛异常）
        assertThatCode(() -> deserialized.close()).doesNotThrowAnyException();

        // close后可以再次close（幂等性）
        assertThatCode(() -> deserialized.close()).doesNotThrowAnyException();

        // 验证closed状态已经设置（通过toString检查）
        String str = deserialized.toString();
        assertThat(str).contains("closed=true");
    }

    @Test
    @DisplayName("多次序列化/反序列化结果一致")
    void testMultipleRoundTripsProduceSameResult() throws Exception {
        // 第一次往返
        byte[] firstSerialized = serialize(originalTable);
        XlakeTableImpl firstDeserialized = deserialize(firstSerialized);

        // 第二次往返
        byte[] secondSerialized = serialize(firstDeserialized);
        XlakeTableImpl secondDeserialized = deserialize(secondSerialized);

        // 第三次往返
        byte[] thirdSerialized = serialize(secondDeserialized);
        XlakeTableImpl thirdDeserialized = deserialize(thirdSerialized);

        // 所有结果的tableIdentifier应该相同
        assertThat(firstDeserialized.getTableIdentifier())
                .isEqualTo(secondDeserialized.getTableIdentifier())
                .isEqualTo(thirdDeserialized.getTableIdentifier())
                .isEqualTo(originalTable.getTableIdentifier());

        // 所有结果的meta应该相等
        assertThat(firstDeserialized.meta())
                .isEqualTo(secondDeserialized.meta())
                .isEqualTo(thirdDeserialized.meta())
                .isEqualTo(originalTable.meta());
    }

    @Test
    @DisplayName("空DynamicTableInfo也可以正确序列化")
    void testEmptyDynamicInfoSerialization() throws Exception {
        // Given: 创建空的DynamicTableInfo
        DynamicTableInfo emptyInfo = DynamicTableInfo.builder().build();
        XlakeTableImpl tableWithEmptyInfo = new XlakeTableImpl(
                "default.db.empty_table",
                tableMeta,
                emptyInfo
        );

        // When: 序列化和反序列化
        XlakeTableImpl deserialized = deserialize(serialize(tableWithEmptyInfo));

        // Then: 验证空的dynamicInfo被正确保留
        assertThat(deserialized.dynamicInfo()).isNotNull();
        assertThat(deserialized.dynamicInfo().snapshots()).isEmpty();
        assertThat(deserialized.currentSnapshot()).isNull();
    }

    @Test
    @DisplayName("复杂Schema（多字段+主键）可以正确序列化")
    void testComplexSchemaSerialization() throws Exception {
        // Given: 创建包含多种类型的复杂Schema
        Schema complexSchema = Schema.builder()
                .field("id", Types.int64())
                .field("name", Types.string())
                .field("age", Types.int32())
                .field("score", Types.float64())
                .field("active", Types.bool())
                .field("created_at", Types.timestamp())
                .primaryKey("id")
                .build();

        TableMeta complexMeta = TableMeta.builder()
                .withTableId(999L)
                .withCatalogName("default")
                .withDatabaseName("test_db")
                .withTableName("complex_schema_table")
                .withTableType(TableMeta.TableType.PRIMARY_KEY)
                .withSchema(complexSchema)
                .withPrimaryKey(PrimaryKey.of("id"))
                .withLocation(tempDir.toString() + "/complex")
                .build();

        XlakeTableImpl complexTable = new XlakeTableImpl(
                "default.test_db.complex_schema_table",
                complexMeta,
                DynamicTableInfo.builder().build()
        );

        // When: 序列化和反序列化
        XlakeTableImpl deserialized = deserialize(serialize(complexTable));

        // Then: 验证复杂Schema完整保留
        Schema deserializedSchema = deserialized.meta().schema();
        assertThat(deserializedSchema.fields()).hasSize(6);
        assertThat(deserializedSchema.fields().get(0).name()).isEqualTo("id");
        assertThat(deserializedSchema.fields().get(1).name()).isEqualTo("name");
        assertThat(deserializedSchema.primaryKeys()).containsExactly("id");
    }

    @Test
    @DisplayName("序列化字节数组非空且大小合理")
    void testSerializedBytesNonEmptyAndReasonableSize() throws Exception {
        // When: 序列化
        byte[] serialized = serialize(originalTable);

        // Then: 验证序列化结果
        assertThat(serialized).isNotEmpty();
        assertThat(serialized.length).isGreaterThan(0);

        // 序列化后的数据应该有一定的合理大小（至少几百字节，因为包含完整的元数据）
        // 这里不做严格的大小限制，但确保不是空数组或极小的数据
        System.out.println("[DEBUG] Serialized size: " + serialized.length + " bytes");
    }

    // ==================== 辅助方法 ====================

    
    private byte[] serialize(XlakeTableImpl obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
        }
        return bos.toByteArray();
    }

    
    private XlakeTableImpl deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(data))) {
            return (XlakeTableImpl) ois.readObject();
        }
    }

    
    private TableMeta createComplexTableMeta() {
        Schema schema = Schema.builder()
                .field("user_id", Types.string())
                .field("name", Types.string())
                .field("email", Types.string())
                .field("age", Types.int32())
                .field("score", Types.float64())
                .primaryKey("user_id")
                .build();

        return TableMeta.builder()
                .withTableId(42L)
                .withCatalogName("default")
                .withDatabaseName("test_db")
                .withTableName("serialization_test")
                .withTableType(TableMeta.TableType.PRIMARY_KEY)
                .withSchema(schema)
                .withPrimaryKey(PrimaryKey.of("user_id"))
                .withLocation(tempDir.toString() + "/test_db/serialization_test")
                .withCreatedAt(Instant.now().minusSeconds(3600))
                .withUpdatedAt(Instant.now())
                .build();
    }

    
    private DynamicTableInfo createDynamicInfoWithSnapshots() {
        Snapshot snapshot1 = Snapshot.of(100L, "append");
        Snapshot snapshot2 = Snapshot.of(101L, "overwrite");
        Snapshot snapshot3 = Snapshot.of(102L, "merge");

        return DynamicTableInfo.builder()
                .withSnapshots(List.of(snapshot1, snapshot2, snapshot3))
                .withCurrentSequenceNumber(102L)
                .withLastCommitTimestamp(System.currentTimeMillis())
                .build();
    }
}
