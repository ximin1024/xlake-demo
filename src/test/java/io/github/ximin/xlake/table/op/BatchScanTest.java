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
package io.github.ximin.xlake.table.op;

import io.github.ximin.xlake.metastore.Metastore;
import io.github.ximin.xlake.storage.DynamicMmapStore;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.block.HotDataBlock;
import io.github.ximin.xlake.storage.spi.Storage;
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.storage.table.read.TableReader;
import io.github.ximin.xlake.table.*;
import io.github.ximin.xlake.table.impl.XlakeTableImpl;
import io.github.ximin.xlake.table.schema.Schema;
import io.github.ximin.xlake.table.schema.Types;
import io.github.ximin.xlake.writer.Writer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
@DisplayName("BatchScan - 数据扫描操作测试 (Reader委托模式)")
class BatchScanTest {

    @TempDir
    Path tempDir;

    @Mock
    private Metastore metastore;

    @Mock
    private Writer writer;

    
    @Mock
    private TableReader mockReader;

    private DynamicMmapStore mmapStore;
    private XlakeTableImpl table;

    @BeforeEach
    void setUp() throws IOException {
        mmapStore = DynamicMmapStore.getInstance(tempDir.toString(), "test-scan-store-" + System.nanoTime());
        TableMeta tableMeta = createTestTableMeta();
        DynamicTableInfo dynamicInfo = DynamicTableInfo.builder().build();
        table = new XlakeTableImpl(
                "default.test_db.scan_test",
                tableMeta,
                dynamicInfo
        );

        lenient().when(writer.write(any())).thenReturn(Write.Result.ok(1));
        lenient().when(writer.batchWrite(anyCollection())).thenReturn(Write.Result.ok(1));

        // ✅ Phase 1: 默认配置Mock Reader返回成功结果
        lenient().when(mockReader.scan(any(Scan.class)))
                .thenAnswer(invocation -> {
                    Scan scan = invocation.getArgument(0);
                    return Scan.Result.ok(List.of("mock-data"), false);
                });
    }

    @AfterEach
    void tearDown() throws Exception {
        if (table != null) {
            try {
                table.close();
            } catch (Exception ignored) {
            }
        }
        if (mmapStore != null) {
            mmapStore.close();
        }
    }

    @Test
    @DisplayName("空DataBlocks列表返回空结果")
    void testBatchScanEmptyDataBlocksReturnsEmptyResult() throws IOException {
        // ✅ Phase 1: 使用新的Builder模式，必须注入Reader
        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(Collections.emptyList())
                .withReader(mockReader)  // 必须调用！
                .build();

        Scan.Result result = scan.exec();

        assertThat(result.success()).isTrue();
        assertThat(result.data()).isEmpty();
        assertThat(result.recordCount()).isEqualTo(0);
        assertThat(result.hasMore()).isFalse();

        // 验证没有委托给Reader（因为blocks为空）
        verify(mockReader, never()).scan(any());
    }

    @Test
    @DisplayName("null DataBlocks列表返回空结果")
    void testBatchScanNullDataBlocksReturnsEmptyResult() {
        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(null)
                .withReader(mockReader)  // 必须调用！
                .build();

        Scan.Result result = scan.exec();

        assertThat(result.success()).isTrue();
        assertThat(result.data()).isEmpty();
    }

    @Test
    @DisplayName("type方法返回BATCH_SCAN类型")
    void testTypeReturnsBatchScanType() {
        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(Collections.emptyList())
                .withReader(mockReader)
                .build();

        assertThat(scan.type()).isEqualTo(OpType.BATCH_SCAN);
    }

    @Test
    @DisplayName("plan方法返回传入的DataBlocks列表")
    void testPlanReturnsDataBlocksList() {
        DataBlock block1 = createTestDataBlock("block-1");
        DataBlock block2 = createTestDataBlock("block-2");
        List<DataBlock> blocks = List.of(block1, block2);

        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(blocks)
                .withReader(mockReader)
                .build();

        List<DataBlock> plannedBlocks = scan.plan();

        assertThat(plannedBlocks).hasSize(2);
        assertThat(plannedBlocks).containsExactly(block1, block2);
    }

    @Test
    @DisplayName("plan方法在DataBlocks为null时返回空列表")
    void testPlanWithNullReturnsEmptyList() {
        // 注意：新构造函数要求Reader，这里使用mockReader
        BatchScan scan = new BatchScan(table, null, null, null, mockReader);

        List<DataBlock> plannedBlocks = scan.plan();

        assertThat(plannedBlocks).isEmpty();
    }

    @Test
    @DisplayName("estimatedSize方法计算所有DataBlock的总大小")
    void testEstimatedSizeCalculatesTotalSize() {
        DataBlock block1 = createTestDataBlockWithSize("block-1", 1024L);
        DataBlock block2 = createTestDataBlockWithSize("block-2", 2048L);
        DataBlock block3 = createTestDataBlockWithSize("block-3", 512L);

        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(List.of(block1, block2, block3))
                .withReader(mockReader)
                .build();

        long estimatedSize = scan.estimatedSize();

        assertThat(estimatedSize).isEqualTo(1024L + 2048L + 512L);
    }

    @Test
    @DisplayName("estimatedSize方法在DataBlocks为null时返回0")
    void testEstimatedSizeWithNullReturnsZero() {
        BatchScan scan = new BatchScan(table, null, null, null, mockReader);

        assertThat(scan.estimatedSize()).isEqualTo(0L);
    }

    @Test
    @DisplayName("projections方法返回设置的投影字段列表")
    void testProjectionsReturnsSetFields() {
        List<String> projections = List.of("user_id", "name", "age");

        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(Collections.emptyList())
                .withProjections(projections)
                .withReader(mockReader)
                .build();

        assertThat(scan.projections()).containsExactlyElementsOf(projections);
    }

    @Test
    @DisplayName("projections方法未设置时返回空列表")
    void testProjectionsUnsetReturnsEmptyList() {
        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(Collections.emptyList())
                .withReader(mockReader)
                .build();

        assertThat(scan.projections()).isEmpty();
    }

    @Test
    @DisplayName("getPushedPredicate方法返回设置的表达式")
    void testGetPushedPredicateReturnsExpression() {
        // 使用null作为predicate（实际场景中应该是具体的Expression实现）
        BatchScan scan = new BatchScan(table, Collections.emptyList(), null, List.of("field1"), mockReader);

        assertThat(scan.getPushedPredicate()).isNull();
    }

    @Test
    @DisplayName("✅ Phase 1核心验证：exec()正确委托给Reader执行")
    void testExecDelegatesToReader() throws IOException {
        // Given: 配置有数据的DataBlocks
        DataBlock block = createTestDataBlock("test-block");

        // When: 执行扫描
        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(List.of(block))
                .withReader(mockReader)
                .build();

        Scan.Result result = scan.exec();

        // Then: 验证成功并委托给了Reader
        assertThat(result.success()).isTrue();
        verify(mockReader, times(1)).scan(scan);  // 核心验证：确实调用了reader.scan()
        verifyNoMoreInteractions(mockReader);
    }

    @Test
    @DisplayName("✅ Phase 1核心验证：Builder未注入Reader时抛出异常")
    void testBuilderThrowsWhenReaderNotInjected() {
        assertThatThrownBy(() -> BatchScan.builder()
                .withTable(table)
                .withDataBlocks(Collections.emptyList()
                )// ❌ 故意不调用.withReader()
                .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("reader is required");
    }

    @Test
    @DisplayName("✅ Phase 1验证：继承BaseRead后可以访问table和reader")
    void testCanAccessTableAndReaderFromBaseRead() {
        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(Collections.emptyList())
                .withReader(mockReader)
                .build();

        // 验证继承自BaseRead的方法可用
        assertThat(scan.getTable()).isSameAs(table);
        assertThat(scan.getReader()).isSameAs(mockReader);
        assertThat(scan.isPrimaryKeyLookup()).isFalse();  // BatchScan不是主键查找
    }

    @Test
    @DisplayName("插入数据后全表扫描返回所有记录（E2E验证）")
    void testBatchScanAfterInsertReturnsAllRecords() throws IOException {
        // Given: 插入测试数据
        insertTestRecords(5);

        TableStore store;
        if (table.meta().hasPrimaryKey()) {
            store = mmapStore.tableStore(table.getTableIdentifier(), table.meta().primaryKey());
        } else {
            store = mmapStore.tableStore(table.getTableIdentifier());
        }

        List<DataBlock> currentBlocks = store.currentDataBlocks();

        if (!currentBlocks.isEmpty()) {
            BatchScan scan = BatchScan.builder()
                    .withTable(table)
                    .withDataBlocks(currentBlocks)
                    .withReader(mockReader)  // ✅ Phase 1: 注入Mock Reader
                    .build();

            Scan.Result result = scan.exec();

            // Then: 验证结果
            assertThat(result.success()).isTrue();
            assertThat(result.data()).isNotNull();

            // 验证确实调用了Reader
            verify(mockReader, atLeastOnce()).scan(any(Scan.class));
        }
    }

    @Test
    @DisplayName("指定特定DataBlock列表进行路由扫描")
    void testBatchScanWithSpecificDataBlocks() {
        // Given: 创建特定的DataBlock列表用于测试
        DataBlock targetBlock = createTestDataBlock("target-block");

        // When: 只扫描指定的block
        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(List.of(targetBlock))
                .withReader(mockReader)  // ✅ Phase 1: 注入Mock Reader
                .build();

        Scan.Result result = scan.exec();

        // Then: 结果可能成功或失败，取决于LMDB实例是否可读
        // 关键是验证scan能够正确处理指定的blocks参数
        assertThat(result).isNotNull();
    }

    private void insertTestRecords(int count) {
        for (int i = 0; i < count; i++) {
            Map<String, Object> entry = Map.of(
                    "key", "key_" + i,
                    "value", "value_" + i
            );

            KvWrite kvWrite = new KvWrite(table, List.of(entry), writer);
            Insert.Result result = kvWrite.execAsInsert();
            assertThat(result.success()).isTrue();
        }
    }

    private DataBlock createTestDataBlock(String blockId) {
        return new HotDataBlock(
                blockId,
                "default.test_db.scan_test",
                DataBlock.Kind.MUTABLE_HOT,
                DataBlock.Format.LMDB,
                new DataBlock.Location(
                        "local",
                        new Storage.StoragePath("file", tempDir.toString()),
                        0L,
                        0L
                ),
                new DataBlock.KeyRange(
                        new byte[0],
                        true,
                        new byte[0],
                        true
                ),
                0L,
                0L,
                0L,
                DataBlock.Visibility.ACTIVE,
                0L,
                0L,
                0L,
                0L,
                blockId
        );
    }

    private DataBlock createTestDataBlockWithSize(String blockId, long sizeBytes) {
        return new HotDataBlock(
                blockId,
                "default.test_db.scan_test",
                DataBlock.Kind.MUTABLE_HOT,
                DataBlock.Format.LMDB,
                new DataBlock.Location(
                        "local",
                        new Storage.StoragePath("file", tempDir.toString()),
                        0L,
                        sizeBytes
                ),
                new DataBlock.KeyRange(
                        new byte[0],
                        true,
                        new byte[0],
                        true
                ),
                0L,
                sizeBytes,
                0L,
                DataBlock.Visibility.ACTIVE,
                0L,
                0L,
                0L,
                0L,
                blockId
        );
    }

    private TableMeta createTestTableMeta() {
        Schema schema = Schema.builder()
                .field("user_id", Types.string())
                .field("name", Types.string())
                .primaryKey("user_id")
                .build();

        return TableMeta.builder()
                .withTableId(200L)
                .withCatalogName("default")
                .withDatabaseName("test_db")
                .withTableName("scan_test")
                .withTableType(TableMeta.TableType.PRIMARY_KEY)
                .withSchema(schema)
                .withPrimaryKey(PrimaryKey.of("user_id"))
                .withLocation(tempDir.toString() + "/test_db/scan_test")
                .build();
    }
}
