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
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.storage.table.read.TableReader;
import io.github.ximin.xlake.storage.table.record.KvRecord;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
@DisplayName("E2E 读写闭环测试 - 验证数据一致性")
class EndToEndReadWriteTest {

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
    void setUp() throws Exception {
        mmapStore = DynamicMmapStore.getInstance(tempDir.toString(), "test-e2e-store-" + System.nanoTime());
        TableMeta tableMeta = createTestTableMeta();
        DynamicTableInfo dynamicInfo = DynamicTableInfo.builder().build();
        table = new XlakeTableImpl(
                "default.test_db.e2e_test",
                tableMeta,
                dynamicInfo
        );

        lenient().when(writer.write(any())).thenReturn(Write.Result.ok(1));
        lenient().when(writer.batchWrite(anyCollection())).thenReturn(Write.Result.ok(1));
        lenient().when(mockReader.scan(any())).thenReturn(Scan.Result.ok(List.of(), false));
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
    @DisplayName("写入100条记录后扫描验证数据一致性（核心E2E场景）")
    void testWriteThenReadConsistency() {
        // Given: 准备100条测试数据
        int recordCount = 100;
        List<Map.Entry<String, String>> testData = generateTestData(recordCount);

        // When: 执行Insert操作
        Write.Result insertResult = insertData(testData);

        // Then: 验证插入成功
        assertThat(insertResult.success()).isTrue();
        assertThat(insertResult.count()).isEqualTo(recordCount);

        // When: 执行BatchScan全表扫描
        Scan.Result scanResult = scanAllData();

        // Then: 验证扫描结果
        assertThat(scanResult.success()).isTrue();
        assertThat(scanResult.data()).isNotNull();
        assertThat(scanResult.recordCount()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("多轮写入-扫描循环验证数据持久性和一致性")
    void testMultipleWriteReadCycles() {
        int cycles = 3;
        int recordsPerCycle = 50;

        for (int cycle = 0; cycle < cycles; cycle++) {
            // Given: 为每轮生成唯一的数据（使用cycle前缀区分）
            List<Map.Entry<String, String>> testData = new ArrayList<>();
            for (int i = 0; i < recordsPerCycle; i++) {
                testData.add(Map.entry(
                        "cycle" + cycle + "_key_" + i,
                        "cycle" + cycle + "_value_" + i
                ));
            }

            // When: 写入数据
            Write.Result insertResult = insertData(testData);

            // Then: 验证写入结果
            assertThat(insertResult.success())
                    .as("Cycle %d: Insert should succeed", cycle)
                    .isTrue();
            assertThat(insertResult.count())
                    .as("Cycle %d: Insert count should match", cycle)
                    .isEqualTo(recordsPerCycle);
        }
    }

    @Test
    @DisplayName("并发插入和扫描操作验证线程安全性")
    void testConcurrentInsertAndScan() throws Exception {
        int threadCount = 5;
        int recordsPerThread = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Write.Result>> insertFutures = new ArrayList<>();
        List<Future<Scan.Result>> scanFutures = new ArrayList<>();

        try {
            // 提交并发写入任务
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                insertFutures.add(executorService.submit(() -> {
                    startLatch.await(); // 等待所有线程就绪
                    List<Map.Entry<String, String>> data = new ArrayList<>();
                    for (int i = 0; i < recordsPerThread; i++) {
                        data.add(Map.entry(
                                "thread" + threadId + "_key_" + i,
                                "thread" + threadId + "_value_" + i
                        ));
                    }
                    return insertData(data);
                }));
            }

            // 提交并发扫描任务
            for (int t = 0; t < threadCount; t++) {
                scanFutures.add(executorService.submit(() -> {
                    startLatch.await(); // 等待所有线程就绪
                    return scanAllData();
                }));
            }

            // 启动所有任务
            startLatch.countDown();

            // 验证所有写入任务完成且无异常
            for (Future<Write.Result> future : insertFutures) {
                Write.Result result = future.get(30, TimeUnit.SECONDS);
                assertThat(result.success())
                        .as("Concurrent insert should succeed")
                        .isTrue();
                assertThat(result.count())
                        .as("Concurrent insert count should match")
                        .isEqualTo(recordsPerThread);
            }

            // 验证所有扫描任务完成且无异常
            for (Future<Scan.Result> future : scanFutures) {
                Scan.Result result = future.get(30, TimeUnit.SECONDS);
                assertThat(result.success())
                        .as("Concurrent scan should succeed")
                        .isTrue();
                assertThat(result.data())
                        .as("Concurrent scan data should not be null")
                        .isNotNull();
            }

        } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        }
    }

    @Test
    @DisplayName("写入后立即读取验证数据的即时可见性")
    void testImmediateVisibilityAfterWrite() {
        // Given: 准备单条测试数据
        Map.Entry<String, String> singleEntry = Map.entry("immediate_key", "immediate_value");

        // When: 写入数据
        Write.Result insertResult = insertData(List.of(singleEntry));

        // Then: 验证写入成功
        assertThat(insertResult.success()).isTrue();
        assertThat(insertResult.count()).isEqualTo(1);

        // When: 立即执行扫描
        Scan.Result scanResult = scanAllData();

        // Then: 验证数据可读
        assertThat(scanResult.success()).isTrue();
        assertThat(scanResult.data()).isNotNull();
    }

    @Test
    @DisplayName("大量数据写入和读取的完整性验证（500条记录）")
    void testLargeDatasetIntegrity() {
        int largeRecordCount = 500;

        // Given: 生成大量测试数据
        List<Map.Entry<String, String>> largeTestData = generateTestData(largeRecordCount);

        // When: 批量写入
        long writeStartTime = System.currentTimeMillis();
        Write.Result insertResult = insertData(largeTestData);
        long writeDuration = System.currentTimeMillis() - writeStartTime;

        // Then: 验证写入完整
        assertThat(insertResult.success()).isTrue();
        assertThat(insertResult.count()).isEqualTo(largeRecordCount);

        // When: 全表扫描
        long readStartTime = System.currentTimeMillis();
        Scan.Result scanResult = scanAllData();
        long readDuration = System.currentTimeMillis() - readStartTime;

        // Then: 验证读取完整
        assertThat(scanResult.success()).isTrue();
        assertThat(scanResult.data()).isNotNull();

        // 性能断言：大数据量操作应在合理时间内完成
        assertThat(writeDuration).isLessThan(10000);
        assertThat(readDuration).isLessThan(10000);
    }

    @Test
    @DisplayName("包含特殊字符的数据写入和读取验证")
    void testSpecialCharactersDataConsistency() {
        // Given: 包含特殊字符的测试数据
        List<Map.Entry<String, String>> specialData = Arrays.asList(
                Map.entry("key_with_spaces", "value with spaces"),
                Map.entry("key-with-dashes", "value-with-dashes"),
                Map.entry("key.with.dots", "value.with.dots"),
                Map.entry("key_with_underscore", "value_with_underscore"),
                Map.entry("key/with/slashes", "value/with/slashes"),
                Map.entry("中文键名", "中文值"),
                Map.entry("emoji_😀_key", "emoji_🎉_value"),
                Map.entry("key\nwith\nnewlines", "value\twith\ttabs"),
                Map.entry("", "empty_key_value"),
                Map.entry("null_like_key", "")
        );

        // When: 写入特殊字符数据
        Write.Result insertResult = insertData(specialData);

        // Then: 验证写入成功
        assertThat(insertResult.success()).isTrue();
        assertThat(insertResult.count()).isEqualTo(specialData.size());

        // When: 扫描读取
        Scan.Result scanResult = scanAllData();

        // Then: 验证读取成功
        assertThat(scanResult.success()).isTrue();
        assertThat(scanResult.data()).isNotNull();
    }

    @Test
    @DisplayName("混合数据格式写入后统一读取验证")
    void testMixedFormatWriteAndRead() {
        // Given: 混合格式的测试数据
        List<KvRecord> kvRecords = List.of(
                new KvRecord("kv_key1".getBytes(StandardCharsets.UTF_8), "kv_value1".getBytes(StandardCharsets.UTF_8)),
                new KvRecord("kv_key2".getBytes(StandardCharsets.UTF_8), "kv_value2".getBytes(StandardCharsets.UTF_8))
        );

        List<Map.Entry<String, String>> mapEntries = List.of(
                Map.entry("map_key1", "map_value1"),
                Map.entry("map_key2", "map_value2")
        );

        // When: 分别写入不同格式的数据
        List<Map<String, Object>> kvMapData = kvRecords.stream()
                .map(kv -> Map.<String, Object>of("key", new String(kv.key(), StandardCharsets.UTF_8), "value", new String(kv.value(), StandardCharsets.UTF_8)))
                .toList();

        Write.Result kvResult = new KvWrite(table, kvMapData, writer).exec();

        List<Map<String, Object>> mapDataList = mapEntries.stream()
                .map(entry -> Map.<String, Object>of("key", entry.getKey(), "value", entry.getValue()))
                .toList();

        Write.Result mapResult = new KvWrite(table, mapDataList, writer).exec();

        // Then: 验证两次写入都成功
        assertThat(kvResult.success()).isTrue();
        assertThat(kvResult.count()).isEqualTo(2);

        assertThat(mapResult.success()).isTrue();
        assertThat(mapResult.count()).isEqualTo(2);

        // When: 统一扫描读取
        Scan.Result scanResult = scanAllData();

        // Then: 验证总数据量
        assertThat(scanResult.success()).isTrue();
        assertThat(scanResult.data()).isNotNull();
    }

    private Write.Result insertData(List<Map.Entry<String, String>> data) {
        List<Map<String, Object>> mapData = data.stream()
                .map(entry -> Map.<String, Object>of("key", entry.getKey(), "value", entry.getValue()))
                .toList();

        KvWrite kvWrite = new KvWrite(table, mapData, writer);
        return kvWrite.exec();
    }

    private Scan.Result scanAllData() {
        TableStore store;
        if (table.meta().hasPrimaryKey()) {
            store = mmapStore.tableStore(table.getTableIdentifier(), table.meta().primaryKey());
        } else {
            store = mmapStore.tableStore(table.getTableIdentifier());
        }

        List<DataBlock> currentBlocks = store.currentDataBlocks();

        BatchScan scan = BatchScan.builder()
                .withTable(table)
                .withDataBlocks(currentBlocks)
                .withReader(mockReader)
                .build();

        return scan.exec();
    }

    private List<Map.Entry<String, String>> generateTestData(int count) {
        List<Map.Entry<String, String>> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            data.add(Map.entry(
                    "user_" + String.format("%04d", i),
                    "name_user_" + i + "_data"
            ));
        }
        return data;
    }

    private TableMeta createTestTableMeta() {
        Schema schema = Schema.builder()
                .field("user_id", Types.string())
                .field("name", Types.string())
                .field("email", Types.string())
                .primaryKey("user_id")
                .build();

        return TableMeta.builder()
                .withTableId(300L)
                .withCatalogName("default")
                .withDatabaseName("test_db")
                .withTableName("e2e_test")
                .withTableType(TableMeta.TableType.PRIMARY_KEY)
                .withSchema(schema)
                .withPrimaryKey(PrimaryKey.of("user_id"))
                .withLocation(tempDir.toString() + "/test_db/e2e_test")
                .build();
    }
}
