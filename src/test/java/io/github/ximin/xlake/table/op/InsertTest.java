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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("KvWrite - 数据写入操作测试（基于Insert接口）")
class InsertTest {

    @TempDir
    Path tempDir;

    @Mock
    private Writer writer;

    private XlakeTableImpl table;

    @BeforeEach
    void setUp() throws Exception {
        TableMeta tableMeta = createTestTableMeta();
        DynamicTableInfo dynamicInfo = DynamicTableInfo.builder().build();
        table = new XlakeTableImpl(
                "default.test_db.insert_test",
                tableMeta,
                dynamicInfo
        );

        lenient().when(writer.write(any(KvRecord.class))).thenReturn(Write.Result.ok(1));
        lenient().when(writer.batchWrite(anyCollection())).thenReturn(Write.Result.ok(3));
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
    @DisplayName("成功插入Map格式数据并返回正确计数")
    void testInsertMapDataSuccessfully() {
        List<Map<String, Object>> data = List.of(
                Map.of("key", "key1", "value", "value1"),
                Map.of("key", "key2", "value", "value2"),
                Map.of("key", "key3", "value", "value3")
        );

        KvWrite insert = new KvWrite(table, data, writer);

        Insert.Result result = insert.execAsInsert();

        assertThat(result.success()).isTrue();
        assertThat(result.affectedRows()).isEqualTo(3);
        verify(writer, times(1)).batchWrite(anyCollection());
    }

    @Test
    @DisplayName("插入空数据集返回Result(success=true, affectedRows=0)")
    void testInsertEmptyDataReturnsOkWithZeroCount() {
        KvWrite insert = new KvWrite(table, Collections.emptyList(), writer);

        Insert.Result result = insert.execAsInsert();

        assertThat(result.success()).isTrue();
        assertThat(result.affectedRows()).isEqualTo(0);
    }

    @Test
    @DisplayName("插入null数据返回Result(success=true, affectedRows=0)")
    void testInsertNullDataReturnsOkWithZeroCount() {
        KvWrite insert = new KvWrite(table, null, writer);

        Insert.Result result = insert.execAsInsert();

        assertThat(result.success()).isTrue();
        assertThat(result.affectedRows()).isEqualTo(0);
    }

    @Test
    @DisplayName("writeSize方法返回数据列表大小")
    void testWriteSizeReturnsCorrectSize() {
        List<Map<String, Object>> data = List.of(
                Map.of("key", "k1", "value", "v1"),
                Map.of("key", "k2", "value", "v2"),
                Map.of("key", "k3", "value", "v3")
        );

        KvWrite insert = new KvWrite(table, data, writer);

        assertThat(insert.writeSize()).isEqualTo(3);
    }

    @Test
    @DisplayName("writeSize在data为null时返回0")
    void testWriteSizeWithNullDataReturnsZero() {
        KvWrite insert = new KvWrite(table, null, writer);

        assertThat(insert.writeSize()).isEqualTo(0);
    }

    @Test
    @DisplayName("type方法返回OpType.KV_WRITE")
    void testTypeReturnsKvWriteType() {
        KvWrite insert = new KvWrite(table, Collections.emptyList(), writer);

        assertThat(insert.type()).isEqualTo(OpType.KV_WRITE);
    }

    @Test
    @DisplayName("Writer写入失败时返回成功但affectedRows为0")
    void testWriterFailureReturnsErrorResult() {
        when(writer.write(any(KvRecord.class))).thenReturn(Write.Result.error("Write failed"));
        when(writer.batchWrite(anyCollection())).thenReturn(Write.Result.error("Batch write failed"));

        List<Map<String, Object>> data = List.of(
                Map.of("key", "fail_key", "value", "fail_value")
        );

        KvWrite insert = new KvWrite(table, data, writer);

        Insert.Result insertResult = insert.execAsInsert();

        assertThat(insertResult.success()).isTrue();
        assertThat(insertResult.affectedRows()).isEqualTo(0);
    }

    @Test
    @DisplayName("execAsInsert方法返回Insert.Result类型")
    void testExecReturnsInsertResult() {
        when(writer.batchWrite(anyCollection())).thenReturn(Write.Result.ok(1));

        List<Map<String, Object>> data = List.of(
                Map.of("key", "test_key", "value", "test_value")
        );

        KvWrite insert = new KvWrite(table, data, writer);

        Insert.Result insertResult = insert.execAsInsert();

        assertThat(insertResult.success()).isTrue();
        assertThat(insertResult.affectedRows()).isEqualTo(1);
    }

    @Test
    @DisplayName("批量插入性能测试（100条记录）")
    void testBulkInsertPerformance() {
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(Map.of(
                    "key", "bulk_key_" + i,
                    "value", "bulk_value_" + i
            ));
        }

        lenient().when(writer.batchWrite(anyCollection())).thenReturn(Write.Result.ok(100));

        KvWrite insert = new KvWrite(table, data, writer);

        long startTime = System.currentTimeMillis();
        Insert.Result result = insert.execAsInsert();
        long duration = System.currentTimeMillis() - startTime;

        assertThat(result.success()).isTrue();
        assertThat(result.affectedRows()).isEqualTo(100);
        assertThat(duration).isLessThan(5000);
        verify(writer, times(1)).batchWrite(anyCollection());
    }

    @Test
    @DisplayName("使用partitionHint构造函数")
    void testConstructorWithPartitionHint() {
        List<Map<String, Object>> data = List.of(
                Map.of("key", "partitioned_key", "value", "partitioned_value")
        );

        KvWrite insert = new KvWrite(table, data, 1, writer);

        assertThat(insert.getPartitionHint()).isEqualTo(1);
        assertThat(insert.writeSize()).isEqualTo(1);
    }

    private TableMeta createTestTableMeta() {
        Schema schema = Schema.builder()
                .field("user_id", Types.string())
                .field("name", Types.string())
                .primaryKey("user_id")
                .build();

        return TableMeta.builder()
                .withTableId(100L)
                .withCatalogName("default")
                .withDatabaseName("test_db")
                .withTableName("insert_test")
                .withTableType(TableMeta.TableType.PRIMARY_KEY)
                .withSchema(schema)
                .withPrimaryKey(PrimaryKey.of("user_id"))
                .withLocation(tempDir.toString() + "/test_db/insert_test")
                .build();
    }
}
