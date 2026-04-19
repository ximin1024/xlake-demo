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
package io.github.ximin.xlake.backend.wal;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("WALIntegrationTest - WAL集成测试")
class WALIntegrationTest {

    @TempDir
    Path tempDir;

    private Configuration hadoopConfig;

    private DefaultWALRecord createDataRecord(long sequence, byte[] data, String tableId) {
        return new DefaultWALRecord(sequence, data, System.nanoTime(), 1L, tableId, "DATA");
    }

    private DefaultWALRecord createSyncMarker(long sequence, String tableId) {
        return new DefaultWALRecord(sequence, new byte[0], System.nanoTime(), -1L, tableId, "SYNC_MARKER");
    }

    @Nested
    @DisplayName("完整写入-恢复循环测试")
    class FullWriteRecoverCycleTest {

        @Test
        @DisplayName("写入记录后通过WALReader读回，数据完整性验证")
        void testWriteAndRecoverDataIntegrity() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            byte[] data1 = "key1:value1".getBytes();
            byte[] data2 = "key2:value2".getBytes();
            byte[] data3 = "key3:value3".getBytes();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, data1, "table1"));
            writer.write(createDataRecord(2, data2, "table1"));
            writer.write(createDataRecord(3, data3, "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(3);
            assertThat(records.get(0).value()).isEqualTo(data1);
            assertThat(records.get(1).value()).isEqualTo(data2);
            assertThat(records.get(2).value()).isEqualTo(data3);

            assertThat(records.get(0).sequence()).isEqualTo(1);
            assertThat(records.get(1).sequence()).isEqualTo(2);
            assertThat(records.get(2).sequence()).isEqualTo(3);
        }

        @Test
        @DisplayName("写入大量记录后读回全部数据")
        void testWriteAndRecoverLargeNumberOfRecords() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            int numRecords = 100;
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            for (int i = 0; i < numRecords; i++) {
                writer.write(createDataRecord(i, ("record_" + i).getBytes(), "table1"));
            }
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(numRecords);
            for (int i = 0; i < numRecords; i++) {
                assertThat(records.get(i).sequence()).isEqualTo(i);
                assertThat(records.get(i).value()).isEqualTo(("record_" + i).getBytes());
            }
        }
    }

    @Nested
    @DisplayName("多表写入测试")
    class MultipleTablesTest {

        @Test
        @DisplayName("写入不同tableId的记录，WALReader正确按tableId分组")
        void testWriteMultipleTableIdsAndGroupByTableId() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data_t1_1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data_t2_1".getBytes(), "table2"));
            writer.write(createDataRecord(3, "data_t1_2".getBytes(), "table1"));
            writer.write(createDataRecord(4, "data_t2_2".getBytes(), "table2"));
            writer.write(createDataRecord(5, "data_t3_1".getBytes(), "table3"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(5);

            Map<String, List<WALRecord>> byTableId = records.stream()
                    .collect(Collectors.groupingBy(WALRecord::uniqTableIdentifier));

            assertThat(byTableId).containsKeys("table1", "table2", "table3");
            assertThat(byTableId.get("table1")).hasSize(2);
            assertThat(byTableId.get("table2")).hasSize(2);
            assertThat(byTableId.get("table3")).hasSize(1);
        }

        @Test
        @DisplayName("不同tableId的记录保持原始顺序")
        void testMultipleTableIdsPreserveOrder() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(10, "data1".getBytes(), "alpha"));
            writer.write(createDataRecord(20, "data2".getBytes(), "beta"));
            writer.write(createDataRecord(30, "data3".getBytes(), "alpha"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records.get(0).sequence()).isEqualTo(10);
            assertThat(records.get(0).uniqTableIdentifier()).isEqualTo("alpha");
            assertThat(records.get(1).sequence()).isEqualTo(20);
            assertThat(records.get(1).uniqTableIdentifier()).isEqualTo("beta");
            assertThat(records.get(2).sequence()).isEqualTo(30);
            assertThat(records.get(2).uniqTableIdentifier()).isEqualTo("alpha");
        }
    }

    @Nested
    @DisplayName("SYNC_MARKER集成测试")
    class SyncMarkerIntegrationTest {

        @Test
        @DisplayName("写入记录+SYNC_MARKER+更多记录，filterUnflushed正确过滤")
        void testSyncMarkerFilterUnflushedIntegration() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "before_sync_1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "before_sync_2".getBytes(), "table1"));
            writer.write(createSyncMarker(2, "table1"));
            writer.write(createDataRecord(3, "after_sync_1".getBytes(), "table1"));
            writer.write(createDataRecord(4, "after_sync_2".getBytes(), "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> allRecords = reader.readAll();
            List<WALRecord> unflushedRecords = reader.readUnflushed();
            reader.close();

            assertThat(allRecords).hasSize(5);
            assertThat(unflushedRecords).hasSize(2);
            assertThat(unflushedRecords.get(0).sequence()).isEqualTo(3);
            assertThat(unflushedRecords.get(1).sequence()).isEqualTo(4);
        }

        @Test
        @DisplayName("多个SYNC_MARKER场景，filterUnflushed以最后一个为准")
        void testMultipleSyncMarkersIntegration() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker(1, "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            writer.write(createSyncMarker(3, "table1"));
            writer.write(createDataRecord(4, "data4".getBytes(), "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).hasSize(1);
            assertThat(unflushed.get(0).sequence()).isEqualTo(4);
        }

        @Test
        @DisplayName("SYNC_MARKER在readAll中包含但marker字段为SYNC_MARKER")
        void testSyncMarkerInReadAll() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker(1, "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> allRecords = reader.readAll();
            reader.close();

            assertThat(allRecords).hasSize(3);
            assertThat(allRecords.get(0).marker()).isEqualTo("DATA");
            assertThat(allRecords.get(1).marker()).isEqualTo("SYNC_MARKER");
            assertThat(allRecords.get(2).marker()).isEqualTo("DATA");
        }
    }

    @Nested
    @DisplayName("文件滚动+恢复集成测试")
    class FileRollingRecoveryTest {

        @Test
        @DisplayName("文件滚动后所有记录可读")
        void testFileRollingAndRecovery() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            long maxSize = 512L;
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, maxSize, 0L);

            byte[] largeData = new byte[200];
            java.util.Arrays.fill(largeData, (byte) 'A');

            for (int i = 0; i < 5; i++) {
                writer.write(createDataRecord(i, largeData, "table1"));
            }
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(5);
            for (int i = 0; i < 5; i++) {
                assertThat(records.get(i).sequence()).isEqualTo(i);
                assertThat(records.get(i).value()).isEqualTo(largeData);
            }
        }
    }

    @Nested
    @DisplayName("空值和边界条件集成测试")
    class EdgeCaseIntegrationTest {

        @Test
        @DisplayName("空数据的DATA记录可正确写入和读回")
        void testEmptyDataRecordRoundTrip() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(new DefaultWALRecord(1, new byte[0], System.nanoTime(), 1L, "table1", "DATA"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(1);
            assertThat(records.get(0).value()).isEmpty();
            assertThat(records.get(0).marker()).isEqualTo("DATA");
        }

        @Test
        @DisplayName("大tableId的记录可正确写入和读回")
        void testLongTableIdRoundTrip() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            String longTableId = "catalog.database.very_long_table_name_with_many_segments_1234567890";
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data".getBytes(), longTableId));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(1);
            assertThat(records.get(0).uniqTableIdentifier()).isEqualTo(longTableId);
        }
    }
}
