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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * WALRecord和DefaultWALRecord序列化/反序列化测试
 *
 * 测试要点:
 * 1. DefaultWALRecord创建时设置所有字段
 * 2. serialize()和deserialize()保持数据一致
 * 3. createSyncMarker()创建正确的同步标记
 */
@DisplayName("WALRecord - 序列化/反序列化测试")
class WALRecordTest {

    @Nested
    @DisplayName("DefaultWALRecord创建测试")
    class DefaultWALRecordCreationTest {

        @Test
        @DisplayName("DefaultWALRecord创建时正确设置所有字段")
        void testDefaultWALRecordCreation() {
            long lsn = 12345L;
            byte[] data = "test_data".getBytes();
            long timestamp = System.nanoTime();
            long commitId = 999L;
            String tableId = "test_table";
            String marker = "DATA";

            DefaultWALRecord record = new DefaultWALRecord(lsn, data, timestamp, commitId, tableId, marker);

            assertThat(record.sequence()).isEqualTo(lsn);
            assertThat(record.value()).isEqualTo(data);
            assertThat(record.timestamp()).isEqualTo(timestamp);
            assertThat(record.commitId()).isEqualTo(commitId);
            assertThat(record.uniqTableIdentifier()).isEqualTo(tableId);
            assertThat(record.marker()).isEqualTo(marker);
        }

        @Test
        @DisplayName("DefaultWALRecord.length()返回正确长度")
        void testLengthCalculation() {
            String tableId = "table1";
            String marker = "DATA";
            byte[] data = new byte[]{1, 2, 3};

            DefaultWALRecord record = new DefaultWALRecord(100L, data, 1000L, 1L, tableId, marker);

            // 长度 = 4(tableId长度) + tableId长度 + 8(commitId) + 8(lsn) + 4(marker长度) + marker长度 + 8(timestamp) + 4(data长度) + data长度
            int expectedLength = 4 + tableId.getBytes().length + 8 + 8 + 4 + marker.getBytes().length + 8 + 4 + data.length;
            assertThat(record.length()).isEqualTo(expectedLength);
        }
    }

    @Nested
    @DisplayName("序列化/反序列化一致性测试")
    class SerializationDeserializationTest {

        @Test
        @DisplayName("serialize()和deserialize()保持数据一致")
        void testSerializeDeserializeConsistency() {
            long lsn = 98765L;
            byte[] originalData = "consistent_data_test".getBytes();
            long timestamp = System.nanoTime();
            long commitId = 555L;
            String tableId = "consistency_test_table";
            String marker = "DATA";

            DefaultWALRecord original = new DefaultWALRecord(lsn, originalData, timestamp, commitId, tableId, marker);

            // Serialize
            byte[] serialized = original.serialize();

            // Deserialize
            DefaultWALRecord deserialized = DefaultWALRecord.deserialize(serialized);

            // Verify all fields match
            assertThat(deserialized.sequence()).isEqualTo(original.sequence());
            assertThat(deserialized.value()).isEqualTo(original.value());
            assertThat(deserialized.timestamp()).isEqualTo(original.timestamp());
            assertThat(deserialized.commitId()).isEqualTo(original.commitId());
            assertThat(deserialized.uniqTableIdentifier()).isEqualTo(original.uniqTableIdentifier());
            assertThat(deserialized.marker()).isEqualTo(original.marker());
        }

        @Test
        @DisplayName("序列化后字节数组长度等于record.length()")
        void testSerializedLengthMatchesLengthMethod() {
            String tableId = "length_test";
            byte[] data = "test".getBytes();

            DefaultWALRecord record = new DefaultWALRecord(1L, data, 1000L, 1L, tableId, "DATA");
            byte[] serialized = record.serialize();

            assertThat(serialized.length).isEqualTo(record.length());
        }

        @Test
        @DisplayName("空数据序列化/反序列化正常")
        void testEmptyDataSerialization() {
            byte[] emptyData = new byte[0];
            DefaultWALRecord record = new DefaultWALRecord(1L, emptyData, 1000L, 1L, "table", "DATA");

            byte[] serialized = record.serialize();
            DefaultWALRecord deserialized = DefaultWALRecord.deserialize(serialized);

            assertThat(deserialized.value()).isEqualTo(emptyData);
        }

        @Test
        @DisplayName("特殊字符表名序列化/反序列化正常")
        void testSpecialCharactersInTableId() {
            String specialTableId = "table_with_中文_and_emoji_🎉";
            byte[] data = "data".getBytes();

            DefaultWALRecord record = new DefaultWALRecord(1L, data, 1000L, 1L, specialTableId, "DATA");

            byte[] serialized = record.serialize();
            DefaultWALRecord deserialized = DefaultWALRecord.deserialize(serialized);

            assertThat(deserialized.uniqTableIdentifier()).isEqualTo(specialTableId);
        }
    }

    @Nested
    @DisplayName("createSyncMarker测试")
    class SyncMarkerTest {

        @Test
        @DisplayName("createSyncMarker()创建正确的同步标记")
        void testCreateSyncMarker() {
            String tableId = "sync_test_table";
            WALRecord marker = WALRecord.createSyncMarker(tableId);

            assertThat(marker.sequence()).isEqualTo(-1L);
            assertThat(marker.value()).isEmpty();
            assertThat(marker.commitId()).isEqualTo(-1L);
            assertThat(marker.uniqTableIdentifier()).isEqualTo(tableId);
            assertThat(marker.marker()).isEqualTo("SYNC_MARKER");
        }

        @Test
        @DisplayName("SYNC_MARKER序列化后仍可识别")
        void testSyncMarkerSerialization() {
            String tableId = "marker_serialization_test";
            WALRecord originalMarker = WALRecord.createSyncMarker(tableId);

            byte[] serialized = originalMarker.serialize();
            DefaultWALRecord deserialized = DefaultWALRecord.deserialize(serialized);

            assertThat(deserialized.marker()).isEqualTo("SYNC_MARKER");
            assertThat(deserialized.sequence()).isEqualTo(-1L);
            assertThat(deserialized.value()).isEmpty();
        }

        @Test
        @DisplayName("SYNC_MARKER的length()正确计算")
        void testSyncMarkerLength() {
            String tableId = "length_test";
            WALRecord marker = WALRecord.createSyncMarker(tableId);

            // SYNC_MARKER有特殊的序列化格式
            int expectedLength = 4 + tableId.getBytes().length + 8 + 8 + 4 + "SYNC_MARKER".getBytes().length + 8 + 4 + 0;
            assertThat(marker.length()).isEqualTo(expectedLength);
        }
    }

    @Nested
    @DisplayName("WALRecord接口方法测试")
    class WALRecordInterfaceTest {

        @Test
        @DisplayName("WALRecord.createSyncMarker返回的实例可以调用所有接口方法")
        void testSyncMarkerImplementsWALRecord() {
            WALRecord marker = WALRecord.createSyncMarker("test");

            // 确保所有WALRecord接口方法都可调用
            assertThatCode(() -> marker.serialize()).doesNotThrowAnyException();
            assertThatCode(() -> marker.sequence()).doesNotThrowAnyException();
            assertThatCode(() -> marker.marker()).doesNotThrowAnyException();
            assertThatCode(() -> marker.length()).doesNotThrowAnyException();
            assertThatCode(() -> marker.value()).doesNotThrowAnyException();
            assertThatCode(() -> marker.timestamp()).doesNotThrowAnyException();
            assertThatCode(() -> marker.commitId()).doesNotThrowAnyException();
            assertThatCode(() -> marker.uniqTableIdentifier()).doesNotThrowAnyException();
        }
    }
}
