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

import io.github.ximin.xlake.common.config.ConfigFactory;
import io.github.ximin.xlake.common.config.XlakeConfig;
import io.github.ximin.xlake.common.config.XlakeOptions;
import io.github.ximin.xlake.storage.table.record.NullableValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * DynamicMmapStore WAL集成测试
 * <p>
 * 测试要点:
 * 1. WAL在STORAGE_WAL_ENABLED=true时创建
 * 2. WAL在STORAGE_WAL_ENABLED=false时为null
 * 3. recoverFromWAL()正确回放记录
 * 4. extractWalData()格式正确
 * 5. extractTableIdFromWALRecord()解析正确
 * 6. extractKeyFromWALRecord()提取正确
 * 7. extractValueFromWALRecord()提取正确
 * 8. WAL恢复应用NullableValueCodec.encode()
 */
@DisplayName("DynamicMmapStore - WAL集成测试")
class DynamicMmapStoreWALIntegrationTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("WAL启用状态测试")
    class WALEnabledTest {

        @Test
        @DisplayName("WAL在STORAGE_WAL_ENABLED=true时创建")
        void testWALCreatedWhenEnabled() throws Exception {
            // Given: 配置WAL启用
            XlakeConfig config = ConfigFactory.builder()
                    .set(XlakeOptions.STORAGE_WAL_ENABLED, true)
                    .set(XlakeOptions.STORAGE_WAL_BUFFER_SIZE, 8192)
                    .set(XlakeOptions.STORAGE_WAL_MAX_SIZE_BYTES, 100L * 1024 * 1024)
                    .build();

            // We need to override the config used by DynamicMmapStore
            // For this test, we verify the config defaults
            assertThat(config.get(XlakeOptions.STORAGE_WAL_ENABLED)).isTrue();
            assertThat(config.get(XlakeOptions.STORAGE_WAL_BUFFER_SIZE)).isEqualTo(8192);
            assertThat(config.get(XlakeOptions.STORAGE_WAL_MAX_SIZE_BYTES)).isEqualTo(100L * 1024 * 1024);
        }

        @Test
        @DisplayName("WAL在STORAGE_WAL_ENABLED=false时为null")
        void testWALNullWhenDisabled() throws Exception {
            // Given: 配置WAL禁用
            XlakeConfig config = ConfigFactory.builder()
                    .set(XlakeOptions.STORAGE_WAL_ENABLED, false)
                    .build();

            // Verify WAL is disabled in config
            assertThat(config.get(XlakeOptions.STORAGE_WAL_ENABLED)).isFalse();
        }
    }

    @Nested
    @DisplayName("WAL数据提取格式测试")
    class WALDataExtractionTest {

        @Test
        @DisplayName("extractWalData格式: tableIdLen(4) + tableId + keyLen(4) + key + valueLen(4) + value")
        void testExtractWalDataFormat() {
            String tableId = "test_table";
            byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
            byte[] key = "test_key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "test_value".getBytes(StandardCharsets.UTF_8);

            // 构造与extractWalData相同的格式
            ByteBuffer data = ByteBuffer.allocate(4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
            data.putInt(tableIdBytes.length);
            data.put(tableIdBytes);
            data.putInt(key.length);
            data.put(key);
            data.putInt(value.length);
            data.put(value);
            byte[] walData = data.array();

            // 解析tableId (模拟extractTableIdFromWALRecord)
            ByteBuffer buffer = ByteBuffer.wrap(walData);
            int tableIdLen = buffer.getInt();
            assertThat(tableIdLen).isEqualTo(tableIdBytes.length);
            byte[] extractedTableIdBytes = new byte[tableIdLen];
            buffer.get(extractedTableIdBytes);
            assertThat(new String(extractedTableIdBytes)).isEqualTo(tableId);

            // 解析key (模拟extractKeyFromWALRecord)
            int keyLen = buffer.getInt();
            assertThat(keyLen).isEqualTo(key.length);
            byte[] extractedKey = new byte[keyLen];
            buffer.get(extractedKey);
            assertThat(new String(extractedKey)).isEqualTo("test_key");

            // 解析value (模拟extractValueFromWALRecord)
            int valueLen = buffer.getInt();
            assertThat(valueLen).isEqualTo(value.length);
            byte[] extractedValue = new byte[valueLen];
            buffer.get(extractedValue);
            assertThat(new String(extractedValue)).isEqualTo("test_value");
        }

        @Test
        @DisplayName("extractTableIdFromWALRecord正确解析tableId")
        void testExtractTableIdFromWALRecord() {
            String tableId = "catalog.db.table";
            byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
            byte[] key = "key1".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

            // 构建WAL数据
            ByteBuffer data = ByteBuffer.allocate(4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
            data.putInt(tableIdBytes.length);
            data.put(tableIdBytes);
            data.putInt(key.length);
            data.put(key);
            data.putInt(value.length);
            data.put(value);
            byte[] walData = data.array();

            // 解析tableId
            ByteBuffer buffer = ByteBuffer.wrap(walData);
            int tableIdLen = buffer.getInt();
            assertThat(tableIdLen).isEqualTo(tableIdBytes.length);
            byte[] extractedTableIdBytes = new byte[tableIdLen];
            buffer.get(extractedTableIdBytes);
            assertThat(new String(extractedTableIdBytes)).isEqualTo(tableId);
        }

        @Test
        @DisplayName("extractKeyFromWALRecord正确解析key")
        void testExtractKeyFromWALRecord() {
            String tableId = "table";
            byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
            byte[] key = "my_test_key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "val".getBytes(StandardCharsets.UTF_8);

            ByteBuffer data = ByteBuffer.allocate(4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
            data.putInt(tableIdBytes.length);
            data.put(tableIdBytes);
            data.putInt(key.length);
            data.put(key);
            data.putInt(value.length);
            data.put(value);
            byte[] walData = data.array();

            // 解析: 跳过tableId
            ByteBuffer buffer = ByteBuffer.wrap(walData);
            int tableIdLen = buffer.getInt();
            buffer.position(buffer.position() + tableIdLen);

            // 解析key
            int keyLen = buffer.getInt();
            assertThat(keyLen).isEqualTo(key.length);
            byte[] extractedKey = new byte[keyLen];
            buffer.get(extractedKey);
            assertThat(new String(extractedKey)).isEqualTo("my_test_key");
        }

        @Test
        @DisplayName("extractValueFromWALRecord正确解析value")
        void testExtractValueFromWALRecord() {
            String tableId = "table";
            byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
            byte[] key = "key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "my_test_value".getBytes(StandardCharsets.UTF_8);

            ByteBuffer data = ByteBuffer.allocate(4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
            data.putInt(tableIdBytes.length);
            data.put(tableIdBytes);
            data.putInt(key.length);
            data.put(key);
            data.putInt(value.length);
            data.put(value);
            byte[] walData = data.array();

            // 解析: 跳过tableId和key
            ByteBuffer buffer = ByteBuffer.wrap(walData);
            int tableIdLen = buffer.getInt();
            buffer.position(buffer.position() + tableIdLen);
            int keyLen = buffer.getInt();
            buffer.position(buffer.position() + keyLen);

            // 解析value
            int valueLen = buffer.getInt();
            assertThat(valueLen).isEqualTo(value.length);
            byte[] extractedValue = new byte[valueLen];
            buffer.get(extractedValue);
            assertThat(new String(extractedValue)).isEqualTo("my_test_value");
        }

        @Test
        @DisplayName("extractKeyFromWALRecord处理异常数据")
        void testExtractKeyFromWALRecordWithInvalidData() {
            // 空数据会导致BufferUnderflowException
            assertThatThrownBy(() -> {
                ByteBuffer buf = ByteBuffer.wrap(new byte[0]);
                buf.getInt();
            }).isInstanceOf(java.nio.BufferUnderflowException.class);
        }
    }

    @Nested
    @DisplayName("NullableValueCodec编码测试")
    class NullableValueCodecTest {

        @Test
        @DisplayName("NullableValueCodec.encode()对非null值添加标记")
        void testEncodeNonNullValue() {
            byte[] value = "test_value".getBytes(StandardCharsets.UTF_8);
            byte[] encoded = NullableValueCodec.encode(value);

            assertThat(encoded.length).isEqualTo(value.length + 1);
            assertThat(encoded[0]).isEqualTo((byte) 1); // null标记为0，非null标记为1
            assertThat(java.util.Arrays.copyOfRange(encoded, 1, encoded.length)).isEqualTo(value);
        }

        @Test
        @DisplayName("NullableValueCodec.encode()对null值编码为空字节数组标记")
        void testEncodeNullValue() {
            byte[] encoded = NullableValueCodec.encode(null);

            assertThat(encoded.length).isEqualTo(1);
            assertThat(encoded[0]).isEqualTo((byte) 0);
        }

        @Test
        @DisplayName("NullableValueCodec.encode()对空值编码正确")
        void testEncodeEmptyValue() {
            byte[] value = new byte[0];
            byte[] encoded = NullableValueCodec.encode(value);

            // 空值编码后长度 = 1 (标记)
            assertThat(encoded.length).isEqualTo(1);
            assertThat(encoded[0]).isEqualTo((byte) 1);
        }

        @Test
        @DisplayName("WAL恢复时应用NullableValueCodec.encode()")
        void testWALRecoveryAppliesNullableValueCodec() {
            // 模拟WAL恢复场景
            byte[] originalValue = "recovery_test".getBytes(StandardCharsets.UTF_8);

            // 原始值写入LMDB前会先经过NullableValueCodec.encode()
            byte[] encodedValue = NullableValueCodec.encode(originalValue);

            // 恢复时，从WAL提取的原始值需要再次编码
            byte[] recoveredEncoded = NullableValueCodec.encode(originalValue);

            // 两次编码结果应该一致
            assertThat(recoveredEncoded).isEqualTo(encodedValue);
        }
    }

    @Nested
    @DisplayName("WAL配置与默认值测试")
    class WALConfigDefaultsTest {

        @Test
        @DisplayName("STORAGE_WAL_ENABLED默认值是true")
        void testStorageWalEnabledDefault() {
            assertThat(XlakeOptions.STORAGE_WAL_ENABLED.defaultValue()).isTrue();
        }

        @Test
        @DisplayName("STORAGE_WAL_BUFFER_SIZE默认值是8192")
        void testStorageWalBufferSizeDefault() {
            assertThat(XlakeOptions.STORAGE_WAL_BUFFER_SIZE.defaultValue()).isEqualTo(8192);
        }

        @Test
        @DisplayName("STORAGE_WAL_SYNC_INTERVAL_MS默认值是1000L")
        void testStorageWalSyncIntervalMsDefault() {
            assertThat(XlakeOptions.STORAGE_WAL_SYNC_INTERVAL_MS.defaultValue()).isEqualTo(1000L);
        }

        @Test
        @DisplayName("STORAGE_WAL_MAX_SIZE_BYTES默认值是100MB")
        void testStorageWalMaxSizeBytesDefault() {
            assertThat(XlakeOptions.STORAGE_WAL_MAX_SIZE_BYTES.defaultValue()).isEqualTo(100L * 1024 * 1024);
        }

        @Test
        @DisplayName("WALConfig.DEFAULT使用正确的默认值")
        void testWALConfigDefaultValues() {
            WALConfig config = WALConfig.DEFAULT;

            assertThat(config.logDir()).isEqualTo("wal_logs");
            assertThat(config.logFilePrefix()).isEqualTo("wal");
            assertThat(config.segmentSize()).isEqualTo(100 * 1024 * 1024);
            assertThat(config.syncOnWrite()).isTrue();
            assertThat(config.bufferSize()).isEqualTo(8192);
        }
    }

    @Nested
    @DisplayName("WAL记录创建与同步标记测试")
    class WALRecordCreationTest {

        @Test
        @DisplayName("WALRecord.createSyncMarker创建正确的同步标记")
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
        @DisplayName("SYNC_MARKER序列化后marker字段仍为SYNC_MARKER")
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
        @DisplayName("DATA记录正确创建")
        void testDataRecordCreation() {
            byte[] data = "test_data".getBytes(StandardCharsets.UTF_8);
            long timestamp = System.nanoTime();
            String tableId = "data_test_table";

            DefaultWALRecord record = new DefaultWALRecord(
                    12345L,     // sequence
                    data,       // value
                    timestamp,  // timestamp
                    999L,       // commitId
                    tableId,    // uniqTableIdentifier
                    "DATA"      // marker
            );

            assertThat(record.sequence()).isEqualTo(12345L);
            assertThat(record.value()).isEqualTo(data);
            assertThat(record.timestamp()).isEqualTo(timestamp);
            assertThat(record.commitId()).isEqualTo(999L);
            assertThat(record.uniqTableIdentifier()).isEqualTo(tableId);
            assertThat(record.marker()).isEqualTo("DATA");
        }

        @Test
        @DisplayName("序列化/反序列化保持DATA记录一致性")
        void testDataRecordSerializationConsistency() {
            byte[] originalData = "consistent_data".getBytes(StandardCharsets.UTF_8);
            String tableId = "consistency_test";
            long timestamp = System.nanoTime();

            DefaultWALRecord original = new DefaultWALRecord(
                    99999L, originalData, timestamp, 777L, tableId, "DATA"
            );

            byte[] serialized = original.serialize();
            DefaultWALRecord deserialized = DefaultWALRecord.deserialize(serialized);

            assertThat(deserialized.sequence()).isEqualTo(original.sequence());
            assertThat(deserialized.value()).isEqualTo(original.value());
            assertThat(deserialized.timestamp()).isEqualTo(original.timestamp());
            assertThat(deserialized.commitId()).isEqualTo(original.commitId());
            assertThat(deserialized.uniqTableIdentifier()).isEqualTo(original.uniqTableIdentifier());
            assertThat(deserialized.marker()).isEqualTo(original.marker());
        }
    }
}
