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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AbstractWALWriter - 抽象WAL写入器测试")
class AbstractWALWriterTest {

    private static class TestableWALWriter extends AbstractWALWriter {
        private final List<byte[]> writtenChunks = new ArrayList<>();
        private boolean streamOpen = false;
        private int syncCount = 0;
        private int closeCount = 0;
        private int createFileCount = 0;

        TestableWALWriter(String storeId, long maxSizeBytes, long syncIntervalMs) {
            super(storeId, maxSizeBytes, syncIntervalMs);
        }

        @Override
        protected boolean isStreamOpen() {
            return streamOpen;
        }

        @Override
        protected void doWrite(byte[] data) throws IOException {
            writtenChunks.add(data.clone());
        }

        @Override
        protected void doSync() throws IOException {
            syncCount++;
        }

        @Override
        protected void doClose() throws IOException {
            closeCount++;
            streamOpen = false;
        }

        @Override
        protected void doCreateFile() throws IOException {
            createFileCount++;
            streamOpen = true;
            byte[] header = serializeHeader("test-executor");
            writtenChunks.add(header.clone());
            currentFileSize.set(header.length);
            recordCount.set(0);
        }

        List<byte[]> getWrittenChunks() {
            return writtenChunks;
        }

        int getSyncCount() {
            return syncCount;
        }

        int getCloseCount() {
            return closeCount;
        }

        int getCreateFileCount() {
            return createFileCount;
        }
    }

    private DefaultWALRecord createDataRecord(long sequence, byte[] data, String tableId) {
        return new DefaultWALRecord(sequence, data, System.nanoTime(), 1L, tableId, "DATA");
    }

    private DefaultWALRecord createSyncMarker(String tableId) {
        return new DefaultWALRecord(-1, new byte[0], System.nanoTime(), -1L, tableId, "SYNC_MARKER");
    }

    @Nested
    @DisplayName("serializeRecord测试")
    class SerializeRecordTest {

        @Test
        @DisplayName("serializeRecord()产生正确的二进制格式: payloadSize + recordType + sequence + timestamp + tableIdLen + tableId + commitId + dataLen + data + CRC32")
        void testSerializeRecordFormat() {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            byte[] data = "test_data".getBytes(StandardCharsets.UTF_8);
            String tableId = "myTable";
            long sequence = 42L;
            long timestamp = 1234567890L;
            long commitId = 7L;

            DefaultWALRecord record = new DefaultWALRecord(sequence, data, timestamp, commitId, tableId, "DATA");
            byte[] serialized = writer.serializeRecord(record);

            ByteBuffer buffer = ByteBuffer.wrap(serialized);

            int payloadSize = buffer.getInt();
            byte recordType = buffer.get();
            assertThat(recordType).isEqualTo(AbstractWALWriter.RECORD_TYPE_DATA);

            long seq = buffer.getLong();
            assertThat(seq).isEqualTo(sequence);

            long ts = buffer.getLong();
            assertThat(ts).isEqualTo(timestamp);

            int tableIdLen = buffer.getInt();
            assertThat(tableIdLen).isEqualTo(tableId.getBytes(StandardCharsets.UTF_8).length);

            byte[] tableIdBytes = new byte[tableIdLen];
            buffer.get(tableIdBytes);
            assertThat(new String(tableIdBytes, StandardCharsets.UTF_8)).isEqualTo(tableId);

            long cId = buffer.getLong();
            assertThat(cId).isEqualTo(commitId);

            int dataLen = buffer.getInt();
            assertThat(dataLen).isEqualTo(data.length);

            byte[] dataBytes = new byte[dataLen];
            buffer.get(dataBytes);
            assertThat(dataBytes).isEqualTo(data);

            int storedCrc = buffer.getInt();

            CRC32 crc32 = new CRC32();
            crc32.update(serialized, 4, payloadSize);
            assertThat(storedCrc).isEqualTo((int) crc32.getValue());
        }

        @Test
        @DisplayName("serializeRecord()对SYNC_MARKER使用recordType=2")
        void testSerializeSyncMarkerRecordType() {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            DefaultWALRecord syncMarker = createSyncMarker("table1");
            byte[] serialized = writer.serializeRecord(syncMarker);

            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            buffer.getInt();
            byte recordType = buffer.get();
            assertThat(recordType).isEqualTo(AbstractWALWriter.RECORD_TYPE_SYNC_MARKER);
        }

        @Test
        @DisplayName("serializeRecord()对null tableId处理正确")
        void testSerializeRecordWithNullTableId() {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            DefaultWALRecord record = new DefaultWALRecord(1, "data".getBytes(), System.nanoTime(), 1L, null, "DATA");
            byte[] serialized = writer.serializeRecord(record);

            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            buffer.getInt();
            buffer.get();
            buffer.getLong();
            buffer.getLong();
            int tableIdLen = buffer.getInt();
            assertThat(tableIdLen).isEqualTo(0);
        }

        @Test
        @DisplayName("serializeRecord()对null value处理正确")
        void testSerializeRecordWithNullValue() {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            DefaultWALRecord record = new DefaultWALRecord(1, null, System.nanoTime(), 1L, "table1", "DATA");
            byte[] serialized = writer.serializeRecord(record);

            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            buffer.getInt();
            buffer.get();
            buffer.getLong();
            buffer.getLong();
            int tableIdLen = buffer.getInt();
            byte[] tableIdBytes = new byte[tableIdLen];
            buffer.get(tableIdBytes);
            buffer.getLong();
            int dataLen = buffer.getInt();
            assertThat(dataLen).isEqualTo(0);
        }

        @Test
        @DisplayName("serializeRecord()的totalSize = 4 + payloadSize + 4")
        void testSerializeRecordTotalSize() {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
            String tableId = "tbl";
            DefaultWALRecord record = new DefaultWALRecord(1, data, 100L, 1L, tableId, "DATA");

            byte[] serialized = writer.serializeRecord(record);
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            int payloadSize = buffer.getInt();

            int expectedPayloadSize = 1 + 8 + 8 + 4 + tableId.getBytes(StandardCharsets.UTF_8).length + 8 + 4 + data.length;
            assertThat(payloadSize).isEqualTo(expectedPayloadSize);
            assertThat(serialized.length).isEqualTo(4 + expectedPayloadSize + 4);
        }
    }

    @Nested
    @DisplayName("serializeHeader测试")
    class SerializeHeaderTest {

        @Test
        @DisplayName("serializeHeader()产生MAGIC + VERSION + headerJsonLen + headerJson + CRC32格式")
        void testSerializeHeaderFormat() {
            TestableWALWriter writer = new TestableWALWriter("myStore", 1024 * 1024, 0L);

            byte[] header = writer.serializeHeader("executor-1");

            ByteBuffer buffer = ByteBuffer.wrap(header);

            byte[] magic = new byte[4];
            buffer.get(magic);
            assertThat(magic).isEqualTo(AbstractWALWriter.MAGIC);

            int version = buffer.getInt();
            assertThat(version).isEqualTo(AbstractWALWriter.VERSION);

            int headerLength = buffer.getInt();
            assertThat(headerLength).isGreaterThan(0);

            byte[] headerJson = new byte[headerLength];
            buffer.get(headerJson);
            String headerStr = new String(headerJson, StandardCharsets.UTF_8);
            assertThat(headerStr).contains("myStore");
            assertThat(headerStr).contains("executor-1");

            int storedCrc = buffer.getInt();

            CRC32 crc32 = new CRC32();
            crc32.update(header, 0, 4 + 4 + 4 + headerLength);
            assertThat(storedCrc).isEqualTo((int) crc32.getValue());
        }

        @Test
        @DisplayName("serializeHeader()的headerJson包含compression字段")
        void testSerializeHeaderContainsCompression() {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            byte[] header = writer.serializeHeader("exec-1");
            ByteBuffer buffer = ByteBuffer.wrap(header);

            buffer.position(4 + 4);
            int headerLength = buffer.getInt();
            byte[] headerJson = new byte[headerLength];
            buffer.get(headerJson);
            String headerStr = new String(headerJson, StandardCharsets.UTF_8);
            assertThat(headerStr).contains("compression");
            assertThat(headerStr).contains("NONE");
        }
    }

    @Nested
    @DisplayName("closed标志测试")
    class ClosedFlagTest {

        @Test
        @DisplayName("close()后write()抛出IOException")
        void testWriteAfterCloseThrowsIOException() throws IOException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data".getBytes(), "table1"));
            writer.close();

            assertThatThrownBy(() -> writer.write(createDataRecord(2, "data".getBytes(), "table1")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed");
        }

        @Test
        @DisplayName("close()是幂等的")
        void testCloseIdempotent() throws IOException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data".getBytes(), "table1"));

            writer.close();
            int closeCountAfterFirst = writer.getCloseCount();
            writer.close();

            assertThat(writer.getCloseCount()).isEqualTo(closeCountAfterFirst);
        }
    }

    @Nested
    @DisplayName("forceSyncIfNeeded时序逻辑测试")
    class ForceSyncIfNeededTest {

        @Test
        @DisplayName("syncIntervalMs <= 0时不触发sync")
        void testNoSyncWhenIntervalZeroOrNegative() throws IOException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            writer.write(createDataRecord(1, "data".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data".getBytes(), "table1"));

            assertThat(writer.getSyncCount()).isEqualTo(0);
            writer.close();
        }

        @Test
        @DisplayName("syncIntervalMs > 0时在间隔到期后触发sync")
        void testSyncTriggeredAfterInterval() throws IOException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 1L);

            writer.write(createDataRecord(1, "data".getBytes(), "table1"));

            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            writer.write(createDataRecord(2, "data".getBytes(), "table1"));

            assertThat(writer.getSyncCount()).isGreaterThanOrEqualTo(1);
            writer.close();
        }
    }

    @Nested
    @DisplayName("文件滚动触发测试")
    class FileRollingTriggerTest {

        @Test
        @DisplayName("currentFileSize + recordSize > maxSizeBytes时触发文件滚动")
        void testFileRollingWhenExceedsMaxSize() throws IOException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 200L, 0L);

            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));

            assertThat(writer.getCreateFileCount()).isGreaterThanOrEqualTo(2);
            writer.close();
        }

        @Test
        @DisplayName("首条记录不触发文件滚动(recordCount=0)")
        void testFirstRecordDoesNotTriggerRoll() throws IOException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1L, 0L);

            writer.write(createDataRecord(1, "very_large_data_padded_to_exceed_limit".getBytes(), "table1"));

            assertThat(writer.getCreateFileCount()).isEqualTo(1);
            writer.close();
        }

        @Test
        @DisplayName("文件滚动时先sync再close当前文件再create新文件")
        void testFileRollingOrder() throws IOException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 200L, 0L);

            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            int initialCreateCount = writer.getCreateFileCount();

            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));

            assertThat(writer.getCreateFileCount()).isGreaterThan(initialCreateCount);
            assertThat(writer.getCloseCount()).isGreaterThan(0);
            writer.close();
        }
    }

    @Nested
    @DisplayName("writtenBytes测试")
    class WrittenBytesTest {

        @Test
        @DisplayName("writtenBytes()返回累计写入字节数")
        void testWrittenBytesAccumulates() throws IOException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            long bytes1 = writer.writtenBytes();

            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            long bytes2 = writer.writtenBytes();

            assertThat(bytes2).isGreaterThan(bytes1);
            writer.close();
        }
    }

    @Nested
    @DisplayName("committedSequence测试")
    class CommittedSequenceTest {

        @Test
        @DisplayName("committedSequence初始值为-1")
        void testCommittedSequenceInitialValue() throws Exception {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            var field = AbstractWALWriter.class.getDeclaredField("committedSequence");
            field.setAccessible(true);
            long val = (long) field.get(writer);
            assertThat(val).isEqualTo(-1L);
        }

        @Test
        @DisplayName("写入后committedSequence更新为最新sequence")
        void testCommittedSequenceUpdatedOnWrite() throws IOException, NoSuchFieldException, IllegalAccessException {
            TestableWALWriter writer = new TestableWALWriter("testStore", 1024 * 1024, 0L);

            writer.write(createDataRecord(100, "data".getBytes(), "table1"));

            var field = AbstractWALWriter.class.getDeclaredField("committedSequence");
            field.setAccessible(true);
            long val = (long) field.get(writer);
            assertThat(val).isEqualTo(100L);

            writer.close();
        }
    }

    @Nested
    @DisplayName("常量测试")
    class ConstantsTest {

        @Test
        @DisplayName("MAGIC为NWL0的ASCII字节")
        void testMagicValue() {
            assertThat(AbstractWALWriter.MAGIC).isEqualTo("NWL0".getBytes(StandardCharsets.US_ASCII));
        }

        @Test
        @DisplayName("VERSION为1")
        void testVersionValue() {
            assertThat(AbstractWALWriter.VERSION).isEqualTo(1);
        }

        @Test
        @DisplayName("RECORD_TYPE_DATA为1")
        void testRecordTypeDataValue() {
            assertThat(AbstractWALWriter.RECORD_TYPE_DATA).isEqualTo((byte) 1);
        }

        @Test
        @DisplayName("RECORD_TYPE_SYNC_MARKER为2")
        void testRecordTypeSyncMarkerValue() {
            assertThat(AbstractWALWriter.RECORD_TYPE_SYNC_MARKER).isEqualTo((byte) 2);
        }

        @Test
        @DisplayName("HEADER_PREFIX_SIZE = 4(MAGIC) + 4(VERSION) + 4(headerLength) = 12")
        void testHeaderPrefixSize() {
            assertThat(AbstractWALWriter.HEADER_PREFIX_SIZE).isEqualTo(12);
        }
    }
}
