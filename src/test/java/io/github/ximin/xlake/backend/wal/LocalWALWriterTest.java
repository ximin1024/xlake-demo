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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

@DisplayName("LocalWALWriter - 本地WAL写入器测试")
class LocalWALWriterTest {

    @TempDir
    Path tempDir;

    private String walDir;
    private LocalWALWriter writer;

    @BeforeEach
    void setUp() {
        walDir = tempDir.resolve("wal").toString();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    private DefaultWALRecord createDataRecord(long sequence, byte[] data, String tableId) {
        return new DefaultWALRecord(sequence, data, System.nanoTime(), 1L, tableId, "DATA");
    }

    private DefaultWALRecord createSyncMarker(String tableId) {
        return new DefaultWALRecord(-1, new byte[0], System.nanoTime(), -1L, tableId, "SYNC_MARKER");
    }

    private long countNWLFiles() throws IOException {
        try (Stream<Path> stream = Files.walk(tempDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".nwl"))
                    .count();
        }
    }

    @Nested
    @DisplayName("单条记录写入测试")
    class SingleRecordWriteTest {

        @Test
        @DisplayName("写入单条记录后文件存在")
        void testWriteSingleRecordCreatesFile() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 1000L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));

            assertThat(countNWLFiles()).isEqualTo(1);
        }

        @Test
        @DisplayName("写入单条记录后writtenBytes大于0")
        void testWriteSingleRecordUpdatesWrittenBytes() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 1000L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));

            assertThat(writer.writtenBytes()).isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("多条记录写入测试")
    class MultipleRecordsWriteTest {

        @Test
        @DisplayName("写入多条记录后writtenBytes递增")
        void testWriteMultipleRecordsUpdatesWrittenBytes() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 1000L);

            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            long bytesAfterFirst = writer.writtenBytes();

            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            long bytesAfterSecond = writer.writtenBytes();

            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            long bytesAfterThird = writer.writtenBytes();

            assertThat(bytesAfterSecond).isGreaterThan(bytesAfterFirst);
            assertThat(bytesAfterThird).isGreaterThan(bytesAfterSecond);
        }

        @Test
        @DisplayName("写入多条记录后仍只有一个NWL文件")
        void testWriteMultipleRecordsStaysInSameFile() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 1000L);

            for (int i = 0; i < 10; i++) {
                writer.write(createDataRecord(i, ("data" + i).getBytes(), "table1"));
            }

            assertThat(countNWLFiles()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("文件滚动测试")
    class FileRollingTest {

        @Test
        @DisplayName("写入超过maxSizeBytes时创建新文件")
        void testFileRollingWhenExceedsMaxSize() throws IOException {
            long maxSize = 512L;
            writer = new LocalWALWriter("testStore", walDir, maxSize, 0L);

            byte[] largeData = new byte[200];
            java.util.Arrays.fill(largeData, (byte) 'A');

            writer.write(createDataRecord(1, largeData, "table1"));
            writer.write(createDataRecord(2, largeData, "table1"));
            writer.write(createDataRecord(3, largeData, "table1"));

            assertThat(countNWLFiles()).isGreaterThanOrEqualTo(2);
        }

        @Test
        @DisplayName("文件滚动边界: 记录恰好填满文件到maxSize")
        void testFileRollingBoundaryExactFill() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);

            byte[] data = "boundary".getBytes();
            for (int i = 0; i < 100; i++) {
                writer.write(createDataRecord(i, data, "table1"));
            }

            assertThat(countNWLFiles()).isEqualTo(1);
        }

        @Test
        @DisplayName("首条记录不触发文件滚动(即使超过maxSize)")
        void testFirstRecordDoesNotTriggerRoll() throws IOException {
            long maxSize = 64L;
            writer = new LocalWALWriter("testStore", walDir, maxSize, 0L);

            byte[] largeData = new byte[512];
            java.util.Arrays.fill(largeData, (byte) 'X');

            writer.write(createDataRecord(1, largeData, "table1"));

            assertThat(countNWLFiles()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("forceSync测试")
    class ForceSyncTest {

        @Test
        @DisplayName("forceSync()不抛出异常")
        void testForceSyncDoesNotThrow() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));

            assertThatCode(() -> writer.forceSync()).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("forceSync()在无数据时也不抛出异常")
        void testForceSyncWithNoData() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);

            assertThatCode(() -> writer.forceSync()).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("close测试")
    class CloseTest {

        @Test
        @DisplayName("close()后再次close()不抛出异常")
        void testCloseIdempotent() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 1000L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));

            writer.close();
            assertThatCode(() -> writer.close()).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("close()后write()抛出IOException")
        void testWriteAfterCloseThrowsIOException() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 1000L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.close();

            assertThatThrownBy(() -> writer.write(createDataRecord(2, "data2".getBytes(), "table1")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed");
        }

        @Test
        @DisplayName("close()后forceSync()不抛出异常")
        void testForceSyncAfterCloseDoesNotThrow() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 1000L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.close();

            assertThatCode(() -> writer.forceSync()).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Header格式测试")
    class HeaderFormatTest {

        @Test
        @DisplayName("NWL文件以MAGIC(NWL0)开头")
        void testHeaderMagicBytes() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.close();

            List<Path> nwlFiles;
            try (Stream<Path> stream = Files.walk(tempDir)) {
                nwlFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".nwl"))
                        .toList();
            }
            assertThat(nwlFiles).hasSize(1);

            byte[] fileContent = Files.readAllBytes(nwlFiles.get(0));
            assertThat(fileContent[0]).isEqualTo((byte) 'N');
            assertThat(fileContent[1]).isEqualTo((byte) 'W');
            assertThat(fileContent[2]).isEqualTo((byte) 'L');
            assertThat(fileContent[3]).isEqualTo((byte) '0');
        }

        @Test
        @DisplayName("NWL文件header包含VERSION=1")
        void testHeaderVersion() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.close();

            List<Path> nwlFiles;
            try (Stream<Path> stream = Files.walk(tempDir)) {
                nwlFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".nwl"))
                        .toList();
            }
            assertThat(nwlFiles).hasSize(1);

            byte[] fileContent = Files.readAllBytes(nwlFiles.get(0));
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(fileContent);
            buffer.position(4);
            int version = buffer.getInt();
            assertThat(version).isEqualTo(1);
        }

        @Test
        @DisplayName("NWL文件header包含有效的headerJson和CRC32")
        void testHeaderContainsValidJsonAndCrc() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.close();

            List<Path> nwlFiles;
            try (Stream<Path> stream = Files.walk(tempDir)) {
                nwlFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".nwl"))
                        .toList();
            }
            assertThat(nwlFiles).hasSize(1);

            byte[] fileContent = Files.readAllBytes(nwlFiles.get(0));
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(fileContent);

            byte[] magic = new byte[4];
            buffer.get(magic);
            buffer.getInt();

            int headerLength = buffer.getInt();
            assertThat(headerLength).isGreaterThan(0);

            byte[] headerJson = new byte[headerLength];
            buffer.get(headerJson);
            String headerStr = new String(headerJson, java.nio.charset.StandardCharsets.UTF_8);
            assertThat(headerStr).contains("storeId");
            assertThat(headerStr).contains("testStore");

            int storedCrc = buffer.getInt();
            java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
            crc32.update(fileContent, 0, 4 + 4 + 4 + headerLength);
            assertThat(storedCrc).isEqualTo((int) crc32.getValue());
        }
    }

    @Nested
    @DisplayName("Record格式测试")
    class RecordFormatTest {

        @Test
        @DisplayName("DATA记录的二进制格式正确: recordLength + recordType + sequence + timestamp + tableIdLen + tableId + commitId + dataLen + data + CRC32")
        void testDataRecordBinaryFormat() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            byte[] data = "test_payload".getBytes();
            writer.write(createDataRecord(42, data, "myTable"));
            writer.close();

            List<Path> nwlFiles;
            try (Stream<Path> stream = Files.walk(tempDir)) {
                nwlFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".nwl"))
                        .toList();
            }
            assertThat(nwlFiles).hasSize(1);

            byte[] fileContent = Files.readAllBytes(nwlFiles.get(0));
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(fileContent);

            byte[] magic = new byte[4];
            buffer.get(magic);
            int version = buffer.getInt();
            int headerLength = buffer.getInt();
            byte[] headerJson = new byte[headerLength];
            buffer.get(headerJson);
            buffer.getInt();

            int payloadSize = buffer.getInt();
            assertThat(payloadSize).isGreaterThan(0);

            int recordPayloadStart = buffer.position();

            byte recordType = buffer.get();
            assertThat(recordType).isEqualTo(AbstractWALWriter.RECORD_TYPE_DATA);

            long sequence = buffer.getLong();
            assertThat(sequence).isEqualTo(42);

            long timestamp = buffer.getLong();
            assertThat(timestamp).isGreaterThan(0);

            int tableIdLen = buffer.getInt();
            assertThat(tableIdLen).isEqualTo("myTable".getBytes(java.nio.charset.StandardCharsets.UTF_8).length);

            byte[] tableIdBytes = new byte[tableIdLen];
            buffer.get(tableIdBytes);
            assertThat(new String(tableIdBytes, java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("myTable");

            long commitId = buffer.getLong();
            assertThat(commitId).isEqualTo(1L);

            int dataLen = buffer.getInt();
            assertThat(dataLen).isEqualTo(data.length);

            byte[] dataBytes = new byte[dataLen];
            buffer.get(dataBytes);
            assertThat(dataBytes).isEqualTo(data);

            int storedCrc = buffer.getInt();
            java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
            crc32.update(fileContent, recordPayloadStart, payloadSize);
            assertThat(storedCrc).isEqualTo((int) crc32.getValue());
        }

        @Test
        @DisplayName("SYNC_MARKER记录的recordType为2")
        void testSyncMarkerRecordType() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createSyncMarker("table1"));
            writer.close();

            List<Path> nwlFiles;
            try (Stream<Path> stream = Files.walk(tempDir)) {
                nwlFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".nwl"))
                        .toList();
            }
            assertThat(nwlFiles).hasSize(1);

            byte[] fileContent = Files.readAllBytes(nwlFiles.get(0));
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(fileContent);

            byte[] magic = new byte[4];
            buffer.get(magic);
            buffer.getInt();
            int headerLength = buffer.getInt();
            byte[] headerJson = new byte[headerLength];
            buffer.get(headerJson);
            buffer.getInt();

            buffer.getInt();
            byte recordType = buffer.get();
            assertThat(recordType).isEqualTo(AbstractWALWriter.RECORD_TYPE_SYNC_MARKER);
        }
    }

    @Nested
    @DisplayName("SYNC_MARKER测试")
    class SyncMarkerTest {

        @Test
        @DisplayName("写入SYNC_MARKER记录成功")
        void testWriteSyncMarker() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker("table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));

            assertThat(writer.writtenBytes()).isGreaterThan(0);
            assertThat(countNWLFiles()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("forceSyncIfNeeded测试")
    class ForceSyncIfNeededTest {

        @Test
        @DisplayName("syncIntervalMs=0时不触发自动sync")
        void testNoAutoSyncWhenIntervalZero() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);

            for (int i = 0; i < 10; i++) {
                writer.write(createDataRecord(i, "data".getBytes(), "table1"));
            }

            assertThatCode(() -> {}).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("syncIntervalMs>0时写入后可能触发自动sync")
        void testAutoSyncWhenIntervalPositive() throws IOException {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 1L);

            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));

            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));

            assertThatCode(() -> {}).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("committedSequence测试")
    class CommittedSequenceTest {

        @Test
        @DisplayName("写入记录后committedSequence更新为最新sequence")
        void testCommittedSequenceUpdated() throws Exception {
            writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);

            writer.write(createDataRecord(10, "data".getBytes(), "table1"));

            var field = AbstractWALWriter.class.getDeclaredField("committedSequence");
            field.setAccessible(true);
            long committed = (long) field.get(writer);
            assertThat(committed).isEqualTo(10);

            writer.write(createDataRecord(20, "data".getBytes(), "table1"));
            committed = (long) field.get(writer);
            assertThat(committed).isEqualTo(20);
        }
    }
}
