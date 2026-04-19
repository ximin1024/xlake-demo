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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("WALReader - WAL读取器测试")
class WALReaderTest {

    @TempDir
    Path tempDir;

    private String walDir;
    private Configuration hadoopConfig;

    @BeforeEach
    void setUp() {
        walDir = tempDir.resolve("wal").toString();
        hadoopConfig = new Configuration();
    }

    private DefaultWALRecord createDataRecord(long sequence, byte[] data, String tableId) {
        return new DefaultWALRecord(sequence, data, System.nanoTime(), 1L, tableId, "DATA");
    }

    private DefaultWALRecord createSyncMarker(long sequence, String tableId) {
        return new DefaultWALRecord(sequence, new byte[0], System.nanoTime(), -1L, tableId, "SYNC_MARKER");
    }

    private void writeNWLFile(LocalWALWriter writer, List<WALRecord> records) throws IOException {
        for (WALRecord record : records) {
            writer.write(record);
        }
        writer.close();
    }

    @Nested
    @DisplayName("Write-then-Read往返测试")
    class RoundTripTest {

        @Test
        @DisplayName("写入NWL文件后WALReader能读回所有记录")
        void testReadNWLFileWrittenByLocalWALWriter() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(3);
            assertThat(records.get(0).sequence()).isEqualTo(1);
            assertThat(records.get(1).sequence()).isEqualTo(2);
            assertThat(records.get(2).sequence()).isEqualTo(3);
        }

        @Test
        @DisplayName("读回的记录数据内容正确")
        void testReadBackDataContent() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            byte[] data1 = "hello_world".getBytes();
            byte[] data2 = "another_record".getBytes();
            writer.write(createDataRecord(1, data1, "table1"));
            writer.write(createDataRecord(2, data2, "table2"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(2);
            assertThat(records.get(0).value()).isEqualTo(data1);
            assertThat(records.get(0).uniqTableIdentifier()).isEqualTo("table1");
            assertThat(records.get(1).value()).isEqualTo(data2);
            assertThat(records.get(1).uniqTableIdentifier()).isEqualTo("table2");
        }

        @Test
        @DisplayName("读回的commitId正确")
        void testReadBackCommitId() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            DefaultWALRecord record = new DefaultWALRecord(1, "data".getBytes(), System.nanoTime(), 42L, "table1", "DATA");
            writer.write(record);
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(1);
            assertThat(records.get(0).commitId()).isEqualTo(42L);
        }
    }

    @Nested
    @DisplayName("多NWL文件读取测试")
    class MultipleNWLFilesTest {

        @Test
        @DisplayName("读取多个NWL文件")
        void testReadMultipleNWLFiles() throws IOException {
            LocalWALWriter writer1 = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer1.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer1.close();

            LocalWALWriter writer2 = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer2.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer2.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(2);
        }
    }

    @Nested
    @DisplayName("filterUnflushed测试")
    class FilterUnflushedTest {

        @Test
        @DisplayName("无SYNC_MARKER时返回所有DATA记录")
        void testFilterUnflushedWithoutSyncMarker() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).hasSize(2);
        }

        @Test
        @DisplayName("有SYNC_MARKER时返回最后SYNC_MARKER之后的记录")
        void testFilterUnflushedWithSyncMarker() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.write(createSyncMarker(2, "table1"));
            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            writer.write(createDataRecord(4, "data4".getBytes(), "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).hasSize(2);
            assertThat(unflushed.get(0).sequence()).isEqualTo(3);
            assertThat(unflushed.get(1).sequence()).isEqualTo(4);
        }

        @Test
        @DisplayName("SYNC_MARKER是最后一条记录时返回空列表")
        void testFilterUnflushedWhenSyncMarkerIsLast() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker(1, "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).isEmpty();
        }

        @Test
        @DisplayName("多个SYNC_MARKER时以最后一个为准")
        void testFilterUnflushedWithMultipleSyncMarkers() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker(1, "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.write(createSyncMarker(2, "table1"));
            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).hasSize(1);
            assertThat(unflushed.get(0).sequence()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("空WAL目录测试")
    class EmptyWALDirectoryTest {

        @Test
        @DisplayName("读取不存在的目录返回空列表")
        void testReadNonExistentDirectory() throws IOException {
            String nonExistent = tempDir.resolve("non_existent").toString();
            WALReader reader = new WALReader(nonExistent, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }

        @Test
        @DisplayName("读取空目录返回空列表")
        void testReadEmptyDirectory() throws IOException {
            Path emptyDir = tempDir.resolve("empty_wal");
            Files.createDirectories(emptyDir);

            WALReader reader = new WALReader(emptyDir.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }
    }

    @Nested
    @DisplayName("损坏NWL文件测试")
    class CorruptedNWLFileTest {

        @Test
        @DisplayName("MAGIC不匹配的NWL文件被跳过")
        void testBadMagicSkipped() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            ByteBuffer buffer = ByteBuffer.allocate(64);
            buffer.put("BAD0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(1);
            buffer.putInt(0);
            buffer.putInt(0);
            Files.write(badFile, buffer.array());

            WALReader reader = new WALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }

        @Test
        @DisplayName("VERSION不匹配的NWL文件被跳过")
        void testBadVersionSkipped() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            ByteBuffer buffer = ByteBuffer.allocate(64);
            buffer.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(999);
            buffer.putInt(0);
            buffer.putInt(0);
            Files.write(badFile, buffer.array());

            WALReader reader = new WALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }

        @Test
        @DisplayName("Header CRC不匹配的NWL文件被跳过")
        void testBadHeaderCrcSkipped() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            String headerJson = "{\"storeId\":\"test\",\"executorId\":\"local\",\"createdAt\":1000,\"compression\":\"NONE\"}";
            byte[] headerJsonBytes = headerJson.getBytes(StandardCharsets.UTF_8);

            int totalSize = 4 + 4 + 4 + headerJsonBytes.length + 4;
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            buffer.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(1);
            buffer.putInt(headerJsonBytes.length);
            buffer.put(headerJsonBytes);
            buffer.putInt(0xDEADBEEF);

            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            Files.write(badFile, buffer.array());

            WALReader reader = new WALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }

        @Test
        @DisplayName("Record CRC不匹配的记录被跳过")
        void testBadRecordCrcSkipped() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            String headerJson = "{\"storeId\":\"test\",\"executorId\":\"local\",\"createdAt\":1000,\"compression\":\"NONE\"}";
            byte[] headerJsonBytes = headerJson.getBytes(StandardCharsets.UTF_8);

            ByteBuffer headerBuf = ByteBuffer.allocate(4 + 4 + 4 + headerJsonBytes.length + 4);
            headerBuf.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            headerBuf.putInt(1);
            headerBuf.putInt(headerJsonBytes.length);
            headerBuf.put(headerJsonBytes);

            CRC32 headerCrc = new CRC32();
            headerCrc.update(headerBuf.array(), 0, headerBuf.position());
            headerBuf.putInt((int) headerCrc.getValue());

            byte[] payload = new byte[1 + 8 + 8 + 4 + 0 + 8 + 4 + 0];
            ByteBuffer payloadBuf = ByteBuffer.wrap(payload);
            payloadBuf.put((byte) 1);
            payloadBuf.putLong(1L);
            payloadBuf.putLong(System.nanoTime());
            payloadBuf.putInt(0);
            payloadBuf.putLong(1L);
            payloadBuf.putInt(0);

            ByteBuffer recordBuf = ByteBuffer.allocate(4 + payload.length + 4);
            recordBuf.putInt(payload.length);
            recordBuf.put(payload);
            recordBuf.putInt(0xBADBAD);

            ByteBuffer fileBuf = ByteBuffer.allocate(headerBuf.position() + recordBuf.position());
            fileBuf.put(headerBuf.array(), 0, headerBuf.position());
            fileBuf.put(recordBuf.array(), 0, recordBuf.position());

            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            Files.write(badFile, fileBuf.array());

            WALReader reader = new WALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }
    }

    @Nested
    @DisplayName("混合格式文件测试")
    class MixedFormatTest {

        @Test
        @DisplayName("目录中非NWL/Parquet文件被忽略")
        void testNonWalFilesIgnored() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            Path txtFile = walPath.resolve("readme.txt");
            Files.writeString(txtFile, "not a wal file");

            Path jsonFile = walPath.resolve("config.json");
            Files.writeString(jsonFile, "{}");

            WALReader reader = new WALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }
    }

    @Nested
    @DisplayName("文件排序测试")
    class FileOrderingTest {

        @Test
        @DisplayName("NWL文件按sequence number排序")
        void testFilesSortedBySequenceNumber() throws IOException {
            long maxSize = 256L;
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, maxSize, 0L);

            byte[] data = new byte[100];
            java.util.Arrays.fill(data, (byte) 'A');

            writer.write(createDataRecord(1, data, "table1"));
            writer.write(createDataRecord(2, data, "table1"));
            writer.write(createDataRecord(3, data, "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(3);
            assertThat(records.get(0).sequence()).isEqualTo(1);
            assertThat(records.get(1).sequence()).isEqualTo(2);
            assertThat(records.get(2).sequence()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Header CRC验证测试")
    class HeaderCrcValidationTest {

        @Test
        @DisplayName("正确的Header CRC通过验证")
        void testValidHeaderCrcPasses() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.close();

            WALReader reader = new WALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("close测试")
    class CloseTest {

        @Test
        @DisplayName("close()安全关闭")
        void testCloseIsSafe() throws IOException {
            WALReader reader = new WALReader(walDir, hadoopConfig);
            reader.close();
            reader.close();
        }
    }
}
