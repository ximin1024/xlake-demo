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

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

@Slf4j
public class WALReader implements Closeable {

    private static final byte[] EXPECTED_MAGIC = AbstractWALWriter.MAGIC;
    private static final int EXPECTED_VERSION = AbstractWALWriter.VERSION;
    private static final Pattern NWL_FILE_PATTERN = Pattern.compile("wal_([^_]+)_(\\d+)_(\\d+)\\.nwl");
    private static final Pattern PARQUET_FILE_PATTERN = Pattern.compile("wal_(\\d+)_(\\d+)\\.parquet");
    private static final byte RECORD_TYPE_DATA = 1;
    private static final byte RECORD_TYPE_SYNC_MARKER = 2;

    private final String basePath;
    private final Configuration hadoopConfig;

    public WALReader(String basePath, Configuration hadoopConfig) {
        this.basePath = basePath;
        this.hadoopConfig = hadoopConfig;
    }

    public List<WALRecord> readAll() throws IOException {
        List<WALRecord> records = new ArrayList<>();

        boolean isHdfs = isHdfsPath(basePath);

        if (isHdfs) {
            records.addAll(readFromHdfs(basePath));
        } else {
            records.addAll(readFromLocal(basePath));
        }

        return records;
    }

    public List<WALRecord> readUnflushed() throws IOException {
        List<WALRecord> allRecords = readAll();
        return filterUnflushed(allRecords);
    }

    private List<WALRecord> filterUnflushed(List<WALRecord> allRecords) {
        long lastSyncSequence = -1;
        for (WALRecord record : allRecords) {
            if ("SYNC_MARKER".equals(record.marker())) {
                lastSyncSequence = record.sequence();
            }
        }

        if (lastSyncSequence == -1) {
            return allRecords.stream()
                    .filter(r -> !"SYNC_MARKER".equals(r.marker()))
                    .toList();
        }

        final long syncSeq = lastSyncSequence;
        return allRecords.stream()
                .filter(r -> r.sequence() > syncSeq)
                .filter(r -> !"SYNC_MARKER".equals(r.marker()))
                .toList();
    }

    private List<WALRecord> readFromHdfs(String hdfsPath) throws IOException {
        List<WALRecord> records = new ArrayList<>();
        Path path = new Path(hdfsPath);
        FileSystem fs = path.getFileSystem(hadoopConfig);

        if (!fs.exists(path)) {
            log.info("WAL HDFS path does not exist: {}", hdfsPath);
            return records;
        }

        List<Path> walFiles = listFilesRecursively(fs, path);

        walFiles.sort(this::compareWALFiles);

        for (Path walFile : walFiles) {
            records.addAll(readSingleFile(walFile, fs));
        }

        return records;
    }

    private List<Path> listFilesRecursively(FileSystem fs, Path path) throws IOException {
        List<Path> result = new ArrayList<>();
        if (!fs.exists(path)) {
            return result;
        }
        FileStatus[] statuses = fs.listStatus(path);
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                result.addAll(listFilesRecursively(fs, status.getPath()));
            } else {
                String name = status.getPath().getName();
                if (name.endsWith(".nwl") || name.endsWith(".parquet")) {
                    result.add(status.getPath());
                }
            }
        }
        return result;
    }

    private List<WALRecord> readFromLocal(String localPath) throws IOException {
        List<WALRecord> records = new ArrayList<>();
        java.nio.file.Path dir = Paths.get(localPath);

        if (!Files.exists(dir)) {
            log.info("WAL local path does not exist: {}", localPath);
            return records;
        }

        List<java.nio.file.Path> walFiles = new ArrayList<>();
        try (var stream = Files.walk(dir)) {
            stream.filter(Files::isRegularFile)
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.endsWith(".nwl") || name.endsWith(".parquet");
                    })
                    .forEach(walFiles::add);
        }

        walFiles.sort(this::compareLocalWALFiles);

        for (java.nio.file.Path walFile : walFiles) {
            String name = walFile.getFileName().toString();
            if (name.endsWith(".nwl")) {
                records.addAll(readNWLFileLocal(walFile));
            } else if (name.endsWith(".parquet")) {
                records.addAll(readSingleParquetFileLocal(walFile));
            }
        }

        return records;
    }

    private List<WALRecord> readSingleFile(Path walFile, FileSystem fs) throws IOException {
        String name = walFile.getName();
        if (name.endsWith(".nwl")) {
            return readNWLFileHdfs(walFile, fs);
        } else if (name.endsWith(".parquet")) {
            return readSingleParquetFileHdfs(walFile, fs);
        }
        return new ArrayList<>();
    }

    private List<WALRecord> readNWLFileHdfs(Path hdfsPath, FileSystem fs) throws IOException {
        List<WALRecord> records = new ArrayList<>();
        try (var in = fs.open(hdfsPath)) {
            byte[] headerPrefix = new byte[AbstractWALWriter.HEADER_PREFIX_SIZE];
            in.readFully(headerPrefix);

            ByteBuffer prefixBuf = ByteBuffer.wrap(headerPrefix);
            byte[] magic = new byte[4];
            prefixBuf.get(magic);
            if (!java.util.Arrays.equals(magic, EXPECTED_MAGIC)) {
                log.warn("Invalid magic in NWL file: {}", hdfsPath);
                return records;
            }

            int version = prefixBuf.getInt();
            if (version != EXPECTED_VERSION) {
                log.warn("Unsupported NWL version {} in file: {}, skipping", version, hdfsPath);
                return records;
            }

            int headerLength = prefixBuf.getInt();
            if (headerLength < 0 || headerLength >= 1024 * 1024) {
                log.warn("Invalid header length {} in NWL file: {}, skipping", headerLength, hdfsPath);
                return records;
            }

            byte[] headerJson = new byte[headerLength];
            if (headerLength > 0) {
                in.readFully(headerJson);
            }

            int storedHeaderCrc = in.readInt();
            CRC32 crc32 = new CRC32();
            crc32.update(headerPrefix);
            if (headerLength > 0) {
                crc32.update(headerJson);
            }
            int computedHeaderCrc = (int) crc32.getValue();
            if (storedHeaderCrc != computedHeaderCrc) {
                log.warn("Header CRC mismatch in NWL file: {}, skipping", hdfsPath);
                return records;
            }

            while (true) {
                try {
                    int payloadSize = in.readInt();
                    if (payloadSize <= 0 || payloadSize > 256 * 1024 * 1024) {
                        break;
                    }

                    byte[] payload = new byte[payloadSize];
                    in.readFully(payload);

                    int storedCrc = in.readInt();

                    CRC32 recordCrc = new CRC32();
                    recordCrc.update(payload);
                    int computedCrc = (int) recordCrc.getValue();

                    if (storedCrc != computedCrc) {
                        log.warn("CRC32 mismatch in NWL file: {}, skipping record", hdfsPath);
                        continue;
                    }

                    WALRecord record = deserializeRecord(payload);
                    if (record != null) {
                        records.add(record);
                    }
                } catch (EOFException e) {
                    break;
                } catch (Exception e) {
                    log.warn("Error reading record from NWL file: {}, skipping", hdfsPath, e);
                    break;
                }
            }
        }
        return records;
    }

    private List<WALRecord> readNWLFileLocal(java.nio.file.Path localPath) throws IOException {
        List<WALRecord> records = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(localPath.toFile(), "r");
             var channel = raf.getChannel()) {

            ByteBuffer headerPrefixBuf = ByteBuffer.allocate(AbstractWALWriter.HEADER_PREFIX_SIZE);
            readFully(channel, headerPrefixBuf);
            headerPrefixBuf.flip();

            byte[] headerPrefix = headerPrefixBuf.array();
            ByteBuffer prefixView = ByteBuffer.wrap(headerPrefix);
            byte[] magic = new byte[4];
            prefixView.get(magic);
            if (!java.util.Arrays.equals(magic, EXPECTED_MAGIC)) {
                log.warn("Invalid magic in NWL file: {}", localPath);
                return records;
            }

            int version = prefixView.getInt();
            if (version != EXPECTED_VERSION) {
                log.warn("Unsupported NWL version {} in file: {}, skipping", version, localPath);
                return records;
            }

            int headerLength = prefixView.getInt();
            if (headerLength < 0 || headerLength >= 1024 * 1024) {
                log.warn("Invalid header length {} in NWL file: {}, skipping", headerLength, localPath);
                return records;
            }

            ByteBuffer headerJsonBuf = ByteBuffer.allocate(headerLength);
            readFully(channel, headerJsonBuf);
            headerJsonBuf.flip();

            ByteBuffer headerCrcBuf = ByteBuffer.allocate(4);
            readFully(channel, headerCrcBuf);
            headerCrcBuf.flip();
            int storedHeaderCrc = headerCrcBuf.getInt();

            CRC32 crc32 = new CRC32();
            crc32.update(headerPrefix);
            if (headerLength > 0) {
                crc32.update(headerJsonBuf.array(), 0, headerLength);
            }
            int computedHeaderCrc = (int) crc32.getValue();
            if (storedHeaderCrc != computedHeaderCrc) {
                log.warn("Header CRC mismatch in NWL file: {}, skipping", localPath);
                return records;
            }

            while (true) {
                try {
                    ByteBuffer sizeBuf = ByteBuffer.allocate(4);
                    int read = readFully(channel, sizeBuf);
                    if (read < 4) {
                        break;
                    }
                    sizeBuf.flip();
                    int payloadSize = sizeBuf.getInt();
                    if (payloadSize <= 0 || payloadSize > 256 * 1024 * 1024) {
                        break;
                    }

                    ByteBuffer payloadBuf = ByteBuffer.allocate(payloadSize + 4);
                    read = readFully(channel, payloadBuf);
                    if (read < payloadSize + 4) {
                        break;
                    }
                    payloadBuf.flip();

                    byte[] payload = new byte[payloadSize];
                    payloadBuf.get(payload);
                    int storedCrc = payloadBuf.getInt();

                    CRC32 recordCrc = new CRC32();
                    recordCrc.update(payload);
                    int computedCrc = (int) recordCrc.getValue();

                    if (storedCrc != computedCrc) {
                        log.warn("CRC32 mismatch in NWL file: {}, skipping record", localPath);
                        continue;
                    }

                    WALRecord record = deserializeRecord(payload);
                    if (record != null) {
                        records.add(record);
                    }
                } catch (Exception e) {
                    log.warn("Error reading record from NWL file: {}, stopping", localPath, e);
                    break;
                }
            }
        }
        return records;
    }

    private static int readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                return totalRead;
            }
            totalRead += read;
        }
        return totalRead;
    }

    private List<WALRecord> readSingleParquetFileHdfs(Path hdfsPath, FileSystem fs) throws IOException {
        List<WALRecord> records = new ArrayList<>();
        try (var reader = AvroParquetReader.<GenericRecord>builder(hdfsPath)
                .withConf(hadoopConfig)
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                WALRecord walRecord = genericRecordToWALRecord(record);
                if (walRecord != null) {
                    records.add(walRecord);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read Parquet WAL file: {}", hdfsPath, e);
        }
        return records;
    }

    private List<WALRecord> readSingleParquetFileLocal(java.nio.file.Path localPath) throws IOException {
        List<WALRecord> records = new ArrayList<>();
        try {
            Path hdfsPath = new Path(localPath.toUri());
            try (var reader = AvroParquetReader.<GenericRecord>builder(hdfsPath)
                    .withConf(hadoopConfig)
                    .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    WALRecord walRecord = genericRecordToWALRecord(record);
                    if (walRecord != null) {
                        records.add(walRecord);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read Parquet WAL file: {}", localPath, e);
        }
        return records;
    }

    private WALRecord genericRecordToWALRecord(GenericRecord record) {
        long sequence = (Long) record.get("sequence");
        long timestamp = (Long) record.get("timestamp");
        byte[] data = toBytes(record.get("data"));
        int dataLength = (Integer) record.get("dataLength");

        long commitId = -1L;
        String uniqTableIdentifier = "";

        String marker = (dataLength == 0) ? "SYNC_MARKER" : "DATA";

        return new DefaultWALRecord(
                sequence,
                dataLength > 0 ? data : new byte[0],
                timestamp,
                commitId,
                uniqTableIdentifier,
                marker
        );
    }

    private byte[] toBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer byteBuffer) {
            ByteBuffer duplicate = byteBuffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }
        if (value instanceof CharSequence sequence) {
            return sequence.toString().getBytes();
        }
        return value.toString().getBytes();
    }

    private WALRecord deserializeRecord(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        byte recordType = buffer.get();
        long sequence = buffer.getLong();
        long timestamp = buffer.getLong();

        int tableIdLength = buffer.getInt();
        if (tableIdLength < 0 || tableIdLength > buffer.remaining()) {
            log.warn("Invalid tableId length: {}", tableIdLength);
            return null;
        }
        byte[] tableIdBytes = new byte[tableIdLength];
        buffer.get(tableIdBytes);
        String tableId = new String(tableIdBytes, StandardCharsets.UTF_8);

        long commitId = buffer.getLong();

        int dataLength = buffer.getInt();
        if (dataLength < 0 || dataLength > buffer.remaining()) {
            log.warn("Invalid data length: {}", dataLength);
            return null;
        }
        byte[] data = new byte[dataLength];
        buffer.get(data);

        String marker = (recordType == RECORD_TYPE_SYNC_MARKER) ? "SYNC_MARKER" : "DATA";

        return new DefaultWALRecord(sequence, data, timestamp, commitId, tableId, marker);
    }

    private boolean isHdfsPath(String path) {
        return path != null && (path.startsWith("hdfs://") || path.startsWith("viewfs://"));
    }

    private int compareWALFiles(Path p1, Path p2) {
        return Long.compare(extractSequence(p1.getName()), extractSequence(p2.getName()));
    }

    private int compareLocalWALFiles(java.nio.file.Path p1, java.nio.file.Path p2) {
        return Long.compare(extractSequence(p1.getFileName().toString()),
                extractSequence(p2.getFileName().toString()));
    }

    private long extractSequence(String fileName) {
        Matcher nwlMatcher = NWL_FILE_PATTERN.matcher(fileName);
        if (nwlMatcher.matches()) {
            try {
                return Long.parseLong(nwlMatcher.group(3));
            } catch (NumberFormatException e) {
                return extractTimestampFallback(fileName);
            }
        }
        return extractTimestampFallback(fileName);
    }

    private long extractTimestampFallback(String fileName) {
        Matcher parquetMatcher = PARQUET_FILE_PATTERN.matcher(fileName);
        if (parquetMatcher.matches()) {
            try {
                return Long.parseLong(parquetMatcher.group(1));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    @Override
    public void close() throws IOException {
        log.debug("WALReader closed");
    }
}
