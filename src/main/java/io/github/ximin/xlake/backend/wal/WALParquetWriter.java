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

import io.github.ximin.xlake.backend.writer.DefaultParquetWriterBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

@Slf4j
public class WALParquetWriter implements Closeable {
    private final String basePath;
    private final Configuration hadoopConfig;
    private final long maxLogSize;
    private final long syncIntervalMs;
    private final ReentrantLock writeLock = new ReentrantLock();

    private ParquetWriter<Group> currentWriter;
    private final SimpleGroupFactory groupFactory;
    private final AtomicLong recordCount = new AtomicLong(0);
    private final AtomicLong totalRecordSize = new AtomicLong(0);
    private final GroupWriteSupport writeSupport;

    private long lastSyncTime = System.currentTimeMillis();
    private volatile long committedSequence = -1;

    private static final MessageType SCHEMA = Types.buildMessage()
            .required(INT64).named("sequence")
            .required(INT64).named("timestamp")
            .required(BINARY).named("data")
            .required(INT32).named("dataLength")
            .required(BINARY).as(UTF8).named("commitId")
            .optional(BINARY).as(UTF8).named("uniqTableIdentifier")
            .required(INT64).named("checksum")
            .required(INT64).named("writeTimestamp")
            .named("wal_record");

    public WALParquetWriter(String basePath, Configuration hadoopConfig, long maxLogSize, long syncIntervalMs) {
        this.basePath = basePath;
        this.hadoopConfig = hadoopConfig;
        this.maxLogSize = maxLogSize;
        this.syncIntervalMs = syncIntervalMs;
        this.groupFactory = new SimpleGroupFactory(SCHEMA);

        GroupWriteSupport.setSchema(SCHEMA, hadoopConfig);
        this.writeSupport = new GroupWriteSupport();

        hadoopConfig.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        hadoopConfig.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        hadoopConfig.setBoolean("parquet.enable.dictionary", true);
        hadoopConfig.setInt("parquet.page.size", 64 * 1024); // 64KB pages
        hadoopConfig.setInt("parquet.block.size", 256 * 1024 * 1024); // 256MB row groups
    }

    public void write(WALRecord entry) throws IOException {
        writeLock.lock();
        try {

            if (currentWriter == null || shouldRollFile()) {
                createNewFile();
            }

            Group group = createGroup(entry);
            currentWriter.write(group);

            recordCount.incrementAndGet();
            totalRecordSize.addAndGet(entry.length());
            committedSequence = entry.sequence();

            // 强制同步检查
            forceSyncIfNeeded();

        } finally {
            writeLock.unlock();
        }
    }

    private Group createGroup(WALRecord entry) {
        return groupFactory.newGroup()
                .append("sequence", entry.sequence())
                .append("timestamp", entry.timestamp())
                .append("data", String.valueOf(ByteBuffer.wrap(entry.value(), 0, entry.value().length)))
                .append("dataLength", entry.length())
                .append("writeTimestamp", System.currentTimeMillis());
    }

    // todo 改成size判断
    private boolean shouldRollFile() {
        return recordCount.get() >= maxLogSize;
    }

    private void createNewFile() throws IOException {
        if (currentWriter != null) {
            forceSync();
            currentWriter.close();
            log.info("Rolled to new Parquet file, records: {}", recordCount.get());
        }

        String filePath = generateFilePath();
        Path hdfsPath = new Path(filePath);

        // 确保目录存在
        FileSystem fs = hdfsPath.getFileSystem(hadoopConfig);
        fs.mkdirs(hdfsPath.getParent());

        currentWriter = new DefaultParquetWriterBuilder<>(hdfsPath, writeSupport)
                .withConf(hadoopConfig)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withValidation(true)
                .withDictionaryEncoding(true)
                .withPageSize(64 * 1024)
                .withRowGroupSize(256 * 1024 * 1024L)
                .build();

        // todo 统计信息上报
        recordCount.set(0);
        totalRecordSize.set(0);
        lastSyncTime = System.currentTimeMillis();

        log.info("Created new Parquet file: {}", filePath);
    }

    private String generateFilePath() {
        long timestamp = System.currentTimeMillis();
        return String.format("%s/wal_%d_%d.parquet", basePath, timestamp, System.nanoTime() % 1000000);
    }

    private void forceSyncIfNeeded() throws IOException {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastSyncTime >= syncIntervalMs) {
            forceSync();
            lastSyncTime = currentTime;
        }
    }

    public void forceSync() throws IOException {
        writeLock.lock();
        try {
            if (currentWriter != null) {
                // ParquetWriter本身不提供flush方法，我们通过关闭并重新创建来实现
                // 在实际生产环境中，可以考虑使用更底层的API
                String currentFile = filePath();
                currentWriter.close();

                // 重新打开文件继续写入（如果需要）
                if (recordCount.get() > 0) {
                    currentWriter = new DefaultParquetWriterBuilder<>(
                            new Path(currentFile), new GroupWriteSupport())
                            .withConf(hadoopConfig)
                            .withWriteMode(ParquetFileWriter.Mode.CREATE)
                            .build();
                }

                log.debug("Force sync completed, sequence: {}", committedSequence);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private String filePath() {
        return basePath + "/" + committedSequence;
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (currentWriter != null) {
                forceSync();
                currentWriter.close();
                currentWriter = null;
            }
            log.info("WALParquetWriter closed");
        } finally {
            writeLock.unlock();
        }
    }
}
