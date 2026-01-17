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
package io.github.ximin.xlake.backend.spark;

import io.github.ximin.xlake.backend.DynamicMmapStore;
import io.github.ximin.xlake.backend.wal.DefaultWALRecord;
import io.github.ximin.xlake.backend.wal.WAL;
import io.github.ximin.xlake.backend.writer.AbstractDataWriter;
import io.github.ximin.xlake.backend.writer.MemoryWriter;
import io.github.ximin.xlake.common.Config;
import io.github.ximin.xlake.common.SingletonContainer;
import io.github.ximin.xlake.table.DataBlock;
import io.github.ximin.xlake.table.schema.Schema;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SparkMmapDataWriter extends AbstractDataWriter<InternalRow> implements MemoryWriter<InternalRow> {
    private final Schema schema;
    private final Config.WriteConf writeConf;
    private final DynamicMmapStore memStore;
    private long recordsWritten = 0;
    private final String uniqTableIdentifier;
    private final long commitId;
    private WAL wal;
    private final AtomicLong sequence = new AtomicLong(0);

    public SparkMmapDataWriter(Schema schema,
                               Config.WriteConf writeConf,
                               long commitId,
                               String uniqTableIdentifier) {
        this.schema = schema;
        this.writeConf = writeConf;
        this.memStore = SingletonContainer.getInstance(DynamicMmapStore.class, writeConf.getMmapPath(), writeConf.getMmapSize());
        this.commitId = commitId;
        this.uniqTableIdentifier = uniqTableIdentifier;

    }

    @Override
    public void init(Properties props) {
        this.wal = new WAL(100 * 1024, 256 * 1024 * 1024);
    }

    @Override
    public long recordsWritten() {
        return recordsWritten;
    }

    @Override
    public void write(InternalRow internalRow) {
        byte[] key = internalRow.getString(0).getBytes(StandardCharsets.UTF_8);
        byte[] value = internalRow.getString(1).getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(key.length + value.length);
        buffer.put(key);
        buffer.put(value);
        // 根据配置决定，如果不开启mem store和wal，直接写数据文件
        long walSeq = wal.append(new DefaultWALRecord(
                sequence.incrementAndGet(),
                buffer.array(),
                System.nanoTime(),
                commitId,
                uniqTableIdentifier, ""), false);
        if (walSeq != -1) {
            memStore.put(uniqTableIdentifier, key, value);
            recordsWritten++;
        } else {
            log.error("wal seq is -1");
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        List<DataBlock<String>> state = memStore.currentState().commitState(commitId);
        return new SparkCommitMessage(commitId, state);
    }

    @Override
    public void abort() throws IOException {
        // 写用于回滚的逻辑，需要知道commitId，也就是本次提交的是哪个事务，要标记哪些lmdb或者文件是无效的
        // 提交的meta store，标记该commit id是失败的，每次读的时候，都要拿着失败的commit id，对数据进行过滤
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void checkIfReadyToWrite() {
        // 两种计算是否可写的方式，取舍下
        //boolean factor = (double) preEntryTotalSize / totalMappedMemorySize <= 0.9;
        // 这种写法得考虑性能，每次写的时候都统计一遍，是不是有点费
        // todo 开发memstorespace monitor
        // spaceMonitor.refresh(avgEntrySize);
        //boolean factor = spaceMonitor.getSpaceInfo().hasSpace();
        boolean factor = false;
        if (!factor) {
            // make active immutable
            // check whether current executor total memory meets the requirements
            // if not, force flush
            // other ways to ensure write ability,including compact, archive(move to other file system)
            // if true open new active db to write
        }
    }

}
