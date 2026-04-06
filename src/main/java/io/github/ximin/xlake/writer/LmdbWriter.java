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
package io.github.ximin.xlake.writer;

import io.github.ximin.xlake.table.KvRecord;
import io.github.ximin.xlake.backend.LmdbInstance;
import io.github.ximin.xlake.table.op.Write;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

@Slf4j
public class LmdbWriter implements Writer<KvRecord> {

    public final static String MAPFULL_MESSAGE = "LMDB Map Full";
    public final static String READONLY_MESSAGE = "Instance is read-only";

    private final LmdbInstance instance;
    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> dbi;

    private Properties config;
    private long writtenCount = 0;
    private long failedCount = 0;
    private volatile boolean closed = false;

    private List<KvRecord> buffer;
    private int batchSize;
    private boolean useBuffer;

    // 死信处理
    private final DeadLetterHandler deadLetterHandler;

    public LmdbWriter(LmdbInstance instance) {
        this.instance = instance;
        this.env = instance.getEnv();
        this.dbi = instance.getDbi();
        this.deadLetterHandler = new HdfsDeadLetterHandler("hdfs://localhost:9000", "/lmdb/dead-letter");
    }

    @Override
    public void init(Properties config) {
        if (this.instance == null) {
            throw new IllegalStateException("LmdbInstance not initialized");
        }
        this.config = config;
        int confBatchSize = (int) config.getOrDefault("writer_batch_size", 0);
        // 默认 1000 条刷盘一次
        this.batchSize = confBatchSize > 0 ? confBatchSize : 100;
        this.useBuffer = (boolean) config.getOrDefault("writer_use_buffer", false);
        this.buffer = new ArrayList<>(batchSize);
    }

    @Override
    public Write.Result write(KvRecord data) {
        if (closed) {
            return Write.Result.error("Writer is closed");
        }
        if (data == null) {
            return Write.Result.error("Object cannot be null");
        }

        if (useBuffer) {
            try {

                buffer.add(data);

                if (buffer.size() >= batchSize) {
                    flush();
                }

                return Write.Result.ok(1);

            } catch (Exception e) {
                failedCount++;
                return Write.Result.error("Serialization error: " + e.getMessage());
            }
        }

        return doWrite(data);
    }

    @Override
    public Write.Result batchWrite(Collection<KvRecord> objs) {
        // LMDB 批量写入的最佳实践：在一个事务中写入所有数据
        // 注意：LmdbInstance 当前的 put 实现是自动提交事务。
        // 如果 LmdbInstance 没有暴露批量事务接口，这里只能循环调用，效率稍低。
        // 假设我们使用标准的 LmdbInstance.put
        int success = 0;
        int fail = 0;

        for (KvRecord obj : objs) {
            Write.Result res = doWrite(obj);
            if (res.success()) {
                success++;
            } else {
                fail++;
                log.warn("Batch write failed for one record: {}", res.message());
            }
        }

        this.writtenCount += success;
        this.failedCount += fail;

        return new Write.Result(fail == 0, "Batch completed", null, success);
    }

    @Override
    public void flush() {
        if (buffer.isEmpty()) {
            return;
        }

        try (Txn<ByteBuffer> txn = env.txnWrite()) {

            for (KvRecord record : buffer) {
                // Java 21 标准：使用 ByteBuffer.allocateDirect
                // 注意：如果在循环中频繁创建 DirectBuffer 可能会有性能损耗
                // 优化思路：如果 Key/Value 很小，可以复用一个大 Buffer 切片
                ByteBuffer keyBuf = ByteBuffer.allocateDirect(record.key().length);
                keyBuf.put(record.key()).flip();

                ByteBuffer valBuf = ByteBuffer.allocateDirect(record.value().length);
                valBuf.put(record.value()).flip();

                dbi.put(txn, keyBuf, valBuf);
                writtenCount++;
            }

            txn.commit();

            buffer.clear();

        } catch (Env.MapFullException e) {
            failedCount += buffer.size();
            instance.markAsReadOnly();
            throw new RuntimeException("LMDB Map Full, instance switched to read-only.", e);
        } catch (Exception e) {
            // --- 通用异常处理（如 IO 错误、数据损坏等） ---
            failedCount += buffer.size();

            // 物理回滚已自动完成。
            // 此时缓冲区 buffer 仍然持有数据。

            // 3. 关键完善点：缓冲区数据的去向

            // 策略 A：保留缓冲区，允许上层捕获异常后重试 flush
            // 如果异常是暂时的（如瞬时锁冲突），保留 buffer 允许下次 flush 成功
            // 此时什么都不做，直接抛出异常。

            // 策略 B：防止死循环，转存死信队列 (推荐)
            // 如果异常是永久性的（如数据格式错误导致 LMDB 崩溃），保留 buffer 会导致无限重试失败。
            // 应当将数据转存到死信队列，然后清空 buffer，保证 Writer 可继续工作。
            handleFlushFailure(buffer, e);

            throw new RuntimeException("Flush failed, transaction rolled back. Data moved to Dead Letter Queue.", e);
        }
    }


    @Override
    public long recordsWritten() {
        return writtenCount;
    }

    @Override
    public long recordsFailed() {
        return failedCount;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            flush();
            instance.close();
            closed = true;
        }
    }

    private Write.Result doWrite(KvRecord data) {
        try {
            byte[] key = data.key();
            LmdbInstance.PutStatus status = instance.put(key, data.value());

            if (status == LmdbInstance.PutStatus.SUCCESS) {
                writtenCount++;
                return Write.Result.ok(1);
            } else {
                failedCount++;
                String errorMsg = switch (status) {
                    case MAP_FULL -> MAPFULL_MESSAGE;
                    case READ_ONLY -> READONLY_MESSAGE;
                    default -> "Unknown LMDB status: " + status;
                };
                return Write.Result.error(errorMsg);
            }
        } catch (Exception e) {
            failedCount++;
            return Write.Result.error("Write failed: " + e.getMessage());
        }
    }

    private void handleFlushFailure(List<KvRecord> failedBuffer, Exception e) {
        if (deadLetterHandler != null) {
            try {
                // 将失败数据导出到外部存储（文件、日志、Kafka等）
                deadLetterHandler.save(failedBuffer, e);
            } catch (Exception ex) {
                // 如果连 DLQ 都失败了，只能记录日志并丢弃，防止内存撑爆
                log.error("FATAL: Failed to save to Dead Letter Queue. Data lost. Size: {}", failedBuffer.size(), ex);
            }
        }
        // 无论 DLQ 是否成功，都要清空缓冲区，避免 Writer "堵死"
        buffer.clear();
    }
}
