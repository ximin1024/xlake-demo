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

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class WALConsumer implements EventHandler<WALEvent>, LifecycleAware {

    private final WALParquetWriter writer;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final ReentrantLock recoveryLock = new ReentrantLock();

    private volatile boolean running = false;
    @Getter
    private long lastCommittedSequence = -1;

    public WALConsumer(long maxLogSize) {
        Configuration hadoopConfig = new Configuration();
        this.writer = new WALParquetWriter("", hadoopConfig, maxLogSize, 0L);
    }

    @Override
    public void onEvent(WALEvent event, long sequence, boolean b) throws Exception {
        if (!running) {
            return;
        }

        try {
            WALRecord entry = event.getEntry();
            if (entry == null) {
                return;
            }

            entry = new DefaultWALRecord(
                    sequence,
                    entry.value(),
                    entry.timestamp(),
                    entry.commitId(),
                    entry.uniqTableIdentifier(),
                    entry.marker())
            ;

            writer.write(entry);
            lastCommittedSequence = sequence;
            processedCount.incrementAndGet();
            event.markProcessed();

        } catch (Exception e) {
            errorCount.incrementAndGet();
            log.error("Critical error processing event at sequence: {}", sequence, e);

            if (!attemptRecovery(sequence, e)) {
                // 如果恢复失败，停止处理
                running = false;
                throw new RuntimeException("Unrecoverable error at sequence: " + sequence, e);
            }
        }
    }

    @Override
    public void onStart() {
        try {
            running = true;
            log.info("WALConsumer started.");
        } catch (Exception e) {
            running = false;
            throw new RuntimeException("Failed to start WAL consumer", e);
        }
    }

    @Override
    public void onShutdown() {
        running = false;
        try {
            writer.close();
            log.info("StrongConsistencyWALConsumer shutdown completed. " +
                    "Processed: {}, Errors: {}", processedCount.get(), errorCount.get());
        } catch (Exception e) {
            log.error("Error during shutdown", e);
        }
    }

    private boolean attemptRecovery(long failedSequence, Exception error) {
        recoveryLock.lock();
        try {
            log.warn("Attempting recovery after failure at sequence: {}", failedSequence);

            try {
                writer.forceSync();
            } catch (IOException e) {
                log.error("Failed to sync during recovery", e);
                return false;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            log.info("Recovery completed, resuming from sequence: {}", failedSequence + 1);
            return true;

        } finally {
            recoveryLock.unlock();
        }
    }
}
