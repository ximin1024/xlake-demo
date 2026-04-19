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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class WALConsumer implements EventHandler<WALEvent>, LifecycleAware {

    private static final int MAX_WRITE_RETRIES = 3;
    private static final long RETRY_BASE_DELAY_MS = 100;
    private static final long RECOVERY_CHECK_INTERVAL_MS = 30_000;
    private static final int MAX_CONSECUTIVE_RECOVERY_FAILURES = 5;

    private volatile WALWriter writer;
    private final WALConfig config;
    private final Configuration hadoopConfig;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final ReentrantLock recoveryLock = new ReentrantLock();

    private volatile boolean running = false;
    private volatile long lastCommittedSequence = -1;
    private volatile boolean degradedToLocal = false;
    private volatile long lastRecoveryAttemptTime = 0;
    private final AtomicInteger consecutiveRecoveryFailures = new AtomicInteger(0);
    private volatile Runnable onConsumerStopped;

    public WALConsumer(WALConfig config) {
        this(config, "BINARY", new Configuration());
    }

    public WALConsumer(WALConfig config, String fileFormat) {
        this(config, fileFormat, new Configuration());
    }

    public WALConsumer(WALConfig config, String fileFormat, Configuration hadoopConfig) {
        this.config = config;
        this.hadoopConfig = hadoopConfig;

        if (config.isHdfsEnabled()) {
            this.writer = new HdfsWALWriter(
                    config.storeId(),
                    config.executorId(),
                    config.hdfsBasePath(),
                    config.segmentSize(),
                    config.syncIntervalMs(),
                    hadoopConfig
            );
        } else {
            this.writer = new LocalWALWriter(
                    config.storeId(),
                    config.logDir(),
                    config.segmentSize(),
                    config.syncIntervalMs()
            );
        }
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
                    entry.marker());

            writeWithRetry(entry);
            lastCommittedSequence = sequence;
            processedCount.incrementAndGet();
            event.markProcessed();

            maybeRecoverFromDegradation();

        } catch (Exception e) {
            errorCount.incrementAndGet();
            log.error("Critical error processing event at sequence: {}", sequence, e);

            if (!attemptRecovery(sequence, e)) {
                running = false;
                if (onConsumerStopped != null) {
                    onConsumerStopped.run();
                }
                throw new RuntimeException("Unrecoverable error at sequence: " + sequence, e);
            }
        }
    }

    @Override
    public void onStart() {
        try {
            running = true;
            log.info("WALConsumer started with writer: {}", writer.getClass().getSimpleName());
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
            log.info("WALConsumer shutdown completed. " +
                    "Processed: {}, Errors: {}", processedCount.get(), errorCount.get());
        } catch (Exception e) {
            log.error("Error during shutdown", e);
        }
    }

    public long getLastCommittedSequence() {
        return lastCommittedSequence;
    }

    public boolean isRunning() {
        return running;
    }

    public void setOnConsumerStopped(Runnable callback) {
        this.onConsumerStopped = callback;
    }

    private void writeWithRetry(WALRecord entry) throws IOException {
        IOException lastError = null;
        for (int i = 0; i < MAX_WRITE_RETRIES; i++) {
            try {
                writer.write(entry);
                return;
            } catch (IOException e) {
                lastError = e;
                if (i < MAX_WRITE_RETRIES - 1) {
                    long delay = RETRY_BASE_DELAY_MS * (1L << i);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw lastError;
                    }
                }
            }
        }

        if (!degradedToLocal && config.isHdfsEnabled()) {
            degradeToLocalWriter();
            try {
                writer.write(entry);
                return;
            } catch (IOException e) {
                lastError = e;
            }
        }

        throw lastError;
    }

    private void degradeToLocalWriter() {
        log.warn("HDFS WAL write failed after retries, degrading to local WAL writer");
        try {
            writer.close();
        } catch (IOException e) {
            log.warn("Failed to close HDFS writer during degradation", e);
        }
        writer = new LocalWALWriter(config.storeId(), config.logDir(), config.segmentSize(), config.syncIntervalMs());
        degradedToLocal = true;
        log.info("Degraded to local WAL writer");
    }

    private void maybeRecoverFromDegradation() {
        if (!degradedToLocal) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastRecoveryAttemptTime < RECOVERY_CHECK_INTERVAL_MS) {
            return;
        }
        lastRecoveryAttemptTime = now;

        recoveryLock.lock();
        try {
            try {
                Path hdfsPath = new Path(config.hdfsBasePath());
                FileSystem fs = hdfsPath.getFileSystem(hadoopConfig);
                fs.getFileStatus(hdfsPath);
            } catch (IOException e) {
                log.debug("HDFS still unavailable, staying in degraded mode");
                return;
            }

            WALWriter hdfsWriter = new HdfsWALWriter(
                    config.storeId(), config.executorId(), config.hdfsBasePath(),
                    config.segmentSize(), config.syncIntervalMs(), hadoopConfig);

            try {
                writer.close();
            } catch (IOException e) {
                log.warn("Failed to close local writer during HDFS recovery", e);
            }
            writer = hdfsWriter;
            degradedToLocal = false;
            consecutiveRecoveryFailures.set(0);
            log.info("Recovered from local WAL to HDFS WAL writer");
        } finally {
            recoveryLock.unlock();
        }
    }

    private boolean attemptRecovery(long failedSequence, Exception error) {
        recoveryLock.lock();
        try {
            log.warn("Attempting recovery after failure at sequence: {}", failedSequence);

            boolean isHdfsError = isHdfsError(error);
            if (isHdfsError && !degradedToLocal && config.isHdfsEnabled()) {
                try {
                    degradeToLocalWriter();
                    consecutiveRecoveryFailures.set(0);
                    log.info("Recovery completed via degradation to local writer, resuming from sequence: {}", failedSequence + 1);
                    return true;
                } catch (Exception e) {
                    log.error("Failed to degrade to local writer during recovery", e);
                    return false;
                }
            }

            if (!isHdfsError) {
                try {
                    writer.forceSync();
                    consecutiveRecoveryFailures.set(0);
                    log.info("Recovery completed via forceSync, resuming from sequence: {}", failedSequence + 1);
                    return true;
                } catch (IOException e) {
                    log.error("Failed to sync during recovery", e);
                }
            }

            int failures = consecutiveRecoveryFailures.incrementAndGet();
            if (failures >= MAX_CONSECUTIVE_RECOVERY_FAILURES) {
                log.error("Exceeded max consecutive recovery failures ({}), stopping WAL consumer. " +
                        "All subsequent append() calls will fail.", MAX_CONSECUTIVE_RECOVERY_FAILURES);
                try {
                    writer.forceSync();
                } catch (IOException e) {
                    log.error("Failed to sync WAL before stopping", e);
                }
                return false;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            log.info("Recovery attempt completed ({}), resuming from sequence: {}", failures, failedSequence + 1);
            return true;

        } finally {
            recoveryLock.unlock();
        }
    }

    private boolean isHdfsError(Throwable error) {
        if (error == null) {
            return false;
        }
        if (error instanceof ConnectException
                || error instanceof NoRouteToHostException) {
            return true;
        }
        String className = error.getClass().getName();
        if (className.startsWith("org.apache.hadoop.")) {
            return true;
        }
        String message = error.getMessage();
        if (message != null && (message.contains("HDFS") || message.contains("NameNode")
                || message.contains("DataNode"))) {
            return true;
        }
        return isHdfsError(error.getCause());
    }
}
