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

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class WAL {
    private static final int TRY_NEXT_MAX_RETRIES = 3;
    private static final long TRY_NEXT_BACKOFF_MS = 1;

    protected Disruptor<WALEvent> disruptor;
    protected RingBuffer<WALEvent> ringBuffer;
    private final WALConsumer consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ReentrantLock shutdownLock = new ReentrantLock();
    private final long appendTimeoutMs;

    public WAL(WALConfig config) {
        this(config, "BINARY");
    }

    public WAL(WALConfig config, String fileFormat) {
        this(config, fileFormat, new Configuration());
    }

    public WAL(WALConfig config, String fileFormat, Configuration hadoopConfig) {
        this.appendTimeoutMs = config.appendTimeoutMs();
        this.consumer = new WALConsumer(config, fileFormat, hadoopConfig);
        this.consumer.setOnConsumerStopped(this::notifyConsumerStopped);
        this.disruptor = new Disruptor<>(
                new WALEventFactory(),
                config.bufferSize(),
                daemonThreadFactory(),
                ProducerType.MULTI,
                new BlockingWaitStrategy()
        );

        disruptor.setDefaultExceptionHandler(new WALExceptionHandler<>());
        disruptor.handleEventsWith(consumer);

        this.ringBuffer = disruptor.getRingBuffer();
        this.disruptor.start();
        this.running.set(true);
    }

    @Deprecated
    public WAL(WALConfig config, String executorId, String walHdfsPath) {
        this(config, executorId, walHdfsPath, new Configuration());
    }

    @Deprecated
    public WAL(WALConfig config, String executorId, String walHdfsPath, Configuration hadoopConfig) {
        WALConfig updatedConfig = new WALConfig(
                config.logDir(),
                config.logFilePrefix(),
                config.segmentSize(),
                config.syncOnWrite(),
                config.bufferSize(),
                config.compressionCodec(),
                config.storeId(),
                executorId != null ? executorId : config.executorId(),
                walHdfsPath != null ? walHdfsPath : config.hdfsBasePath(),
                config.syncIntervalMs(),
                config.appendTimeoutMs()
        );
        this.appendTimeoutMs = updatedConfig.appendTimeoutMs();
        this.consumer = new WALConsumer(updatedConfig, "BINARY", hadoopConfig);
        this.consumer.setOnConsumerStopped(this::notifyConsumerStopped);
        this.disruptor = new Disruptor<>(
                new WALEventFactory(),
                updatedConfig.bufferSize(),
                daemonThreadFactory(),
                ProducerType.MULTI,
                new BlockingWaitStrategy()
        );

        disruptor.setDefaultExceptionHandler(new WALExceptionHandler<>());
        disruptor.handleEventsWith(consumer);

        this.ringBuffer = disruptor.getRingBuffer();
        this.disruptor.start();
        this.running.set(true);
    }

    private void notifyConsumerStopped() {
        running.set(false);
    }

    public boolean isRunning() {
        return running.get() && consumer.isRunning();
    }

    public void close() {
        shutdownLock.lock();
        try {
            if (running.compareAndSet(true, false)) {
                disruptor.shutdown();
            }
        } finally {
            shutdownLock.unlock();
        }
    }

    public long append(WALRecord data, boolean sync) {
        if (!shutdownLock.tryLock()) {
            throw new WALWriteException("WAL is shutting down");
        }
        try {
            if (!isRunning()) {
                throw new WALWriteException("WAL is not running, cannot append");
            }
            long sequence = tryNextWithRetry();
            try {
                WALEvent event = ringBuffer.get(sequence);
                event.set(data);
            } finally {
                ringBuffer.publish(sequence);
            }

            if (sync) {
                waitForSequence(sequence);
            }

            return sequence;
        } finally {
            shutdownLock.unlock();
        }
    }

    public void insertSyncMarker(String tableIdentifier) {
        if (!shutdownLock.tryLock()) {
            throw new WALWriteException("WAL is shutting down");
        }
        try {
            if (!isRunning()) {
                throw new WALWriteException("WAL is not running, cannot insert sync marker");
            }
            long sequence = tryNextWithRetry();
            try {
                WALEvent event = ringBuffer.get(sequence);
                event.set(WALRecord.createSyncMarker(tableIdentifier));
            } finally {
                ringBuffer.publish(sequence);
            }
            waitForSequence(sequence);
        } finally {
            shutdownLock.unlock();
        }
    }

    private long tryNextWithRetry() {
        long deadline = System.currentTimeMillis() + appendTimeoutMs;
        for (int i = 0; i < TRY_NEXT_MAX_RETRIES; i++) {
            if (System.currentTimeMillis() > deadline) {
                throw new WALWriteException("WAL ring buffer is full (timeout)");
            }
            try {
                return ringBuffer.tryNext();
            } catch (InsufficientCapacityException e) {
                if (i < TRY_NEXT_MAX_RETRIES - 1) {
                    LockSupport.parkNanos(TRY_NEXT_BACKOFF_MS * 1_000_000L);
                }
            }
        }
        throw new WALWriteException("WAL ring buffer is full");
    }

    private void waitForSequence(long sequence) {
        long startTime = System.currentTimeMillis();

        while (consumer.getLastCommittedSequence() < sequence) {
            if (System.currentTimeMillis() - startTime > appendTimeoutMs) {
                throw new WALWriteException("Timeout waiting for sequence: " + sequence);
            }

            if (!running.get()) {
                throw new WALWriteException("WAL stopped while waiting for sequence: " + sequence);
            }

            LockSupport.parkNanos(1_000_000L);
        }
    }

    private static ThreadFactory daemonThreadFactory() {
        return r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("wal-disruptor");
            return t;
        };
    }
}
