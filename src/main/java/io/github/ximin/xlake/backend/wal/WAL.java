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

import com.lmax.disruptor.FatalExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class WAL {
    protected Disruptor<WALEvent> disruptor;
    protected RingBuffer<WALEvent> ringBuffer;
    private final WALConsumer consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ReentrantLock shutdownLock = new ReentrantLock();

    public WAL(int bufferSize, long maxWALSize) {
        this.consumer = new WALConsumer(maxWALSize);
        this.disruptor = new Disruptor<>(
                new WALEventFactory(),
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new TimeoutBlockingWaitStrategy(3 * 1000, TimeUnit.MILLISECONDS)
        );

        disruptor.setDefaultExceptionHandler(new FatalExceptionHandler());
        disruptor.handleEventsWith(consumer);

        this.ringBuffer = disruptor.getRingBuffer();
    }

    public long append(WALRecord data, boolean sync) {
        long sequence = ringBuffer.next();
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
    }

    public void insertSyncMarker(String tableIdentifier) {
        long sequence = ringBuffer.next();
        try {
            WALEvent event = ringBuffer.get(sequence);
            event.set(WALRecord.createSyncMarker(tableIdentifier));
        } finally {
            ringBuffer.publish(sequence);
        }
        waitForSequence(sequence);
    }

    private void waitForSequence(long sequence) {
        // 30秒超时
        long timeoutMs = 30000;
        long startTime = System.currentTimeMillis();

        while (consumer.getLastCommittedSequence() < sequence) {
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                throw new RuntimeException("Timeout waiting for sequence: " + sequence);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for sequence", e);
            }
        }
    }
}
