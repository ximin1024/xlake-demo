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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

@Slf4j
public abstract class AbstractWALWriter implements WALWriter {

    static final byte[] MAGIC = "NWL0".getBytes(StandardCharsets.US_ASCII);
    static final int VERSION = 1;
    static final int HEADER_PREFIX_SIZE = 4 + 4 + 4;
    static final byte RECORD_TYPE_DATA = 1;
    static final byte RECORD_TYPE_SYNC_MARKER = 2;

    protected final String storeId;
    protected final long maxSizeBytes;
    protected final long syncIntervalMs;
    protected final ReentrantLock writeLock = new ReentrantLock();

    protected final AtomicLong currentFileSize = new AtomicLong(0);
    protected final AtomicLong totalWrittenBytes = new AtomicLong(0);
    protected final AtomicLong recordCount = new AtomicLong(0);
    protected final AtomicLong fileSequence = new AtomicLong(0);
    protected volatile long committedSequence = -1;

    private volatile boolean closed = false;
    private volatile long lastSyncTime = System.currentTimeMillis();

    protected AbstractWALWriter(String storeId, long maxSizeBytes, long syncIntervalMs) {
        this.storeId = storeId;
        this.maxSizeBytes = maxSizeBytes;
        this.syncIntervalMs = syncIntervalMs;
    }

    @Override
    public void write(WALRecord entry) throws IOException {
        writeLock.lock();
        try {
            if (closed) {
                throw new IOException("WALWriter is closed");
            }

            if (!isStreamOpen()) {
                doCreateFile();
            }

            byte[] recordBytes = serializeRecord(entry);
            long recordSize = recordBytes.length;

            if (currentFileSize.get() + recordSize > maxSizeBytes && recordCount.get() > 0) {
                closeCurrentFile();
                doCreateFile();
            }

            doWrite(recordBytes);
            currentFileSize.addAndGet(recordSize);
            totalWrittenBytes.addAndGet(recordSize);
            recordCount.incrementAndGet();
            committedSequence = entry.sequence();

            forceSyncIfNeeded();

        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void forceSync() throws IOException {
        writeLock.lock();
        try {
            if (isStreamOpen()) {
                doSync();
                log.debug("Force sync completed, sequence: {}", committedSequence);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long writtenBytes() {
        return totalWrittenBytes.get();
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (isStreamOpen()) {
                try {
                    doSync();
                } catch (IOException e) {
                    log.warn("Sync failed during close", e);
                }
                doClose();
            }
            log.info("{} closed, total written: {} bytes", getClass().getSimpleName(), totalWrittenBytes.get());
        } finally {
            writeLock.unlock();
        }
    }

    protected void forceSyncIfNeeded() throws IOException {
        if (syncIntervalMs <= 0) {
            return;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastSyncTime >= syncIntervalMs) {
            doSync();
            lastSyncTime = currentTime;
        }
    }

    protected byte[] serializeHeader(String executorId) {
        String headerJson = String.format(
                "{\"storeId\":\"%s\",\"executorId\":\"%s\",\"createdAt\":%d,\"compression\":\"NONE\"}",
                storeId, executorId, System.currentTimeMillis());
        byte[] headerJsonBytes = headerJson.getBytes(StandardCharsets.UTF_8);

        int totalSize = HEADER_PREFIX_SIZE + headerJsonBytes.length + 4;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.put(MAGIC);
        buffer.putInt(VERSION);
        buffer.putInt(headerJsonBytes.length);
        buffer.put(headerJsonBytes);

        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc32.getValue());

        return buffer.array();
    }

    protected byte[] serializeRecord(WALRecord entry) {
        byte recordType = "SYNC_MARKER".equals(entry.marker()) ? RECORD_TYPE_SYNC_MARKER : RECORD_TYPE_DATA;
        byte[] tableIdBytes = entry.uniqTableIdentifier() != null
                ? entry.uniqTableIdentifier().getBytes(StandardCharsets.UTF_8)
                : new byte[0];
        byte[] dataBytes = entry.value() != null ? entry.value() : new byte[0];

        int payloadSize = 1 + 8 + 8 + 4 + tableIdBytes.length + 8 + 4 + dataBytes.length;
        int totalSize = 4 + payloadSize + 4;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(payloadSize);
        buffer.put(recordType);
        buffer.putLong(entry.sequence());
        buffer.putLong(entry.timestamp());
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putLong(entry.commitId());
        buffer.putInt(dataBytes.length);
        buffer.put(dataBytes);

        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 4, payloadSize);
        buffer.putInt((int) crc32.getValue());

        return buffer.array();
    }

    protected void closeCurrentFile() throws IOException {
        if (isStreamOpen()) {
            try {
                doSync();
            } catch (IOException e) {
                log.warn("Sync failed during file roll", e);
            }
            doClose();
            log.info("Rolled to new WAL file, records: {}, size: {}",
                    recordCount.get(), currentFileSize.get());
        }
    }

    protected abstract boolean isStreamOpen();

    protected abstract void doWrite(byte[] data) throws IOException;

    protected abstract void doSync() throws IOException;

    protected abstract void doClose() throws IOException;

    protected abstract void doCreateFile() throws IOException;
}
