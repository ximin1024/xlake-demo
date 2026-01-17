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
package io.github.ximin.xlake.backend;

import io.github.ximin.xlake.common.FastLazy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.lmdbjava.*;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

@Slf4j
public class LmdbInstance {

    private final static String LOCK_EXTENSION = ".loc";
    private final static String DEFALUT_DB = "default";

    private final String instanceId;
    @Getter
    private final Env<ByteBuffer> env;
    @Getter
    private final long maxSizeBytes;
    private final Path instancePath;
    private volatile boolean readOnly = false;
    private final Arena arena;
    private final Path lockPath;
    private final Dbi<ByteBuffer> dbi;

    @Getter
    private long totalKeySize;
    @Getter
    private long totalValueSize;
    @Getter
    private long totalUsageSize;
    @Getter
    private long totalRecords;

    private final FastLazy<LmdbSpaceMonitor> spaceMonitor;

    public LmdbInstance(String instanceId, String basePath, long mappedSize, Arena arena) {
        this(instanceId, Path.of(basePath + "/" + instanceId), mappedSize, arena);
    }

    public LmdbInstance(String instanceId, Path basePath, long mappedSize, Arena arena) {
        this.instanceId = instanceId;
        this.instancePath = basePath.resolve(instanceId);
        this.lockPath = instancePath.resolve(LOCK_EXTENSION);
        this.maxSizeBytes = mappedSize * 1024 * 1024;
        this.arena = arena;

        if (!Files.exists(instancePath)) {
            try {
                Files.createDirectories(instancePath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create LMDB directory: " + instancePath, e);
            }
        } else {
            if (Files.exists(lockPath)) {
                this.readOnly = true;
            }
        }

        this.env = Env.create()
                .setMapSize(maxSizeBytes)
                .setMaxDbs(1)
                .setMaxReaders(126)
                .open(instancePath.toFile());
        this.dbi = env.openDbi(DEFALUT_DB, DbiFlags.MDB_CREATE);
        this.spaceMonitor = new FastLazy<>(() -> new LmdbSpaceMonitor(this.env));
    }

    public WriteResult put(byte[] key, byte[] value) {
        if (readOnly) {
            return WriteResult.error(instanceId, "Instance is read-only");
        }

        try {

            if (!hasSpaceForWrite(value.length)) {
                return WriteResult.mapFull(instanceId, "Insufficient space");
            }
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer keyBuf = arena.allocate(key.length).asByteBuffer();
                keyBuf.put(key).flip();

                ByteBuffer valBuf = arena.allocate(value.length).asByteBuffer();
                valBuf.put(value).flip();

                dbi.put(txn, keyBuf, valBuf);
                txn.commit();
                updateWriteStatistics(key.length, value.length);
                return WriteResult.success(instanceId);
            }
        } catch (Env.MapFullException e) {
            markAsReadOnly();
            return WriteResult.mapFull(instanceId, "MDB_MAP_FULL: " + e.getMessage());
        } catch (Exception e) {
            return WriteResult.error(instanceId, "Write failed: " + e.getMessage());
        }
    }

    public void close() {
        if (this.dbi != null) {
            this.dbi.close();
        }
        if (env != null) {
            env.close();
        }
    }

    public Path path() {
        return this.instancePath;
    }

    public synchronized void markAsReadOnly() {
        if (!Files.exists(lockPath)) {
            try {
                Files.createFile(lockPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        this.readOnly = true;
    }

    public boolean readOnly() {
        return this.readOnly;
    }

    public String instanceId() {
        return this.instanceId;
    }

    public Iterator<Pair<byte[], byte[]>> iterator() {
        return new LmdbIterator(instanceId, DEFALUT_DB, env, dbi);
    }

    // lmdb实际占用的空间，和totalUsageSize不一样
    public LmdbSpaceMonitor.SpaceInfo spaceInfo() {
        LmdbSpaceMonitor monitor = spaceMonitor.get();
        monitor.refresh(totalUsageSize / totalRecords);
        return monitor.getSpaceInfo();
    }

    public void destroy() {
        try (Stream<Path> stream = Files.walk(path())) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {

                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public record WriteResult(String instanceId, WriteStatus status, String message) {
        public static WriteResult success(String instanceId) {
            return new WriteResult(instanceId, WriteStatus.SUCCESS, null);
        }

        public static WriteResult mapFull(String instanceId, String message) {
            return new WriteResult(instanceId, WriteStatus.MAP_FULL, message);
        }

        public static WriteResult error(String instanceId, String message) {
            return new WriteResult(instanceId, WriteStatus.ERROR, message);
        }

        public boolean isSuccess() {
            return status == WriteStatus.SUCCESS;
        }

        public boolean isMapFull() {
            return status == WriteStatus.MAP_FULL;
        }
    }

    public enum WriteStatus {SUCCESS, MAP_FULL, ERROR}

    private boolean hasSpaceForWrite(int dataSize) {
        long currentUsage = totalUsageSize;
        // 加上安全余量
        long projectedUsage = currentUsage + dataSize + 1024;
        // 90% 阈值
        return projectedUsage < maxSizeBytes * 0.9;
    }

    private void updateWriteStatistics(long curKeySize, long curValueSize) {
        long curEntrySize = curKeySize + curValueSize;
        totalRecords++;
        totalKeySize += curKeySize;
        totalValueSize += curValueSize;
        totalUsageSize += curEntrySize;
    }

    private static class LmdbIterator implements Iterator<Pair<byte[], byte[]>> {
        private final String instanceId;
        private final String dbName;
        private final Iterator<CursorIterable.KeyVal<ByteBuffer>> cursor;

        public LmdbIterator(String instanceId, String dbName, Env<ByteBuffer> env, Dbi<ByteBuffer> dbi) {
            this.instanceId = instanceId;
            this.dbName = dbName;
            Txn<ByteBuffer> txn = env.txnRead();
            this.cursor = dbi.iterate(txn).iterator();
        }

        @Override
        public boolean hasNext() {
            return cursor.hasNext();
        }

        @Override
        public Pair<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements in lmdb: " + instanceId);
            }

            try {
                CursorIterable.KeyVal<ByteBuffer> keyVal = cursor.next();
                ByteBuffer keyBuf = keyVal.key();
                ByteBuffer valBuf = keyVal.val();

                byte[] keyBytes = new byte[keyBuf.remaining()];
                keyBuf.get(keyBytes);
                byte[] valueBytes = new byte[valBuf.remaining()];
                valBuf.get(valueBytes);

                return Pair.of(keyBytes, valueBytes);
            } catch (Exception e) {
                throw new RuntimeException("Error reading from database: " + dbName, e);
            }
        }
    }
}
