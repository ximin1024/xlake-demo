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
package io.github.ximin.xlake.storage.lmdb;

import io.github.ximin.xlake.common.collection.iterator.CloseableIterator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.lmdbjava.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

@Slf4j
public class LmdbInstance {

    public final static String LOCK_EXTENSION = ".lock";
    public final static String RANGE_FILE = "range";
    private final static String DEFAULT_DB = "default";

    // 状态机：统一管理生命周期
    private enum State {OPEN, READ_ONLY, MARKED_FOR_DELETION, CLOSED}

    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

    @Getter
    private final Env<ByteBuffer> env;
    @Getter
    private final long maxBytes;
    private final String instanceId;
    private final Path instancePath;
    private final Path lockPath;
    private final Path rangePath;
    @Getter
    private final Dbi<ByteBuffer> dbi;

    private final ReentrantLock stateLock = new ReentrantLock();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // 引用计数：用于辅助判断是否可以安全删除
    private final AtomicInteger refCount = new AtomicInteger(0);

    // 统计信息
    @Getter
    private long totalKeySize;
    @Getter
    private long totalValueSize;
    @Getter
    private long totalUsageSize;
    @Getter
    private long totalRecords;
    @Getter
    private long createTime;

    private byte[] minKey;
    private byte[] maxKey;

    //private final FastLazy<LmdbSpaceMonitor> spaceMonitor;
    private static final ThreadLocal<ByteBuffer> KEY_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(256));
    private static final ThreadLocal<ByteBuffer> VALUE_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(1024));

    public enum PutStatus {
        /**
         * 写入成功
         */
        SUCCESS,
        /**
         * LMDB Map 空间已满（包括预检查不足或底层抛出 MapFullException）
         */
        MAP_FULL,
        /**
         * 实例处于只读模式，拒绝写入
         */
        READ_ONLY
    }

    public LmdbInstance(String instanceId, Path basePath, long mappedSize) {
        this.instanceId = instanceId;
        this.instancePath = basePath.resolve(instanceId);
        this.lockPath = instancePath.resolve(LOCK_EXTENSION);
        this.rangePath = instancePath.resolve(RANGE_FILE);
        this.maxBytes = mappedSize;

        if (!Files.exists(instancePath)) {
            try {
                Files.createDirectories(instancePath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create LMDB directory: " + instancePath, e);
            }
        } else {
            if (Files.exists(lockPath)) {
                this.state.set(State.READ_ONLY);
            }
        }

        Env.Builder<ByteBuffer> builder = Env.create()
                .setMapSize(maxBytes)
                .setMaxDbs(1)
                .setMaxReaders(126);

        if (isReadOnly()) {
            this.env = builder.open(instancePath.toFile(), EnvFlags.MDB_RDONLY_ENV);
        } else {
            this.env = builder.open(instancePath.toFile());
        }

        if (isReadOnly()) {
            loadKeyRange();
        }

        this.dbi = env.openDbi(DEFAULT_DB, MDB_CREATE);
        this.createTime = System.currentTimeMillis();
    }

    public PutStatus put(byte[] key, byte[] value) {
        if (isReadOnly() || isMarkedForDeletion()) {
            return PutStatus.READ_ONLY;
        }

        rwLock.readLock().lock();
        try {
            if (isReadOnly() || isMarkedForDeletion()) {
                return PutStatus.READ_ONLY;
            }
            if (!hasSpaceForWrite(value.length)) {
                return PutStatus.MAP_FULL;
            }
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer keyBuf = directBuffer(KEY_BUFFER, key.length);
                keyBuf.put(key).flip();
                ByteBuffer valBuf = directBuffer(VALUE_BUFFER, value.length);
                valBuf.put(value).flip();

                dbi.put(txn, keyBuf, valBuf);
                txn.commit();
                updateWriteStatistics(key.length, value.length);
                return PutStatus.SUCCESS;
            }
        } catch (Env.MapFullException e) {
            markAsReadOnly();
            return PutStatus.MAP_FULL;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public byte[] get(byte[] key) {
        rwLock.readLock().lock();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer keyBuf = directBuffer(KEY_BUFFER, key.length);
            keyBuf.put(key).flip();

            ByteBuffer valBuf = dbi.get(txn, keyBuf);
            if (valBuf == null) return null;
            byte[] result = new byte[valBuf.remaining()];
            valBuf.get(result);
            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Iterator<Pair<byte[], byte[]>> iterator() {
        return closeableIterator();
    }

    public CloseableIterator<Pair<byte[], byte[]>> closeableIterator() {
        rwLock.readLock().lock();
        try {
            return new LockedIterator(new LmdbIterator(), rwLock.readLock());
        } catch (RuntimeException e) {
            rwLock.readLock().unlock();
            throw e;
        }
    }

    public Path path() {
        return this.instancePath;
    }

    public void markAsReadOnly() {
        stateLock.lock();
        try {
            if (state.get() != State.OPEN) {
                return;
            }
            rwLock.writeLock().lock();
            try {
                if (state.get() != State.OPEN) {
                    return;
                }
                computeKeyRange();
                saveKeyRange();
                try {
                    Files.createFile(lockPath);
                } catch (FileAlreadyExistsException ignored) {
                }
                transitionTo(State.READ_ONLY);
            } finally {
                rwLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to mark as read-only: " + instanceId, e);
        } finally {
            stateLock.unlock();
        }
    }

    public void close() {
        stateLock.lock();
        try {
            rwLock.writeLock().lock();
            try {
                closeUnsafe();
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            stateLock.unlock();
        }
    }

    public boolean readOnly() {
        State current = state.get();
        return current == State.READ_ONLY || current == State.MARKED_FOR_DELETION;
    }

    public String instanceId() {
        return this.instanceId;
    }

    public byte[] minKey() {
        return minKey == null ? new byte[0] : Arrays.copyOf(minKey, minKey.length);
    }

    public byte[] maxKey() {
        return maxKey == null ? new byte[0] : Arrays.copyOf(maxKey, maxKey.length);
    }

    public void destroy() {
        stateLock.lock();
        try {
            rwLock.writeLock().lock();
            try {
                closeUnsafe();

                // 3. 进程间互斥：获取 .loc 文件的排他锁
                // 注意：FileChannel.open 会创建文件如果不存在
                try (FileChannel channel = FileChannel.open(lockPath,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                    FileLock fileLock = channel.tryLock();
                    if (fileLock == null) {
                        throw new IOException("Failed to acquire file lock, instance may be in use by another process: " + lockPath);
                    }

                    try {
                        // 4. 安全删除文件
                        try (Stream<Path> stream = Files.walk(instancePath)) {
                            stream.sorted(Comparator.reverseOrder())
                                    .forEach(path -> {
                                        try {
                                            Files.deleteIfExists(path);
                                        } catch (IOException ignored) {
                                        }
                                    });
                        }
                    } finally {
                        fileLock.release();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to destroy instance: " + instanceId, e);
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            stateLock.unlock();
        }
    }

    public boolean tryWriteLock() {
        return rwLock.writeLock().tryLock();
    }

    public void unlockWrite() {
        rwLock.writeLock().unlock();
    }

    public void markForDeletion() {
        stateLock.lock();
        try {
            State current = state.get();
            if (current == State.CLOSED || current == State.MARKED_FOR_DELETION) {
                return;
            }
            rwLock.writeLock().lock();
            try {
                current = state.get();
                if (current == State.CLOSED || current == State.MARKED_FOR_DELETION) {
                    return;
                }
                if (current == State.OPEN) {
                    computeKeyRange();
                    saveKeyRange();
                    try {
                        Files.createFile(lockPath);
                    } catch (FileAlreadyExistsException ignored) {
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to create lock file: " + lockPath, e);
                    }
                }
                transitionTo(State.MARKED_FOR_DELETION);
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            stateLock.unlock();
        }
    }

    public boolean isMarkedForDeletion() {
        return state.get() == State.MARKED_FOR_DELETION;
    }

    public boolean isReadOnly() {
        return state.get() == State.READ_ONLY;
    }

    public boolean tryEnterScope() {
        rwLock.readLock().lock();
        refCount.incrementAndGet();
        if (isMarkedForDeletion()) {
            // 如果已标记删除，立即回退
            refCount.decrementAndGet();
            rwLock.readLock().unlock();
            return false;
        }
        return true;
    }

    public void exitScope() {
        refCount.decrementAndGet();
        rwLock.readLock().unlock();
    }

    public int getRefCount() {
        return refCount.get();
    }

    public static String generateInstanceId(String tableIdentifier) {
        return "inst-" + tableIdentifier + "-" + System.currentTimeMillis();
    }

    private void computeKeyRange() {
        minKey = null;
        maxKey = null;
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
                for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                    ByteBuffer keyBuffer = kv.key();
                    byte[] key = new byte[keyBuffer.remaining()];
                    keyBuffer.get(key);
                    if (minKey == null) {
                        minKey = key;
                    }
                    maxKey = key;
                }
                if (minKey == null) {
                    minKey = maxKey = new byte[0];
                }
            }
        }
    }

    private void saveKeyRange() {
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(rangePath))) {
            dos.writeInt(minKey.length);
            dos.write(minKey);
            dos.writeInt(maxKey.length);
            dos.write(maxKey);
        } catch (IOException e) {
            log.error("Failed to save key range for {}", instanceId, e);
        }
    }

    private void loadKeyRange() {
        if (!Files.exists(rangePath)) {
            return;
        }
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(rangePath))) {
            int len = dis.readInt();
            minKey = new byte[len];
            dis.readFully(minKey);
            len = dis.readInt();
            maxKey = new byte[len];
            dis.readFully(maxKey);
        } catch (IOException e) {
            log.error("Failed to load key range for lmdb: {}, exception: {}", instanceId, e);
        }
    }

    private boolean hasSpaceForWrite(int dataSize) {
        long currentUsage = totalUsageSize;
        // 加上安全余量
        long projectedUsage = currentUsage + dataSize + 1024;
        // 90% 阈值
        return projectedUsage < maxBytes * 0.9;
    }

    private void updateWriteStatistics(long curKeySize, long curValueSize) {
        long curEntrySize = curKeySize + curValueSize;
        totalRecords++;
        totalKeySize += curKeySize;
        totalValueSize += curValueSize;
        totalUsageSize += curEntrySize;
    }

    private void closeUnsafe() {
        if (state.get() != State.CLOSED) {
            transitionTo(State.CLOSED);
        }
        if (this.dbi != null) {
            this.dbi.close();
        }
        if (env != null) {
            env.close();
        }
    }

    private void transitionTo(State target) {
        State current = state.get();
        if (current == target) {
            return;
        }
        if (!canTransition(current, target)) {
            throw new IllegalStateException("Illegal state transition: " + current + " -> " + target);
        }
        state.set(target);
    }

    private static boolean canTransition(State from, State to) {
        if (from == to) {
            return true;
        }
        return switch (from) {
            case OPEN -> to == State.READ_ONLY || to == State.MARKED_FOR_DELETION || to == State.CLOSED;
            case READ_ONLY -> to == State.MARKED_FOR_DELETION || to == State.CLOSED;
            case MARKED_FOR_DELETION -> to == State.CLOSED;
            case CLOSED -> false;
        };
    }

    private static final class LockedIterator implements CloseableIterator<Pair<byte[], byte[]>> {
        private final CloseableIterator<Pair<byte[], byte[]>> delegate;
        private final ReentrantReadWriteLock.ReadLock lock;
        private boolean released;

        private LockedIterator(CloseableIterator<Pair<byte[], byte[]>> delegate,
                               ReentrantReadWriteLock.ReadLock lock) {
            this.delegate = delegate;
            this.lock = lock;
        }

        @Override
        public boolean hasNext() {
            try {
                boolean hasNext = delegate.hasNext();
                if (!hasNext) {
                    release();
                }
                return hasNext;
            } catch (RuntimeException e) {
                release();
                throw e;
            }
        }

        @Override
        public Pair<byte[], byte[]> next() {
            try {
                return delegate.next();
            } catch (RuntimeException e) {
                release();
                throw e;
            }
        }

        @Override
        public void close() {
            release();
        }

        private void release() {
            if (!released) {
                released = true;
                try {
                    delegate.close();
                } catch (Exception ignored) {
                }
                lock.unlock();
            }
        }
    }

    private class LmdbIterator implements CloseableIterator<Pair<byte[], byte[]>> {
        private final CursorIterable<ByteBuffer> cursorIterable;
        private final Iterator<CursorIterable.KeyVal<ByteBuffer>> cursor;
        private final Txn<ByteBuffer> txn;
        private boolean closed;

        LmdbIterator() {
            this.txn = env.txnRead();
            this.cursorIterable = dbi.iterate(txn);
            this.cursor = cursorIterable.iterator();
        }

        @Override
        public boolean hasNext() {
            boolean has = cursor.hasNext();
            if (!has) {
                close();
            }
            return has;
        }

        @Override
        public Pair<byte[], byte[]> next() {
            CursorIterable.KeyVal<ByteBuffer> kv = cursor.next();
            ByteBuffer keyBuf = kv.key();
            ByteBuffer valBuf = kv.val();
            byte[] key = new byte[keyBuf.remaining()];
            keyBuf.get(key);
            byte[] value = new byte[valBuf.remaining()];
            valBuf.get(value);
            return Pair.of(key, value);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                cursorIterable.close();
            } catch (Exception ignored) {
            }
            try {
                txn.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static ByteBuffer directBuffer(ThreadLocal<ByteBuffer> holder, int length) {
        ByteBuffer buffer = holder.get();
        if (buffer.capacity() < length) {
            buffer = ByteBuffer.allocateDirect(length);
            holder.set(buffer);
        }
        buffer.clear();
        buffer.limit(length);
        return buffer;
    }
}
