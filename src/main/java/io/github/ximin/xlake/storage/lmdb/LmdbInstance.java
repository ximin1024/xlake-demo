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
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

@Slf4j
public class LmdbInstance {
    public enum LeaseState {
        ACTIVE,
        RELEASED,
        READ_ONLY,
        MARKED_FOR_DELETION,
        CLOSED,
        DESTROYED
    }

    // Readonly marker file (permanent until destroy)
    public final static String LOCK_EXTENSION = ".lock";
    // Write lease file suffix (placed under table/base path, not inside instance dir)
    public final static String LEASE_EXTENSION = ".lease";
    // System property to override default lease TTL (ms)
    public final static String CONF_LEASE_TTL_MS = "xlake.lmdb.lease.ttl.ms";
    // Default TTL 10s
    private static final long DEFAULT_LEASE_TTL_MS = 10_000L;
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
    private final Path leasePath; // <tablePath>/<instanceId>.lease
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

    // Process identity used for lease ownership.
    private static final String PROCESS_OWNER_ID = buildProcessOwnerId();
    private final long leaseTtlMs;
    private volatile boolean leaseOwner;
    private volatile String activeLeaseOwnerId;
    private volatile long cachedLeaseValidUntil;
    private volatile ScheduledFuture<?> leaseRenewTask;
    private static final ScheduledExecutorService LEASE_RENEWER = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "Lmdb-Lease-Renewer");
            t.setDaemon(true);
            return t;
        }
    });

    public enum PutStatus {

        SUCCESS,

        MAP_FULL,

        READ_ONLY
    }

    public LmdbInstance(String instanceId, Path basePath, long mappedSize) {
        this.instanceId = instanceId;
        this.instancePath = basePath.resolve(instanceId);
        this.lockPath = instancePath.resolve(LOCK_EXTENSION);
        this.leasePath = basePath.resolve(instanceId + LEASE_EXTENSION);
        this.rangePath = instancePath.resolve(RANGE_FILE);
        this.maxBytes = mappedSize;
        this.leaseTtlMs = defaultLeaseTtlMs();

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

        this.env = isReadOnly()
                ? builder.open(instancePath.toFile(), EnvFlags.MDB_RDONLY_ENV)
                : builder.open(instancePath.toFile());

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
            // Lease fencing: only the current lease owner process can write.
            if (!isLeaseWritableByThisProcess()) {
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


    public Path leasePath() {
        return this.leasePath;
    }


    public static long defaultLeaseTtlMs() {
        try {
            return Long.getLong(CONF_LEASE_TTL_MS, DEFAULT_LEASE_TTL_MS);
        } catch (Throwable ignored) {
            return DEFAULT_LEASE_TTL_MS;
        }
    }


    public boolean tryAcquireLease(String ownerId, long ttlMs) {
        long now = System.currentTimeMillis();
        long expireAt = now + Math.max(1L, ttlMs);
        try (FileChannel channel = FileChannel.open(
                leasePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        )) {
            FileLock lock = tryLockLeaseFile(channel);
            if (lock == null) {
                return false;
            }
            try {
                LeaseInfo current = readLeaseFromChannel(channel);
                if (current != null && !current.allowsAcquire(now)) {
                    return false;
                }
                long nextVersion = current == null ? 1L : current.version() + 1L;
                writeLeaseToChannel(channel, ownerId, expireAt, nextVersion, LeaseState.ACTIVE);
                activeLeaseOwnerId = ownerId;
                cachedLeaseValidUntil = Math.min(expireAt, System.currentTimeMillis() + leaseTtlMs / 2);
                return true;
            } finally {
                releaseQuietly(lock);
            }
        } catch (IOException e) {
            log.warn("tryAcquireLease failed for {}: {}", instanceId, e.toString());
            return false;
        }
    }


    public boolean renewLeaseIfOwner(String ownerId, long ttlMs) {
        long now = System.currentTimeMillis();
        long expireAt = now + Math.max(1L, ttlMs);
        if (!Files.exists(leasePath)) {
            return false;
        }
        try (FileChannel channel = FileChannel.open(
                leasePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        )) {
            FileLock lock = tryLockLeaseFile(channel);
            if (lock == null) {
                return false;
            }
            try {
                LeaseInfo current = readLeaseFromChannel(channel);
                if (current == null
                        || current.state() != LeaseState.ACTIVE
                        || !ownerId.equals(current.ownerId())) {
                    return false;
                }
                writeLeaseToChannel(channel, ownerId, expireAt, current.version() + 1L, LeaseState.ACTIVE);
                activeLeaseOwnerId = ownerId;
                cachedLeaseValidUntil = Math.min(expireAt, System.currentTimeMillis() + leaseTtlMs / 2);
                return true;
            } finally {
                releaseQuietly(lock);
            }
        } catch (IOException e) {
            log.warn("renewLeaseIfOwner failed for {}: {}", instanceId, e.toString());
            return false;
        }
    }


    public boolean releaseLeaseIfOwner(String ownerId) {
        try (FileChannel channel = FileChannel.open(
                leasePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        )) {
            FileLock lock = tryLockLeaseFile(channel);
            if (lock == null) {
                return false;
            }
            try {
                LeaseInfo current = readLeaseFromChannel(channel);
                if (current == null || !ownerId.equals(current.ownerId())) {
                    return false;
                }
                writeLeaseToChannel(channel, ownerId, 0L, current.version() + 1L, LeaseState.RELEASED);
            } finally {
                releaseQuietly(lock);
            }
            cachedLeaseValidUntil = 0L;
            clearActiveLeaseOwner(ownerId);
            return true;
        } catch (IOException e) {
            log.warn("releaseLeaseIfOwner failed for {}: {}", instanceId, e.toString());
            return false;
        }
    }


    public LeaseInfo currentLease() {
        return readLeaseInternal();
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
                stopLeaseRenewal();
                updateLeaseState(LeaseState.READ_ONLY, activeLeaseOwnerId);
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
                stopLeaseRenewal();
                updateLeaseState(LeaseState.CLOSED, activeLeaseOwnerId);
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
                stopLeaseRenewal();
                updateLeaseState(LeaseState.DESTROYED, activeLeaseOwnerId);
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
                stopLeaseRenewal();
                updateLeaseState(LeaseState.MARKED_FOR_DELETION, activeLeaseOwnerId);
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

    private LeaseInfo readLeaseInternal() {
        if (!Files.exists(leasePath)) {
            return null;
        }
        try (FileChannel channel = FileChannel.open(leasePath, StandardOpenOption.READ)) {
            return readLeaseFromChannel(channel);
        } catch (IOException e) {
            return null;
        }
    }

    public record LeaseInfo(String ownerId, long expireAt, long version, LeaseState state) {
        public boolean expired() {
            return !isActive(System.currentTimeMillis());
        }

        public boolean isActive(long now) {
            return state == LeaseState.ACTIVE && now < expireAt;
        }

        public boolean allowsAcquire(long now) {
            return switch (state) {
                case ACTIVE -> now >= expireAt;
                case RELEASED -> true;
                case READ_ONLY, MARKED_FOR_DELETION, CLOSED, DESTROYED -> false;
            };
        }
        @Override
        public String toString() {
            return "Lease(owner=" + ownerId
                    + ", expireAt=" + Instant.ofEpochMilli(expireAt)
                    + ", version=" + version
                    + ", state=" + state + ")";
        }
    }

    private boolean isLeaseWritableByThisProcess() {
        long now = System.currentTimeMillis();
        if (now < cachedLeaseValidUntil && activeLeaseOwnerId != null) {
            return true;
        }
        LeaseInfo info = readLeaseInternal();
        if (info != null
                && info.isActive(now)
                && activeLeaseOwnerId != null
                && activeLeaseOwnerId.equals(info.ownerId())) {
            cachedLeaseValidUntil = Math.min(info.expireAt(), now + leaseTtlMs / 2);
            return true;
        }
        cachedLeaseValidUntil = 0L;
        return false;
    }

    private void startLeaseRenewalIfNeeded(String ownerId) {
        if (!leaseOwner) {
            return;
        }
        if (leaseRenewTask != null && !leaseRenewTask.isCancelled()) {
            return;
        }
        long periodMs = Math.max(1_000L, leaseTtlMs / 3);
        leaseRenewTask = LEASE_RENEWER.scheduleAtFixedRate(() -> {
            try {
                if (!renewLeaseIfOwner(ownerId, leaseTtlMs)) {
                    stopLeaseRenewal();
                }
            } catch (Throwable t) {
                // swallow to keep scheduling alive
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    private void stopLeaseRenewal() {
        ScheduledFuture<?> task = leaseRenewTask;
        leaseRenewTask = null;
        leaseOwner = false;
        cachedLeaseValidUntil = 0L;
        if (task != null) {
            task.cancel(false);
        }
    }

    private static String buildProcessOwnerId() {
        long pid;
        try {
            pid = ProcessHandle.current().pid();
        } catch (Throwable t) {
            pid = -1L;
        }
        String host;
        try {
            host = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Throwable t) {
            host = "unknown-host";
        }
        return host + ":" + pid + ":" + UUID.randomUUID();
    }

    public static String routingOwnerId(String executorId, long epoch) {
        return "routing-owner:" + executorId + ":" + epoch;
    }


    public boolean ensureRoutingOwnerLease(String executorId, long epoch) {
        String ownerId = routingOwnerId(executorId, epoch);
        LeaseInfo info = readLeaseInternal();
        if (info != null
                && info.isActive(System.currentTimeMillis())
                && ownerId.equals(info.ownerId())) {
            activeLeaseOwnerId = ownerId;
            leaseOwner = true;
            startLeaseRenewalIfNeeded(ownerId);
            renewLeaseIfOwner(ownerId, leaseTtlMs);
            return true;
        }
        stopLeaseRenewal();
        boolean acquired = tryAcquireLease(ownerId, leaseTtlMs);
        if (acquired) {
            activeLeaseOwnerId = ownerId;
            leaseOwner = true;
            startLeaseRenewalIfNeeded(ownerId);
        }
        return acquired;
    }

    private LeaseInfo readLeaseFromChannel(FileChannel channel) throws IOException {
        channel.position(0L);
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(Math.max(channel.size(), 0L), 4096L));
        while (buffer.hasRemaining() && channel.read(buffer) > 0) {
            // keep reading
        }
        buffer.flip();
        if (!buffer.hasRemaining()) {
            return null;
        }
        String content = StandardCharsets.UTF_8.decode(buffer).toString();
        String[] parts = content.split("\n");
        if (parts.length < 2) {
            return null;
        }
        String owner = parts[0].trim();
        long expireAt = Long.parseLong(parts[1].trim());
        long version = parts.length >= 3 ? Long.parseLong(parts[2].trim()) : 1L;
        LeaseState state = parts.length >= 4 ? LeaseState.valueOf(parts[3].trim()) : LeaseState.ACTIVE;
        return new LeaseInfo(owner, expireAt, version, state);
    }

    private void writeLeaseToChannel(FileChannel channel,
                                     String ownerId,
                                     long expireAt,
                                     long version,
                                     LeaseState state) throws IOException {
        byte[] content = (ownerId + "\n" + expireAt + "\n" + version + "\n" + state.name())
                .getBytes(StandardCharsets.UTF_8);
        channel.truncate(0L);
        channel.position(0L);
        channel.write(ByteBuffer.wrap(content));
        channel.force(true);
    }

    private void updateLeaseState(LeaseState state, String preferredOwnerId) {
        try (FileChannel channel = FileChannel.open(
                leasePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        )) {
            FileLock lock = tryLockLeaseFile(channel);
            if (lock == null) {
                throw new IOException("Failed to acquire lease file lock for state transition: " + leasePath);
            }
            try {
                LeaseInfo current = readLeaseFromChannel(channel);
                String ownerId = preferredOwnerId;
                long nextVersion = 1L;
                if (current != null) {
                    if (ownerId == null || ownerId.isBlank()) {
                        ownerId = current.ownerId();
                    }
                    nextVersion = current.version() + 1L;
                }
                if (ownerId == null) {
                    ownerId = "";
                }
                writeLeaseToChannel(channel, ownerId, 0L, nextVersion, state);
            } finally {
                releaseQuietly(lock);
            }
            clearActiveLeaseOwner(preferredOwnerId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update lease state for " + instanceId + " to " + state, e);
        }
    }

    private FileLock tryLockLeaseFile(FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (OverlappingFileLockException e) {
            return null;
        }
    }

    private void releaseQuietly(FileLock lock) {
        try {
            if (lock != null && lock.isValid()) {
                lock.release();
            }
        } catch (IOException ignored) {
        }
    }

    private void clearActiveLeaseOwner(String ownerId) {
        if (ownerId != null && ownerId.equals(activeLeaseOwnerId)) {
            activeLeaseOwnerId = null;
        }
    }
}
