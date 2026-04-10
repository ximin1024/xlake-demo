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
package io.github.ximin.xlake.storage;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.base.Throwables;
import com.ibm.icu.impl.Pair;
import io.github.ximin.xlake.common.SingletonContainer;
import io.github.ximin.xlake.storage.flush.DefaultFlushCoordinator;
import io.github.ximin.xlake.storage.flush.FlushCoordinator;
import io.github.ximin.xlake.storage.flush.FlushPlan;
import io.github.ximin.xlake.storage.lmdb.LmdbInstance;
import io.github.ximin.xlake.storage.table.StoreState;
import io.github.ximin.xlake.storage.table.TableStorage;
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.table.PrimaryKey;
import io.github.ximin.xlake.table.TableId;
import io.github.ximin.xlake.table.op.Read;
import io.github.ximin.xlake.table.op.Scan;
import io.github.ximin.xlake.table.op.Write;
import io.github.ximin.xlake.table.op.WriteBuilder;
import io.github.ximin.xlake.writer.LmdbWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static io.github.ximin.xlake.common.FileUtils.findMatchingPaths;
import static io.github.ximin.xlake.storage.lmdb.LmdbInstance.generateInstanceId;

@Slf4j
public class DynamicMmapStore {
    private final String basePath;
    private final long instanceMappedBytes;
    //private final Arena arena;

    // 全局资源锁，用于控制创建新实例时的内存检查和 Flush 触发
    private final ReentrantLock globalLock;
    private final Map<String, AtomicInteger> totalRecords;
    private final AtomicLong totalUsedMemory = new AtomicLong(0);

    // 要明确这个参数的逻辑，因为要支持多表，所以每张表的写并发数，都等于这个值
    private final int writableNum;
    private final long totalMemoryLimitBytes;
    private final String remoteBasePath;

    private final ConcurrentHashMap<String, TableStore> tableStores = new ConcurrentHashMap<>();
    private final AsyncLoadingCache<String, String> instances;

    private final ScheduledExecutorService monitorService;
    private final ExecutorService flushExecutor;
    private final DelayQueue<PendingDeletionItem> deletionQueue = new DelayQueue<>();
    private final Thread deletionThread;
    private final FlushCoordinator flushCoordinator;

    // todo
    private final Consumer<FlushedInfo> metadataUpdater;

    public static DynamicMmapStore getInstance(String basePath, String id) {
        return SingletonContainer.getInstance(DynamicMmapStore.class, basePath, id);
    }

    // 加载DynamicMmapStore时，需要做很多工作，并不仅仅是创建一个新的lmdb实例这么简单
    // 比如，先查一下是否有已经存在的mmap文件，如果有，
    // 加载成只读 or 可写实例（根据文件大小或者其他标记实现判断）
    // 然后就是判断是否需要从wal进行回放生成lmdb实例，因为要实现lmdb的容错，当某个executor失败，触发重建executor操作的时候，
    // 需要回放down掉的executor上所有没有被flush的lmdb实例
    // 这里就有一个要求，wal机制，要能标识出怎么回放。也就是说，当某个lmdb被flush后，也要往wal中插入一条数据
    private DynamicMmapStore(String basePath) {
        this.basePath = basePath;
        // todo read from conf
        this.instanceMappedBytes = 128 * 1024 * 1024;

        this.globalLock = new ReentrantLock();

        // todo make sure this is right
        this.totalMemoryLimitBytes = Runtime.getRuntime().maxMemory();
        this.totalRecords = new ConcurrentHashMap<>();

        this.instances = Caffeine.newBuilder()
                .maximumSize(totalMemoryLimitBytes / instanceMappedBytes)
                .evictionListener(this::onEviction)
                .buildAsync(key -> key);

        // todo read from conf
        this.writableNum = Runtime.getRuntime().availableProcessors();
        this.remoteBasePath = "";

        loadExistInstance();

        this.flushExecutor = Executors.newFixedThreadPool(1);

        this.monitorService = Executors.newScheduledThreadPool(1);
        this.monitorService.scheduleAtFixedRate(this::monitorMemory, 5, 5, TimeUnit.MINUTES);
        this.flushCoordinator = new DefaultFlushCoordinator(this::tableStore);

        this.deletionThread = new Thread(this::processDeletions, "Lmdb-Deletion-Thread");
        deletionThread.setDaemon(true);
        deletionThread.start();

        this.metadataUpdater = result -> {
            if (result.success()) {
                log.info("Meta updated: Instance {} flushed to {}", result.instanceId(), result.hdfsPath());
                // TODO: RPC call to Driver MetaStore to update Partition -> File mapping
            }
        };
    }

    public StoreState currentState() {
        // 记录最近n次事务的状态
        return null;
    }

    public TableStore tableStore(String tableIdentifier) {
        return tableStores.computeIfAbsent(tableIdentifier, id -> new TableStore(id, writableNum));
    }

    public TableStore tableStore(String tableIdentifier, PrimaryKey primaryKey) {
        return tableStores.computeIfAbsent(tableIdentifier, id -> new TableStore(id, writableNum, primaryKey));
    }

    public TableStore tableStore(String tableIdentifier, List<String> primaryKeyFields) {
        return tableStores.computeIfAbsent(tableIdentifier, id -> new TableStore(id, writableNum, primaryKeyFields));
    }

    public TableStorage tableStorage(TableId tableId) {
        return tableStores.values().stream()
                .filter(store -> store.tableId().equals(tableId))
                .findFirst()
                .orElse(null);
    }

//    public LmdbInstance getStoreForPartition(int partitionId) {
//        String storeId = "writable-part-" + partitionId;
//        LmdbInstance store = writableStores.get(storeId);
//        if (store == null) {
//            throw new IllegalStateException("Store not initialized for partition " + partitionId);
//        }
//        return store;
//    }

    public void close() throws Exception {
        monitorService.shutdown();
        flushExecutor.shutdown();
        try {
            if (!monitorService.awaitTermination(5, TimeUnit.SECONDS)) {
                monitorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            monitorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        tableStores.forEach((key, tableStore) -> tableStore.close());
    }

    private void waitForResource() {
        try {
            Thread.sleep(100);
            triggerFlushAsync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean checkMemoryAvailable() {
        return totalUsedMemory.get() + instanceMappedBytes < totalMemoryLimitBytes * 0.9;
    }

    private long calculateTotalUsedMemory() {
        AtomicLong total = new AtomicLong();
        instances.asMap().values().forEach(instanceId -> {
            try {
                addMemory(getLmdbInstance(instanceId.get()).getTotalUsageSize());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        return total.get();
    }

    private void monitorMemory() {
        if (totalUsedMemory.get() > totalMemoryLimitBytes * 0.9) {
            log.warn("Memory usage high, triggering async flush");
            triggerFlushAsync();
        }
    }

    private void triggerFlushAsync() {
        coldest().ifPresent(inst -> instances.synchronous().invalidate(inst));
    }

    private void triggerFlushSync() {
        // 同步阻塞 Flush，简单等待 Flush 完成释放内存，实际可以配合 CountDownLatch
        coldest().ifPresent(instanceId -> flushAndScheduleDeletion(getLmdbInstance(instanceId)));
    }

    private void onEviction(String instanceId, String value, RemovalCause cause) {
        log.info("Instance {} evicted due to {}", instanceId, cause);
        if (cause == RemovalCause.SIZE || cause == RemovalCause.EXPLICIT) {
            LmdbInstance instance = getLmdbInstance(instanceId);
            flushExecutor.submit(() -> flushAndScheduleDeletion(instance));
        }
    }

    // 标记删除 -> Flush HDFS -> 更新元数据 -> 加入删除队列
    private void flushAndScheduleDeletion(LmdbInstance instance) {
        String tableIdentifier = findTableIdentifier(instance.instanceId());
        FlushPlan plan = flushCoordinator.plan(tableIdentifier, instance, remoteBasePath);
        flushCoordinator.beforeFlush(tableIdentifier, instance);

        try {
            log.info("Planned flush {} for instance {} to {}",
                    plan.planId(), instance.instanceId(), plan.targetLocation().storagePath().location());
            flushCoordinator.afterFlush(tableIdentifier, instance, plan);
            deletionQueue.offer(new PendingDeletionItem(instance, 30_000));
        } catch (Exception e) {
            flushCoordinator.onFlushFailure(tableIdentifier, instance, e);
            log.error("Flush failed for instance {}", instance.instanceId(), e);
        }
    }

    public LmdbInstance getLmdbInstance(String instanceId) {
        for (TableStore tableStore : tableStores.values()) {
            LmdbInstance instance = tableStore.getInstance(instanceId);
            if (instance != null) {
                return instance;
            }
        }
        return null;
    }

    private void loadExistInstance() {
        log.info("Starting to load existing LMDB instances from {}", basePath);
        Path baseDir = Paths.get(basePath);
        if (!Files.exists(baseDir)) {
            return;
        }
        // tableIdentifier, lmdbPath
        List<Pair<String, Path>> matchingPaths;
        try {
            matchingPaths = findMatchingPaths(basePath)
                    .stream()
                    .map(path -> Pair.of(path.getFileName().toString().split("-")[1], path)).toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        matchingPaths.forEach(pair -> {
            String tableIdentifier = pair.first;
            try (var instancePaths = Files.list(pair.second)) {
                instancePaths.forEach(path -> {
                    String instanceId = path.getFileName().toString();
                    if (instanceId.startsWith("inst-")) {
                        try {
                            if (!checkMemoryAvailable()) {
                                triggerFlushSync();
                                if (!checkMemoryAvailable()) {
                                    log.error("Still no memory after flush. Skip loading {}", instanceId);
                                }
                            } else {
                                LmdbInstance instance = new LmdbInstance(
                                        instanceId, path, instanceMappedBytes);
                                addMemory(instance.getTotalUsageSize());
                                instances.put(instanceId, CompletableFuture.completedFuture(instanceId));
                                TableStore tableStore = new TableStore(tableIdentifier, writableNum);
                                if (instance.readOnly()) {
                                    tableStore.addReadOnlyInstance(instance);
                                } else {
                                    if (tableStore.remainingWritable() < writableNum) {
                                        tableStore.addWritableInstance(instance);
                                    } else {
                                        instance.markAsReadOnly();
                                        tableStore.addReadOnlyInstance(instance);
                                        log.warn("Max writable limit reached. Loaded instance {} as read-only.", instanceId);
                                    }
                                    tableStore.addWritableInstance(instance);
                                }
                                tableStores.put(tableIdentifier, tableStore);
                                log.info("Loaded existing instance: {}", instanceId);
                            }
                        } catch (Exception e) {
                            log.error("Failed to load instance: {}, exception is {}", instanceId,
                                    Throwables.getStackTraceAsString(e));
                        }
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void addMemory(long byteSize) {
        this.totalUsedMemory.addAndGet(byteSize);
    }

    private void subMemory(long byteSize) {
        this.totalUsedMemory.addAndGet(-byteSize);
    }

    private void processDeletions() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 阻塞直到时间到
                PendingDeletionItem item = deletionQueue.take();
                LmdbInstance inst = item.instance;

                if (inst.tryWriteLock()) {
                    try {
                        log.info("Destroying physical files for instance {}", inst.instanceId());
                        inst.destroy();
                        subMemory(instanceMappedBytes);
                    } finally {
                        inst.unlockWrite();
                    }
                } else {
                    // 有读事务还在进行，重新加入队列延迟删除
                    log.warn("Instance {} still has active readers, retry deletion later", inst.instanceId());
                    // 等10秒再试
                    deletionQueue.offer(new PendingDeletionItem(inst, 10_000));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void createNewInstance(String tableIdentifier) {
        String instanceId = generateInstanceId(tableIdentifier);
        LmdbInstance instance = new LmdbInstance(instanceId, Paths.get(basePath), instanceMappedBytes);
        tableStore(tableIdentifier).addWritableInstance(instance);
        addMemory(instanceMappedBytes);
        instances.put(instanceId, CompletableFuture.completedFuture(instanceId));
        log.info("Created new LMDB instance: {}", instanceId);
    }

    private LmdbInstance currentLmdbInstance(String tableIdentifier) {
        TableStore store = tableStore(tableIdentifier);
        LmdbInstance instance = store.getCurrentWritableInstance();
        if (instance == null) {
            while (!tryAllocateNewWritableInstance(tableIdentifier)) {
                // 内存不足且无法立即 Flush，等待
                waitForResource();
            }
            instance = store.getCurrentWritableInstance();
        }
        return instance;
    }

    private String findTableIdentifier(String instanceId) {
        return tableStores.entrySet().stream()
                .filter(entry -> entry.getValue().getInstance(instanceId) != null)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown instanceId: " + instanceId));
    }

    private boolean tryAllocateNewWritableInstance(String tableIdentifier) {
        globalLock.lock();
        try {
            // 1. 检查是否超过最大实例数限制
            // 2. 检查内存是否足够
            if (!checkMemoryAvailable()) {
                // 尝试同步 Flush 释放空间
                triggerFlushSync();
                if (!checkMemoryAvailable()) {
                    // 依然不够，等待异步 Flush 完成
                    return false;
                }
            }

            createNewInstance(tableIdentifier);
            return true;
        } finally {
            globalLock.unlock();
        }
    }


    private Optional<String> coldest() {
        final String[] coldestKey = new String[1];
        Optional<Policy.Eviction<String, String>> evictionOpt = instances.synchronous().policy().eviction();
        evictionOpt.ifPresent(eviction -> {
            Map<String, String> coldest = eviction.coldest(1);
            if (!coldest.isEmpty()) {
                coldestKey[0] = coldest.keySet().iterator().next();
                log.info(" coldest key (via synchronous view): {}", coldestKey[0]);
            }
        });
        return Optional.ofNullable(coldestKey[0]);
    }

    private boolean switchToNewInstance(String tableIdentifier) {
        globalLock.lock();
        try {
            tableStores.get(tableIdentifier).rotateWritableInstance();
            if (!checkMemoryAvailable()) {
                // 内存不足，触发同步 Flush（阻塞当前写线程，直到释放出空间）
                triggerFlushSync();
                if (!checkMemoryAvailable()) {
                    return false;
                }
            }

            createNewInstance(tableIdentifier);
            return true;
        } finally {
            globalLock.unlock();
        }
    }

    public void write(WriteBuilder builder) {
        String tableIdentifier = builder.table().uniqId();
        TableStore tableStore = tableStore(tableIdentifier);
        LmdbInstance current = null;
        while (current == null) {
            current = currentLmdbInstance(tableIdentifier);
            if (current == null) {
                createNewInstance(tableIdentifier);
                continue;
            }

            if (current.readOnly() || current.isMarkedForDeletion()) {
                if (!switchToNewInstance(tableIdentifier)) {
                    // 切换失败（通常是内存不足且无法立即 Flush）,简单自旋等待，或抛异常，这里选择短暂等待重试
                    LockSupport.parkNanos(100_000_000);
                }
            }
        }

        if (current.tryEnterScope()) {
            try {
                Write.Result result;
                try {
                    result = tableStore.write(builder, current);
                } catch (IOException e) {
                    throw new RuntimeException("Write routing failed for table " + tableIdentifier, e);
                }

                if (result.success()) {
                    addMemory(result.count());
                    totalRecords.merge(tableIdentifier,
                            new AtomicInteger(1),
                            (a, b) -> {
                                a.getAndIncrement();
                                return a;
                            });
                } else if (LmdbWriter.MAPFULL_MESSAGE.equals(result.message().orElse(""))) {
                    current.markAsReadOnly();
                }
            } finally {
                current.exitScope();
            }
        }
    }

    public Read.Result read(Read read) throws IOException {
        if (read.getDataBlocks().isEmpty()) {
            return Scan.Result.error("Read routing requires planned DataBlocks", null);
        }
        String tableIdentifier = read.getDataBlocks().getFirst().tableId();
        return tableStore(tableIdentifier).read(read);
    }

    private record PendingDeletionItem(LmdbInstance instance, long expireTime) implements Delayed {
        private PendingDeletionItem(LmdbInstance instance, long expireTime) {
            this.instance = instance;
            this.expireTime = System.currentTimeMillis() + expireTime;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expireTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(expireTime, ((PendingDeletionItem) o).expireTime);
        }
    }

    public record FlushedInfo(String instanceId, String hdfsPath, boolean success, Throwable t) {
    }
}
