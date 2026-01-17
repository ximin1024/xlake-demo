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

import com.github.benmanes.caffeine.cache.*;
import com.google.common.collect.Iterators;
import com.ibm.icu.impl.Pair;
import io.github.ximin.xlake.backend.spark.KvInternalRow;
import io.github.ximin.xlake.table.StoreState;
import io.github.ximin.xlake.table.schema.Schema;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static io.github.ximin.xlake.common.utls.FileUtils.findMatchingPaths;


@Slf4j
public class DynamicMmapStore implements AutoCloseable {
    private final String basePath;
    private final long instanceMappedSizeMB;

    // 监控是否有read only的 lmdb产生，这里想一下将这个功能内聚到这里，还是在外部进行操作
    private final ScheduledExecutorService monitorService;
    private final Arena arena;
    private final ReentrantLock switchLock;
    private final long totalMemoryLimit;
    private final ExecutorService taskService;
    private final Map<String, AtomicInteger> instanceNumMap;
    private final Map<String, AtomicInteger> totalRecords;
    private final Map<String, Deque<String>> queuedInstanceId;
    private AsyncLoadingCache<String, LmdbInstance> instances;
    private final Schema schema;

    public DynamicMmapStore(String basePath, long instanceSizeMB, Schema schema,
                            WriteSupport<InternalRow> writeSupport) {
        this.basePath = basePath;
        this.instanceMappedSizeMB = instanceSizeMB;
        this.queuedInstanceId = new ConcurrentHashMap<>();
        this.instanceNumMap = new ConcurrentHashMap<>();
        this.monitorService = Executors.newSingleThreadScheduledExecutor();
        this.arena = Arena.ofConfined();
        this.switchLock = new ReentrantLock();
        this.totalMemoryLimit = Runtime.getRuntime().maxMemory();
        this.taskService = Executors.newVirtualThreadPerTaskExecutor();
        this.totalRecords = new ConcurrentHashMap<>();
        this.schema = schema;
        this.instances = Caffeine.newBuilder()
                // 最大缓存1000个条目
                .maximumSize(10)
                // 设置自定义过期策略：根据存入的CacheData对象内的duration计算过期时间
                .expireAfter(new CustomExpiry())
                // 设置调度器，用于及时删除过期条目（尤其在写入不频繁的缓存中
                // 对于Java 9及以上，可以使用Scheduler.systemScheduler()来使用系统调度线程
                .scheduler(Scheduler.systemScheduler())
                // 设置移除监听器，处理条目移除事件（包括过期移除）
                .removalListener(new CustomRemovalListener(writeSupport, schema, ""))
                // 构建异步缓存，默认使用CompletableFuture.supplyAsync在ForkJoinPool.commonPool()中执行加载任务
                // 你也可以通过.executor()方法指定自定义线程池
                .buildAsync(new CustomCacheLoader());
        loadExistInstance();
    }

    public LmdbInstance.WriteResult put(String uniqTableIdentifier, byte[] key, byte[] value) {
        while (true) {
            LmdbInstance current = currentLmdbInstance(uniqTableIdentifier);
            if (current == null) {
                createNewInstance(uniqTableIdentifier);
                continue;
            }
            if (current.readOnly()) {
                if (!switchToNewInstance(uniqTableIdentifier)) {
                    return LmdbInstance.WriteResult.error(current.instanceId(),
                            "Failed to switch new instance when current is read-only");
                }
                continue;
            }

            LmdbInstance.WriteResult result = current.put(key, value);

            if (result.isSuccess()) {
                totalRecords.merge(uniqTableIdentifier,
                        new AtomicInteger(1),
                        (a, b) -> {
                            a.getAndIncrement();
                            return a;
                        });
                return result;
            } else if (result.isMapFull()) {
                current.markAsReadOnly();
                if (!switchToNewInstance(uniqTableIdentifier)) {
                    return LmdbInstance.WriteResult.error(result.instanceId(),
                            "Switch to new lmdb instance failed after MAP_FULL");
                }
            } else {
                return result;
            }
        }
    }

    public StoreState currentState() {
        // 记录最近n次事务的状态
        return null;
    }

    private LmdbInstance currentLmdbInstance(String uniqTableIdentifier) {
        Deque<String> instanceIds = queuedInstanceId.get(uniqTableIdentifier);
        if (instanceIds == null) {
            return null;
        }
        String currentInstanceId = instanceIds.peekLast();
        if (currentInstanceId == null) {
            throw new NoSuchElementException("No current instance found for uniq table " + uniqTableIdentifier);
        }
        CompletableFuture<LmdbInstance> current = instances.get(currentInstanceId);
        try {
            return current.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    // todo 这里是否得有个read write lock，原子的标记某个lmdb要被淘汰了
    private String coldest() {
        final String[] coldestKey = new String[1];
        Optional<Policy.Eviction<String, LmdbInstance>> evictionOpt = instances.synchronous().policy().eviction();
        evictionOpt.ifPresent(eviction -> {
            Map<String, LmdbInstance> coldest = eviction.coldest(1);
            if (!coldest.isEmpty()) {
                coldestKey[0] = coldest.keySet().iterator().next();
                log.info(" coldest key (via synchronous view): {}", coldestKey[0]);
            }
        });
        return coldestKey[0];
    }

    private boolean switchToNewInstance(String uniqTableIdentifier) {
        switchLock.lock();
        try {
            LmdbInstance oldInstance = currentInstance(uniqTableIdentifier);
            if (oldInstance == null) {
                log.warn("Current instance {} is null", uniqTableIdentifier);
                return false;
            }
            // todo 注意外层也套了一个while
            if (!checkMemoryLimit()) {
                // todo 内存不够，阻塞当前线程，先check是否有进行中的同步线程，如果没有，最高优强制进行同步线程
                //  淘汰当下最优的readonly的lmdb
                String coldest = coldest();
                if (coldest == null) {
                    log.warn("coldest is null for uniq table {}", uniqTableIdentifier);
                    return false;
                }
                instances.synchronous().invalidate(coldest);
                // todo 可通过监听机制，拿到缓存失效的处理结果
            }

            createNewInstance(uniqTableIdentifier);
            return true;
        } finally {
            switchLock.unlock();
        }
    }

    private boolean checkMemoryLimit() {
        long totalUsedMemory = calculateTotalUsedMemory();
        return totalUsedMemory + instanceMappedSizeMB <= totalMemoryLimit;
    }

    private long calculateTotalUsedMemory() {
        AtomicLong total = new AtomicLong();
        instances.asMap().values().forEach(lmdbInstance -> {
            try {
                total.addAndGet(lmdbInstance.get().getTotalUsageSize());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        return total.get();
    }

    private void checkIfMeetsRequirements() {
        // todo determine whether there is memory space to create new LmdbInstance
        // if fail send events to trigger flush
    }


    private void loadExistInstance() {
        List<Pair<String, Path>> matchingPaths;
        try {
            matchingPaths = findMatchingPaths(basePath)
                    .stream()
                    .map(path -> Pair.of(path.getFileName().toString().split("_")[1], path)).toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        matchingPaths.forEach(pair -> {
            Deque<String> instanceQueue = this.queuedInstanceId.getOrDefault(pair.first, new ConcurrentLinkedDeque<>());
            try (var instancePaths = Files.list(pair.second)) {
                instancePaths.forEach(path -> {
                    String instanceId = path.getFileName().toString();
                    if (instanceId.startsWith("instance-")) {
                        try {
                            LmdbInstance instance = new LmdbInstance(
                                    instanceId, path, instanceMappedSizeMB, arena);
                            instanceQueue.add(instanceId);
                            instances.put(instanceId, CompletableFuture.completedFuture(instance));
                            instanceNumMap.merge(pair.first,
                                    new AtomicInteger(1),
                                    (a, b) -> {
                                        a.getAndIncrement();
                                        return a;
                                    });
                            System.out.println("Loaded existing instance: " + instanceId);
                        } catch (Exception e) {
                            System.err.println("Failed to load instance: " + instanceId);
                        }
                    }
                });
                queuedInstanceId.put(pair.first, instanceQueue);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String tablePath(String uniqTableIdentifier) {
        return basePath + "_" + uniqTableIdentifier;
    }

    private void createNewInstance(String tableIdentifier) {
        LmdbInstance current = currentLmdbInstance(tableIdentifier);
        if (current != null) {
            current.markAsReadOnly();
        }
        String instanceId = generateInstanceId();
        LmdbInstance instance =
                new LmdbInstance(instanceId, basePath, instanceMappedSizeMB, arena);
        instanceNumMap.merge(tableIdentifier, new AtomicInteger(1), (a, b) -> {
            a.getAndIncrement();
            return a;
        });
        queuedInstanceId.computeIfAbsent(tableIdentifier, k -> new ConcurrentLinkedDeque<>()).add(instanceId);
        instances.put(instanceId, CompletableFuture.completedFuture(instance));
    }

    // todo 是否并发安全
    private String generateInstanceId() {
        return "instance-" + System.currentTimeMillis();
    }

    public String get(String key) {
        // todo 根据key range搜索
        return null;
    }

    public LmdbInstance currentInstance(String tableIdentifier) {
        Deque<String> queued = queuedInstanceId.get(tableIdentifier);
        if (queued != null) {
            String instanceId = queued.peekLast();
            try {
                assert instanceId != null;
                return instances.get(instanceId).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        monitorService.shutdown();
        try {
            if (!monitorService.awaitTermination(5, TimeUnit.SECONDS)) {
                monitorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            monitorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        for (CompletableFuture<LmdbInstance> instance : instances.asMap().values()) {
            try {
                instance.get().close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private static class CustomExpiry implements Expiry<String, LmdbInstance> {

        @Override
        public long expireAfterCreate(String key, LmdbInstance value, long currentTime) {
            return 0;
        }

        @Override
        public long expireAfterUpdate(String key, LmdbInstance value, long currentTime, long currentDuration) {
            return 0;
        }

        @Override
        public long expireAfterRead(String key, LmdbInstance value, long currentTime, long currentDuration) {
            return 0;
        }
    }

    private record CustomRemovalListener(WriteSupport<InternalRow> writeSupport,
                                         Schema schema,
                                         String remoteBasePath)
            implements RemovalListener<String, LmdbInstance> {
        @Override
        public void onRemoval(String key, LmdbInstance value, RemovalCause cause) {
            // TODO 清除缓存时进行原子更新，更新range数据分布信息
            switch (cause) {
                case REPLACED, EXPIRED, SIZE, EXPLICIT -> flushInternal(Iterators.transform(value.iterator(),
                                input -> new KvInternalRow(input.getKey(), input.getValue(), schema)),
                        value.instanceId());
            }
        }

        private void flushInternal(Iterator<InternalRow> iterator, String instanceId) {
            String remotePath = remoteBasePath + "/" + instanceId;
            ParquetFlusher<InternalRow> flusher = new ParquetFlusher<>(iterator, remotePath, writeSupport);
            flusher.flush();
        }
    }

    private static class CustomCacheLoader implements CacheLoader<String, LmdbInstance> {
        @Override
        public LmdbInstance load(String key) throws Exception {
            // 当缓存未命中时，提供默认的加载逻辑
            // 例如，从数据库、文件或外部服务加载数据
            // 这里返回一个默认值，实际应用中可根据key进行加载
            System.out.println(">>> 通过CacheLoader加载数据，键: " + key);
            //return new CacheData("默认加载的数据", Duration.ofSeconds(60)); // 默认过期时间60秒
            return null;
        }
    }
}
