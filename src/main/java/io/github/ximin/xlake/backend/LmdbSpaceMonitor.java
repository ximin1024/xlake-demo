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

import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.*;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class LmdbSpaceMonitor {
    private final Env<ByteBuffer> env;
    private long avgEntrySize = 1;

    public LmdbSpaceMonitor(Env<ByteBuffer> env) {
        this.env = env;
    }

    public record SpaceInfo(
            long mapSizeBytes,
            long usedBytes,
            long freeBytes,
            double usagePercentage,
            long freePages,
            long totalPages,
            boolean hasSpace,
            long entriesPerPage,
            long estimatedRemainingCapacity
    ) {
    }

    public void refresh(long avgEntrySize) {
        this.avgEntrySize = avgEntrySize;
    }

    public SpaceInfo getSpaceInfo() {
        try (Txn<ByteBuffer> txn = env.txnRead()) {

            EnvInfo envInfo = env.info();
            Stat stat = env.stat();

            long mapSize = envInfo.mapSize;
            long pageSize = stat.pageSize;
            long totalPages = envInfo.lastPageNumber + 1;
            long usedPages = calculateUsedPages(stat);
            long freePages = totalPages - usedPages;
            long entriesPerPage = pageSize / avgEntrySize;
            // 近似值
            long usedBytes = totalPages * pageSize;
            long freeBytes = freePages * pageSize;
            double usagePercentage = (double) usedBytes / mapSize * 100;
            long estimatedCapacity = freePages * entriesPerPage;

            return new SpaceInfo(
                    mapSize,
                    usedBytes,
                    freeBytes,
                    usagePercentage,
                    freePages,
                    totalPages,
                    // 至少10个空闲页面，可参数化
                    freePages > 10,
                    entriesPerPage,
                    estimatedCapacity
            );
        }
    }

    /**
     * 计算实际使用的页面数
     */
    private long calculateUsedPages(Stat stat) {
        // 基本页面使用估算
        // 每个B+树节点至少使用一个页面
        long minPages = stat.entries > 0 ? stat.entries : 0;
        // 考虑B+树的分支因子，更准确的计算
        long branchPages = calculateBranchPages(stat.entries);
        return Math.max(minPages, branchPages);
    }

    /**
     * 计算B+树分支页面数量
     */
    private long calculateBranchPages(long entryCount) {
        if (entryCount == 0) return 0;

        // todo 修改这个参数 B+树近似计算：假设每个节点有16个子节点
        final int BRANCHING_FACTOR = 16;
        long pages = 1; // 根节点

        long currentLevel = entryCount;
        while (currentLevel > 1) {
            currentLevel = (currentLevel + BRANCHING_FACTOR - 1) / BRANCHING_FACTOR;
            pages += currentLevel;
        }

        return pages;
    }

    private long calculateUsedDataSize(Txn<ByteBuffer> txn) {
        AtomicLong totalSize = new AtomicLong(0);
        env.getDbiNames().forEach(dbName -> {
            try {
                Dbi<ByteBuffer> dbi = env.openDbi(Arrays.toString(dbName));
                totalSize.addAndGet(calculateDbSize(txn, dbi));
            } catch (Exception e) {
                System.err.println(STR."Error calculating size for DB: \{dbName}");
            }
        });
        return totalSize.get();
    }

    private long calculateDbSize(Txn<ByteBuffer> txn, Dbi<ByteBuffer> dbi) {
        AtomicLong dbSize = new AtomicLong();
        try (CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
            cursor.forEach(kv -> {
                ByteBuffer key = kv.key();
                ByteBuffer value = kv.val();
                // 键大小 + 值大小 + LMDB内部开销（约16字节每条目）
                long entrySize = key.remaining() + value.remaining() + 16;
                dbSize.addAndGet(entrySize);
            });
        }
        return dbSize.get();
    }
    
    private long calculateIndexSize(Stat stat, long pageSize) {
        // 索引大小 ≈ B+树节点数量 × 页面大小
        long indexPages = calculateBranchPages(stat.entries);
        return indexPages * pageSize;
    }

    public long getPhysicalFileSize(String dbPath) {
        try {
            Path dataFile = Path.of(dbPath);
            if (Files.exists(dataFile)) {
                return Files.size(dataFile);
            }
        } catch (Exception e) {
            System.err.println(STR."Error getting physical file size: \{e.getMessage()}");
        }
        return 0;
    }
}
