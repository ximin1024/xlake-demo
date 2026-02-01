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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class ContainerMemoryAdvisor {
    private static final ContainerMemoryAdvisor INSTANCE = new ContainerMemoryAdvisor();

    // 避免频繁读取 /proc
    private static final long CACHE_TTL_MS = 1000L; // 1秒

    private volatile CacheEntry cache = null;
    private final ReentrantLock lock = new ReentrantLock();

    private static final double SAFETY_MARGIN_RATIO = 0.1;
    private static final long MIN_SAFETY_MARGIN_BYTES = 2L * 1024 * 1024 * 1024; // 2GB

    private ContainerMemoryAdvisor() {}

    public static ContainerMemoryAdvisor getInstance() {
        return INSTANCE;
    }

    /**
     * 获取可用于 native 内存（如 LMDB）的安全字节数。
     * @param maxRetries 重试次数（默认 3）
     * @return 可用字节数（>=0），若无法确定则返回 0
     */
    public long getAvailableNativeMemory(int maxRetries) {
        return withRetry(maxRetries, this::computeAvailableNativeMemory);
    }

    public long getAvailableNativeMemory() {
        return getAvailableNativeMemory(3);
    }

    private long computeAvailableNativeMemory() {
        CacheEntry current = cache;
        long now = System.currentTimeMillis();
        if (current != null && now - current.timestamp < CACHE_TTL_MS) {
            return current.availableBytes;
        }

        lock.lock();
        try {
            current = cache;
            if (current != null && now - current.timestamp < CACHE_TTL_MS) {
                return current.availableBytes;
            }

            long limit = readMemoryLimit();
            if (limit <= 0 || limit == Long.MAX_VALUE) {
                // 无限制环境（如裸机），保守返回 0 或极大值？这里选择返回 0，强制用户显式配置
                return 0;
            }

            long rss = getCurrentRss();
            if (rss <= 0) {
                return 0;
            }

            long safetyMargin = Math.max(
                    (long) (limit * SAFETY_MARGIN_RATIO),
                    MIN_SAFETY_MARGIN_BYTES
            );

            long available = limit - rss - safetyMargin;
            available = Math.max(0, available);

            this.cache = new CacheEntry(available, now);
            return available;

        } catch (Exception e) {
            System.err.println("Failed to compute available native memory: " + e.getMessage());
            return 0;
        } finally {
            lock.unlock();
        }
    }

    private <T> T withRetry(int maxRetries, Supplier<T> action) {
        Exception lastEx = null;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return action.get();
            } catch (Exception e) {
                lastEx = e;
                if (i < maxRetries - 1) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                }
            }
        }
        throw new RuntimeException("All retries failed", lastEx);
    }

    private long readMemoryLimit() throws IOException {
        // cgroup v1
        Path[] v1Paths = {
                Paths.get("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
                Paths.get("/sys/fs/cgroup/memory.limit_in_bytes")
        };

        for (Path p : v1Paths) {
            if (Files.exists(p)) {
                String line = Files.readAllLines(p).get(0).trim();
                long value = Long.parseLong(line);
                if (value > Long.MAX_VALUE / 2) return Long.MAX_VALUE;
                return value;
            }
        }

        // cgroup v2
        Path v2Path = Paths.get("/sys/fs/cgroup/memory.max");
        if (Files.exists(v2Path)) {
            String line = Files.readAllLines(v2Path).get(0).trim();
            if ("max".equals(line)) return Long.MAX_VALUE;
            return Long.parseLong(line);
        }

        return -1;
    }

    private long getCurrentRss() throws IOException {
        long pid = ProcessHandle.current().pid();
        Path statmPath = Paths.get("/proc/" + pid + "/statm");
        if (!Files.exists(statmPath)) {
            throw new IOException("Missing /proc/" + pid + "/statm");
        }

        String line = Files.readAllLines(statmPath).get(0);
        String[] fields = line.trim().split("\\s+");
        if (fields.length < 2) {
            throw new IOException("Invalid statm format");
        }

        long rssPages = Long.parseLong(fields[1]);
        // 假设 page size = 4KB
        return rssPages * 4096L;
    }

    private static class CacheEntry {
        final long availableBytes;
        final long timestamp;

        CacheEntry(long availableBytes, long timestamp) {
            this.availableBytes = availableBytes;
            this.timestamp = timestamp;
        }
    }

    public static void main(String[] args) {
        ContainerMemoryAdvisor advisor = ContainerMemoryAdvisor.getInstance();
        long available = advisor.getAvailableNativeMemory();
        System.out.printf("Available native memory: %.2f GB%n", available / (1024.0 * 1024 * 1024));
    }
}
