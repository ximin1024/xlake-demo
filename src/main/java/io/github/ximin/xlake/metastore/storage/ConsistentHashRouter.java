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
package io.github.ximin.xlake.metastore.storage;

import io.github.ximin.xlake.common.hash.MurmurHash3;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class ConsistentHashRouter {

    private final ConcurrentSkipListMap<Long, Integer> ring = new ConcurrentSkipListMap<>();
    private final int virtualNodesPerShard;
    private final int numShards;

    public ConsistentHashRouter(int numShards, int virtualNodesPerShard) {
        this.numShards = numShards;
        this.virtualNodesPerShard = virtualNodesPerShard;
        initRing();
    }

    public ConsistentHashRouter(int numShards) {
        this(numShards, 150);
    }

    private void initRing() {
        for (int shardId = 0; shardId < numShards; shardId++) {
            addShard(shardId);
        }
        log.info("ConsistentHashRing initialized: {} physical shards, {} virtual nodes each",
                numShards, virtualNodesPerShard);
    }

    public void addShard(int shardId) {
        for (int i = 0; i < virtualNodesPerShard; i++) {
            long hash = hash(shardId + "-vn-" + i);
            ring.put(hash, shardId);
        }
    }

    public void removeShard(int shardId) {
        for (int i = 0; i < virtualNodesPerShard; i++) {
            long hash = hash(shardId + "-vn-" + i);
            ring.remove(hash);
        }
    }

    public int getShard(byte[] key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("Ring is empty");
        }
        long hash = hashBytes(key);
        Map.Entry<Long, Integer> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    public int getShard(String key) {
        return getShard(key.getBytes(StandardCharsets.UTF_8));
    }

    public Set<Integer> getCandidateShardsForPrefix(byte[] prefix) {
        if (ring.isEmpty()) {
            return Set.of();
        }
        Set<Integer> candidates = new HashSet<>();
        long prefixHash = hashBytes(prefix);

        Map.Entry<Long, Integer> ceiling = ring.ceilingEntry(prefixHash);
        if (ceiling != null) {
            NavigableSet<Map.Entry<Long, Integer>> tail = (NavigableSet<Map.Entry<Long, Integer>>) ring.tailMap(ceiling.getKey(), true).entrySet();
            for (var entry : tail) {
                candidates.add(entry.getValue());
                if (candidates.size() >= numShards) break;
            }
        }
        if (candidates.size() < numShards) {
            for (var entry : ring.entrySet()) {
                candidates.add(entry.getValue());
                if (candidates.size() >= numShards) break;
            }
        }
        return Collections.unmodifiableSet(candidates);
    }

    public Set<Integer> getAllShards() {
        return new HashSet<>(ring.values());
    }

    private long hash(String key) {
        return hash(Arrays.toString(key.getBytes(StandardCharsets.UTF_8)));
    }

    private long hashBytes(byte[] data) {
        return MurmurHash3.hash64(data) & 0x7FFFFFFFFFFFFFFFL;
    }

    public int getNumShards() {
        return numShards;
    }

    public Map<Integer, Double> getDistribution() {
        Map<Integer, Long> counts = new HashMap<>();
        ring.values().forEach(v -> counts.merge(v, 1L, Long::sum));
        long total = ring.size();
        Map<Integer, Double> result = new HashMap<>();
        counts.forEach((k, v) -> result.put(k, (double) v / total));
        return result;
    }
}
