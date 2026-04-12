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
package io.github.ximin.xlake.backend.spark.routing;

import io.github.ximin.xlake.backend.routing.ShardLookupResult;

import java.util.*;
import java.util.function.Function;


public final class ShardOwnerCache {
    private final Function<Set<Integer>, Map<Integer, ShardLookupResult>> batchLookup;
    private final HashMap<Integer, ShardLookupResult> cache = new HashMap<>();

    public ShardOwnerCache(Function<Set<Integer>, Map<Integer, ShardLookupResult>> batchLookup) {
        this.batchLookup = Objects.requireNonNull(batchLookup, "batchLookup must not be null");
    }

    public Map<Integer, ShardLookupResult> get(Set<Integer> shardIds) {
        return getInternal(shardIds, false);
    }

    public Map<Integer, ShardLookupResult> refresh(Set<Integer> shardIds) {
        return getInternal(shardIds, true);
    }

    private Map<Integer, ShardLookupResult> getInternal(Set<Integer> shardIds, boolean forceRefresh) {
        Objects.requireNonNull(shardIds, "shardIds must not be null");
        if (shardIds.isEmpty()) {
            return Map.of();
        }

        HashSet<Integer> missing = new HashSet<>();
        if (forceRefresh) {
            missing.addAll(shardIds);
        } else {
            for (Integer sid : shardIds) {
                if (!cache.containsKey(sid)) {
                    missing.add(sid);
                }
            }
        }

        if (!missing.isEmpty()) {
            Map<Integer, ShardLookupResult> looked = batchLookup.apply(Set.copyOf(missing));
            if (looked != null) {
                cache.putAll(looked);
            }
        }

        HashMap<Integer, ShardLookupResult> out = new HashMap<>(Math.max(16, shardIds.size() * 2));
        for (Integer sid : shardIds) {
            out.put(sid, cache.get(sid));
        }
        return out;
    }
}
