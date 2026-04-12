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
package io.github.ximin.xlake.backend.spark;

import io.github.ximin.xlake.backend.routing.RoutingStatus;
import io.github.ximin.xlake.backend.routing.ShardAssignment;
import io.github.ximin.xlake.backend.routing.ShardId;
import io.github.ximin.xlake.backend.routing.ShardLookupResult;
import io.github.ximin.xlake.backend.spark.routing.SparkRoutingBridge;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkExecutorInfo;

import java.util.*;

public record XlakeWriteTopology(Map<String, Integer> executorsPerHost) {
    public static XlakeWriteTopology inspect(SparkContext sparkContext) {
        LinkedHashMap<String, Integer> perHost = new LinkedHashMap<>();
        for (SparkExecutorInfo executorInfo : sparkContext.statusTracker().getExecutorInfos()) {
            if (executorInfo == null || executorInfo.host() == null || executorInfo.host().isBlank()) {
                continue;
            }
            perHost.merge(executorInfo.host(), 1, Integer::sum);
        }
        return new XlakeWriteTopology(Map.copyOf(perHost));
    }

    public boolean supportsPreferredLocations() {
        return !executorsPerHost.isEmpty()
                && executorsPerHost.values().stream().allMatch(count -> count == 1);
    }

    public Map<Integer, List<String>> preferredLocationsByShard(int shardCount) {
        return preferredLocationsByShard(shardCount, null);
    }

    public Map<Integer, List<String>> preferredLocationsByShard(int shardCount, String driverHost) {
        if (!supportsPreferredLocations() || shardCount <= 0) {
            return Map.of();
        }

        try {
            Set<Integer> shardIds = new LinkedHashSet<>();
            for (int shardId = 0; shardId < shardCount; shardId++) {
                shardIds.add(shardId);
            }
            Map<ShardId, ShardLookupResult> lookups = SparkRoutingBridge.lookupOwners(shardIds);
            return buildPreferredLocationsByShard(
                    new HashSet<>(executorsPerHost.keySet()),
                    driverHost,
                    lookups
            );
        } catch (Exception ignored) {
            return Map.of();
        }
    }

    static Map<Integer, List<String>> buildPreferredLocationsByShard(
            Set<String> activeHosts,
            String driverHost,
            Map<ShardId, ShardLookupResult> lookups
    ) {
        if (activeHosts == null || activeHosts.isEmpty() || lookups == null || lookups.isEmpty()) {
            return Map.of();
        }
        String normalizedDriverHost = (driverHost == null || driverHost.isBlank()) ? null : driverHost.trim();

        LinkedHashMap<Integer, List<String>> preferred = new LinkedHashMap<>();
        for (Map.Entry<ShardId, ShardLookupResult> entry : lookups.entrySet()) {
            ShardLookupResult lookup = entry.getValue();
            if (lookup == null || lookup.status() != RoutingStatus.ASSIGNED) {
                continue;
            }
            ShardAssignment assignment = lookup.assignment();
            if (assignment == null || assignment.nodeSlot() == null) {
                continue;
            }
            String host = assignment.nodeSlot().nodeId();
            if (host == null || host.isBlank()) {
                continue;
            }
            // Only prefer hosts that are currently alive/active, and never prefer the driver host.
            if (!activeHosts.contains(host)) {
                continue;
            }
            if (normalizedDriverHost != null && normalizedDriverHost.equals(host)) {
                continue;
            }
            preferred.put(entry.getKey().value(), List.of(host));
        }
        return preferred.isEmpty() ? Map.of() : Map.copyOf(preferred);
    }
}
