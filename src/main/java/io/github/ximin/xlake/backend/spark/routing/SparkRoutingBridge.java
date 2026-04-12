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

import io.github.ximin.xlake.backend.routing.RoutingCoordinator;
import io.github.ximin.xlake.backend.routing.ShardId;
import io.github.ximin.xlake.backend.routing.ShardLookupResult;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public final class SparkRoutingBridge {
    private static final AtomicReference<RoutingCoordinator> DRIVER_COORDINATOR = new AtomicReference<>();

    private SparkRoutingBridge() {
    }

    public static void install(RoutingCoordinator coordinator) {
        DRIVER_COORDINATOR.set(Objects.requireNonNull(coordinator, "coordinator must not be null"));
    }

    public static void clear() {
        DRIVER_COORDINATOR.set(null);
    }

    public static RoutingCoordinator requireCoordinator() {
        RoutingCoordinator coordinator = DRIVER_COORDINATOR.get();
        if (coordinator == null) {
            throw new IllegalStateException("Spark routing coordinator is not installed on the driver");
        }
        return coordinator;
    }

    public static ShardLookupResult lookupOwner(int shardId) {
        return requireCoordinator().lookupOwner(new ShardId(shardId));
    }

    public static Map<ShardId, ShardLookupResult> lookupOwners(Set<Integer> shardIds) {
        Objects.requireNonNull(shardIds, "shardIds must not be null");
        Set<ShardId> resolvedShardIds = shardIds.stream()
                .map(ShardId::new)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return requireCoordinator().lookupOwners(resolvedShardIds);
    }
}
