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
package io.github.ximin.xlake.backend.routing;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ShardRoutingTable {
    ShardLookupResult lookup(ShardId shardId);

    Map<ShardId, ShardLookupResult> lookupBatch(Set<ShardId> shardIds);

    void registerExecutor(String executorId, NodeSlot nodeSlot);

    void markExecutorReady(String executorId);

    void unregisterExecutorAndReassign(String executorId);

    void assignShard(ShardAssignment assignment);

    void reassignShard(ShardId shardId);

    Collection<ShardAssignment> assignments();

    Collection<ShardLookupResult> routings();

    Optional<NodeSlot> nodeSlotOf(String executorId);

    Optional<NodeSlot> preferredSlotOf(ShardId shardId);
}
