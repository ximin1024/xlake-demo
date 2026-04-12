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

import java.io.Serializable;

public record ShardAssignment(
        ShardId shardId,
        NodeSlot nodeSlot,
        String executorId,
        RoutingEpoch epoch
) implements Serializable {
    public ShardAssignment {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (nodeSlot == null) {
            throw new IllegalArgumentException("nodeSlot must not be null");
        }
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        if (epoch == null) {
            throw new IllegalArgumentException("epoch must not be null");
        }
    }
}
