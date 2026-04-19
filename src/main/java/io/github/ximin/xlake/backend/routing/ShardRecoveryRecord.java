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
import java.util.Set;

public record ShardRecoveryRecord(
        ShardId shardId,
        String previousExecutorId,
        String previousNodeSlotId,
        String previousBasePath,
        String walHdfsDir,
        Set<String> tableIdentifiers,
        RoutingEpoch epoch,
        long lostTimestamp
) implements Serializable {
    public ShardRecoveryRecord {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (previousExecutorId == null || previousExecutorId.isBlank()) {
            throw new IllegalArgumentException("previousExecutorId must not be blank");
        }
        if (previousNodeSlotId == null || previousNodeSlotId.isBlank()) {
            throw new IllegalArgumentException("previousNodeSlotId must not be blank");
        }
        if (epoch == null) {
            throw new IllegalArgumentException("epoch must not be null");
        }
        tableIdentifiers = tableIdentifiers == null ? Set.of() : Set.copyOf(tableIdentifiers);
    }
}
