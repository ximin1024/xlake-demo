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

public record ShardLookupResult(
        ShardId shardId,
        RoutingStatus status,
        ShardAssignment assignment
) implements Serializable {
    public ShardLookupResult {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (status == RoutingStatus.ASSIGNED && assignment == null) {
            throw new IllegalArgumentException("assignment must not be null when status is ASSIGNED");
        }
        if (status == RoutingStatus.PENDING_READY && assignment == null) {
            throw new IllegalArgumentException("assignment must not be null when status is PENDING_READY");
        }
        if (status == RoutingStatus.UNASSIGNED && assignment != null) {
            throw new IllegalArgumentException("assignment must be null when status is UNASSIGNED");
        }
        if (status == RoutingStatus.REASSIGNING && assignment != null) {
            throw new IllegalArgumentException("assignment must be null when status is REASSIGNING");
        }
    }

    public static ShardLookupResult assigned(ShardAssignment assignment) {
        return new ShardLookupResult(assignment.shardId(), RoutingStatus.ASSIGNED, assignment);
    }

    public static ShardLookupResult pendingReady(ShardAssignment assignment) {
        return new ShardLookupResult(assignment.shardId(), RoutingStatus.PENDING_READY, assignment);
    }

    public static ShardLookupResult reassigning(ShardId shardId) {
        return new ShardLookupResult(shardId, RoutingStatus.REASSIGNING, null);
    }

    public static ShardLookupResult unassigned(ShardId shardId) {
        return new ShardLookupResult(shardId, RoutingStatus.UNASSIGNED, null);
    }

    public boolean assigned() {
        return status == RoutingStatus.ASSIGNED;
    }

    public boolean hasAssignment() {
        return assignment != null;
    }
}
