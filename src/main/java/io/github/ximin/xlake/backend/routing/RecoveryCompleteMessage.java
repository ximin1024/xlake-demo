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

public record RecoveryCompleteMessage(
        ShardId shardId,
        RoutingEpoch epoch,
        boolean success,
        String errorMessage,
        RecoveryFailureType failureType
) implements Serializable {
    public RecoveryCompleteMessage {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (epoch == null) {
            throw new IllegalArgumentException("epoch must not be null");
        }
        if (errorMessage == null) {
            errorMessage = "";
        }
        if (failureType == null) {
            failureType = success ? null : RecoveryFailureType.UNKNOWN;
        }
    }

    public static RecoveryCompleteMessage success(ShardId shardId, RoutingEpoch epoch) {
        return new RecoveryCompleteMessage(shardId, epoch, true, "", null);
    }

    public static RecoveryCompleteMessage failure(ShardId shardId, RoutingEpoch epoch,
                                                   String errorMessage, RecoveryFailureType failureType) {
        return new RecoveryCompleteMessage(shardId, epoch, false, errorMessage, failureType);
    }
}
