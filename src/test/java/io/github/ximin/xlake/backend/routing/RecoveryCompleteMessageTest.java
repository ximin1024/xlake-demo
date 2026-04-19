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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryCompleteMessageTest {

    @Test
    void successFactoryMethod() {
        RecoveryCompleteMessage message = RecoveryCompleteMessage.success(
                new ShardId(0), new RoutingEpoch(2)
        );

        assertThat(message.shardId()).isEqualTo(new ShardId(0));
        assertThat(message.epoch()).isEqualTo(new RoutingEpoch(2));
        assertThat(message.success()).isTrue();
        assertThat(message.errorMessage()).isEmpty();
        assertThat(message.failureType()).isNull();
    }

    @Test
    void failureFactoryMethodWithRecoveryFailureType() {
        RecoveryCompleteMessage message = RecoveryCompleteMessage.failure(
                new ShardId(0), new RoutingEpoch(2),
                "WAL read corruption detected",
                RecoveryFailureType.WAL_READ_CORRUPTION
        );

        assertThat(message.shardId()).isEqualTo(new ShardId(0));
        assertThat(message.epoch()).isEqualTo(new RoutingEpoch(2));
        assertThat(message.success()).isFalse();
        assertThat(message.errorMessage()).isEqualTo("WAL read corruption detected");
        assertThat(message.failureType()).isEqualTo(RecoveryFailureType.WAL_READ_CORRUPTION);
    }

    @Test
    void compactConstructorNullShardIdThrows() {
        assertThatThrownBy(() -> new RecoveryCompleteMessage(
                null, new RoutingEpoch(0), true, "", null
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardId");
    }

    @Test
    void compactConstructorNullEpochThrows() {
        assertThatThrownBy(() -> new RecoveryCompleteMessage(
                new ShardId(0), null, true, "", null
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("epoch");
    }

    @Test
    void compactConstructorNullErrorMessageDefaultsToEmpty() {
        RecoveryCompleteMessage message = new RecoveryCompleteMessage(
                new ShardId(0), new RoutingEpoch(0), true, null, null
        );
        assertThat(message.errorMessage()).isEmpty();
    }

    @Test
    void compactConstructorFailureWithNullFailureTypeDefaultsToUnknown() {
        RecoveryCompleteMessage message = new RecoveryCompleteMessage(
                new ShardId(0), new RoutingEpoch(0), false, "something went wrong", null
        );
        assertThat(message.failureType()).isEqualTo(RecoveryFailureType.UNKNOWN);
    }

    @Test
    void compactConstructorSuccessWithNullFailureTypeStaysNull() {
        RecoveryCompleteMessage message = new RecoveryCompleteMessage(
                new ShardId(0), new RoutingEpoch(0), true, null, null
        );
        assertThat(message.failureType()).isNull();
    }

    @Test
    void allRecoveryFailureTypesAreUsable() {
        for (RecoveryFailureType type : RecoveryFailureType.values()) {
            RecoveryCompleteMessage message = RecoveryCompleteMessage.failure(
                    new ShardId(0), new RoutingEpoch(0), "error", type
            );
            assertThat(message.failureType()).isEqualTo(type);
        }
    }
}
