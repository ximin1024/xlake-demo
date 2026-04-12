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

import io.github.ximin.xlake.storage.block.DataBlock;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class RoutingMessages {
    private RoutingMessages() {
    }

    public record RegisterExecutorMessage(String executorId, String nodeId, int slotIndex) implements Serializable {
        public RegisterExecutorMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId must not be blank");
            }
            if (slotIndex < 0) {
                throw new IllegalArgumentException("slotIndex must be non-negative");
            }
        }
    }

    public record ExecutorLostMessage(String executorId) implements Serializable {
        public ExecutorLostMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
        }
    }

    public record ExecutorReadyMessage(String executorId) implements Serializable {
        public ExecutorReadyMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
        }
    }

    public record LookupShardOwnerMessage(int shardId) implements Serializable {
        public LookupShardOwnerMessage {
            if (shardId < 0) {
                throw new IllegalArgumentException("shardId must be non-negative");
            }
        }
    }

    public record LookupShardOwnersMessage(Set<Integer> shardIds) implements Serializable {
        public LookupShardOwnersMessage {
            if (shardIds == null) {
                throw new IllegalArgumentException("shardIds must not be null");
            }
            if (shardIds.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("shardIds must not contain null");
            }
            if (shardIds.stream().anyMatch(id -> id < 0)) {
                throw new IllegalArgumentException("shardIds must be non-negative");
            }
            shardIds = Set.copyOf(new LinkedHashSet<>(shardIds));
        }
    }

    public record RegisterExecutorEndpointMessage(
            String executorId,
            String host,
            int port,
            String endpointName
    ) implements Serializable {
        public RegisterExecutorEndpointMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            if (port <= 0) {
                throw new IllegalArgumentException("port must be positive");
            }
            if (endpointName == null || endpointName.isBlank()) {
                throw new IllegalArgumentException("endpointName must not be blank");
            }
        }
    }

    public record LookupExecutorEndpointMessage(String executorId) implements Serializable {
        public LookupExecutorEndpointMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
        }
    }

    public record ExecutorEndpointInfo(
            String executorId,
            String host,
            int port,
            String endpointName
    ) implements Serializable {
        public ExecutorEndpointInfo {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            if (port <= 0) {
                throw new IllegalArgumentException("port must be positive");
            }
            if (endpointName == null || endpointName.isBlank()) {
                throw new IllegalArgumentException("endpointName must not be blank");
            }
        }
    }

    public record ShardWriteRequest(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String batchId,
            byte[][] keys,
            byte[][] values
    ) implements Serializable {
        public ShardWriteRequest {
            if (basePath == null || basePath.isBlank()) {
                throw new IllegalArgumentException("basePath must not be blank");
            }
            if (storeId == null || storeId.isBlank()) {
                throw new IllegalArgumentException("storeId must not be blank");
            }
            if (tableIdentifier == null || tableIdentifier.isBlank()) {
                throw new IllegalArgumentException("tableIdentifier must not be blank");
            }
            if (shardId < 0) {
                throw new IllegalArgumentException("shardId must be non-negative");
            }
            if (epoch < 0) {
                throw new IllegalArgumentException("epoch must be non-negative");
            }
            if (batchId == null || batchId.isBlank()) {
                throw new IllegalArgumentException("batchId must not be blank");
            }
            if (keys == null || values == null) {
                throw new IllegalArgumentException("keys/values must not be null");
            }
            if (keys.length != values.length) {
                throw new IllegalArgumentException("keys/values must have the same length");
            }
        }
    }

    
    public record MultiShardWriteRequest(
            String basePath,
            String storeId,
            String tableIdentifier,
            int[] shardIds,
            long[] epochs,
            String batchId,
            byte[][] keys,
            byte[][] values
    ) implements Serializable {
        public MultiShardWriteRequest {
            if (basePath == null || basePath.isBlank()) {
                throw new IllegalArgumentException("basePath must not be blank");
            }
            if (storeId == null || storeId.isBlank()) {
                throw new IllegalArgumentException("storeId must not be blank");
            }
            if (tableIdentifier == null || tableIdentifier.isBlank()) {
                throw new IllegalArgumentException("tableIdentifier must not be blank");
            }
            if (batchId == null || batchId.isBlank()) {
                throw new IllegalArgumentException("batchId must not be blank");
            }
            if (shardIds == null || epochs == null || keys == null || values == null) {
                throw new IllegalArgumentException("shardIds/epochs/keys/values must not be null");
            }
            if (shardIds.length != keys.length || shardIds.length != values.length || shardIds.length != epochs.length) {
                throw new IllegalArgumentException("shardIds/epochs/keys/values must have the same length");
            }
            for (int i = 0; i < shardIds.length; i++) {
                if (shardIds[i] < 0) {
                    throw new IllegalArgumentException("shardIds must be non-negative");
                }
                if (epochs[i] < 0) {
                    throw new IllegalArgumentException("epochs must be non-negative");
                }
            }
        }
    }

    public record ShardWriteForward(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String batchId,
            byte[][] keys,
            byte[][] values
    ) implements Serializable {
        public ShardWriteForward {
            if (basePath == null || basePath.isBlank()) {
                throw new IllegalArgumentException("basePath must not be blank");
            }
            if (storeId == null || storeId.isBlank()) {
                throw new IllegalArgumentException("storeId must not be blank");
            }
            if (tableIdentifier == null || tableIdentifier.isBlank()) {
                throw new IllegalArgumentException("tableIdentifier must not be blank");
            }
            if (shardId < 0) {
                throw new IllegalArgumentException("shardId must be non-negative");
            }
            if (epoch < 0) {
                throw new IllegalArgumentException("epoch must be non-negative");
            }
            if (batchId == null || batchId.isBlank()) {
                throw new IllegalArgumentException("batchId must not be blank");
            }
            if (keys == null || values == null) {
                throw new IllegalArgumentException("keys/values must not be null");
            }
            if (keys.length != values.length) {
                throw new IllegalArgumentException("keys/values must have the same length");
            }
        }
    }

    public enum WriteAckStatus {
        OK,
        RETRY,
        STALE_EPOCH,
        ERROR
    }

    public record WriteAck(WriteAckStatus status, String message) implements Serializable {
        public WriteAck {
            if (status == null) {
                throw new IllegalArgumentException("status must not be null");
            }
            if (message == null) {
                throw new IllegalArgumentException("message must not be null");
            }
        }

        public static WriteAck ok() {
            return new WriteAck(WriteAckStatus.OK, "");
        }

        public static WriteAck retry(String message) {
            return new WriteAck(WriteAckStatus.RETRY, message);
        }

        public static WriteAck staleEpoch(String message) {
            return new WriteAck(WriteAckStatus.STALE_EPOCH, message);
        }

        public static WriteAck error(String message) {
            return new WriteAck(WriteAckStatus.ERROR, message);
        }
    }

    // ---- Block Catalog messages (read-path planning) ----

    /**
     * Sent by driver to a single executor: "give me your local blocks for this table".
     */
    public record QueryLocalBlocksMessage(
            String basePath,
            String storeId,
            String tableIdentifier
    ) implements Serializable {
        public QueryLocalBlocksMessage {
            Objects.requireNonNull(basePath, "basePath");
            Objects.requireNonNull(storeId, "storeId");
            Objects.requireNonNull(tableIdentifier, "tableIdentifier");
        }
    }

    /**
     * Response from executor: its local blocks.
     */
    public record LocalBlocksResponse(List<DataBlock> blocks) implements Serializable {
        public LocalBlocksResponse {
            blocks = blocks == null ? List.of() : List.copyOf(blocks);
        }
    }

    /**
     * Sent by DriverBlockListProvider to DriverRoutingEndpoint:
     * "fan-out to all executors and give me the global block list for this table".
     */
    public record QueryGlobalBlocksMessage(
            String basePath,
            String storeId,
            String tableIdentifier
    ) implements Serializable {
        public QueryGlobalBlocksMessage {
            Objects.requireNonNull(basePath, "basePath");
            Objects.requireNonNull(storeId, "storeId");
            Objects.requireNonNull(tableIdentifier, "tableIdentifier");
        }
    }

    /**
     * Response from driver: aggregated blocks from all executors.
     */
    public record GlobalBlocksResponse(List<DataBlock> blocks) implements Serializable {
        public GlobalBlocksResponse {
            blocks = blocks == null ? List.of() : List.copyOf(blocks);
        }
    }
}
