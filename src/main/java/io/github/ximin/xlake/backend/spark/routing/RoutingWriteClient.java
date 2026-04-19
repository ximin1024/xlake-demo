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

import io.github.ximin.xlake.backend.routing.RoutingStatus;
import io.github.ximin.xlake.backend.routing.ShardAssignment;
import io.github.ximin.xlake.backend.routing.ShardId;
import io.github.ximin.xlake.backend.routing.ShardLookupResult;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcTimeout;
import org.apache.spark.util.RpcUtils;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class RoutingWriteClient {
    private final RpcEndpointRef driverEndpoint;
    private final int maxRetries;
    private final long backoffMs;

    public RoutingWriteClient() {
        SparkConf conf = SparkEnv.get().conf();
        this.driverEndpoint = RpcUtils.makeDriverRef(
                DriverRoutingEndpoint.ENDPOINT_NAME,
                conf,
                SparkEnv.get().rpcEnv()
        );
        this.maxRetries = conf.getInt("spark.xlake.routing.forward.maxRetries", 3);
        this.backoffMs = conf.getLong("spark.xlake.routing.forward.backoffMs", 100L);
    }

    public RoutingMessages.WriteAck forward(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String batchId,
            byte[] key,
            byte[] value
    ) {
        RoutingMessages.ShardWriteRequest request = new RoutingMessages.ShardWriteRequest(
                basePath,
                storeId,
                tableIdentifier,
                shardId,
                epoch,
                batchId,
                new byte[][]{key},
                new byte[][]{value}
        );
        return askWithRetry(request);
    }

    public RoutingMessages.WriteAck forwardMultiShard(
            String basePath,
            String storeId,
            String tableIdentifier,
            int[] shardIds,
            long[] epochs,
            String batchId,
            byte[][] keys,
            byte[][] values
    ) {
        RoutingMessages.MultiShardWriteRequest request = new RoutingMessages.MultiShardWriteRequest(
                basePath,
                storeId,
                tableIdentifier,
                shardIds,
                epochs,
                batchId,
                keys,
                values
        );
        return askWithRetry(request);
    }

    public static String newBatchId() {
        return UUID.randomUUID().toString();
    }

    public ShardLookupResult lookupOwner(int shardId) {
        RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
        Object reply = driverEndpoint.askSync(
                new RoutingMessages.LookupShardOwnerMessage(shardId),
                timeout,
                scala.reflect.ClassTag$.MODULE$.apply(Object.class)
        );
        if (reply instanceof ShardLookupResult result) {
            return result;
        }
        throw new IllegalStateException("Invalid lookup reply");
    }

    public Map<Integer, ShardLookupResult> lookupOwners(Set<Integer> shardIds) {
        RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
        Object reply = driverEndpoint.askSync(
                new RoutingMessages.LookupShardOwnersMessage(shardIds),
                timeout,
                scala.reflect.ClassTag$.MODULE$.apply(Object.class)
        );
        if (reply instanceof Map<?, ?> map) {
            // Driver returns Map<ShardId, ShardLookupResult>.
            java.util.HashMap<Integer, ShardLookupResult> out = new java.util.HashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (e.getKey() instanceof ShardId sid && e.getValue() instanceof ShardLookupResult r) {
                    out.put(sid.value(), r);
                }
            }
            return out;
        }
        throw new IllegalStateException("Invalid lookupOwners reply");
    }

    public RoutingMessages.ExecutorEndpointInfo lookupExecutorEndpoint(String executorId) {
        RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
        Object reply = driverEndpoint.askSync(
                new RoutingMessages.LookupExecutorEndpointMessage(executorId),
                timeout,
                scala.reflect.ClassTag$.MODULE$.apply(Object.class)
        );
        if (reply == null) {
            return null;
        }
        if (reply instanceof RoutingMessages.ExecutorEndpointInfo info) {
            return info;
        }
        throw new IllegalStateException("Invalid lookupExecutorEndpoint reply");
    }

    public RoutingMessages.WriteAck directForwardToExecutor(
            RoutingMessages.ExecutorEndpointInfo endpointInfo,
            RoutingMessages.ShardWriteForward forward
    ) {
        if (endpointInfo == null) {
            return RoutingMessages.WriteAck.retry("executor_endpoint_missing");
        }
        RpcEndpointRef target = SparkEnv.get().rpcEnv().setupEndpointRef(
                RpcAddress.apply(endpointInfo.host(), endpointInfo.port()),
                endpointInfo.endpointName()
        );
        RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
        Object reply = target.askSync(
                forward,
                timeout,
                scala.reflect.ClassTag$.MODULE$.apply(Object.class)
        );
        if (reply instanceof RoutingMessages.WriteAck ack) {
            return ack;
        }
        return RoutingMessages.WriteAck.error("invalid_reply");
    }

    public static boolean canFastPath(ShardLookupResult result, String currentExecutorId) {
        if (result == null || currentExecutorId == null) {
            return false;
        }
        if (result.status() != RoutingStatus.ASSIGNED) {
            return false;
        }
        ShardAssignment assignment = result.assignment();
        return assignment != null && currentExecutorId.equals(assignment.executorId());
    }

    private RoutingMessages.WriteAck askWithRetry(Object request) {
        Exception last = null;
        int attempts = Math.max(1, maxRetries);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
                Object reply = driverEndpoint.askSync(
                        request,
                        timeout,
                        scala.reflect.ClassTag$.MODULE$.apply(Object.class)
                );
                if (reply instanceof RoutingMessages.WriteAck ack) {
                    return ack;
                }
                return RoutingMessages.WriteAck.error("invalid_reply");
            } catch (Exception e) {
                last = e;
                if (attempt == attempts) {
                    break;
                }
                sleepQuietly(backoffMs * attempt);
            }
        }
        throw new IllegalStateException("Forwarding write failed", last);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(Math.max(0L, millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while forwarding write", e);
        }
    }
}
