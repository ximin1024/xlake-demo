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

import io.github.ximin.xlake.backend.routing.*;
import io.github.ximin.xlake.common.config.ConfigFactory;
import io.github.ximin.xlake.common.config.XlakeConfig;
import io.github.ximin.xlake.common.config.XlakeOptions;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.rpc.RpcEnv;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

public final class DriverRoutingPlugin implements DriverPlugin {
    private final RoutingCoordinator routingCoordinator;
    private transient RpcEnv rpcEnv;

    public DriverRoutingPlugin() {
        this(new InMemoryRoutingCoordinator(new InMemoryShardRoutingTable()));
    }

    public DriverRoutingPlugin(RoutingCoordinator routingCoordinator) {
        this.routingCoordinator = Objects.requireNonNull(routingCoordinator, "routingCoordinator must not be null");
    }

    @Override
    public Map<String, String> init(SparkContext sparkContext, PluginContext pluginContext) {
        Objects.requireNonNull(sparkContext, "sparkContext must not be null");
        Objects.requireNonNull(pluginContext, "pluginContext must not be null");
        RpcEnv resolvedRpcEnv = null;
        try {
            org.apache.spark.SparkEnv env = sparkContext.env();
            resolvedRpcEnv = env == null ? null : env.rpcEnv();
        } catch (Exception ignored) {
        }
        this.rpcEnv = resolvedRpcEnv;
        if (rpcEnv != null) {
            rpcEnv.setupEndpoint(
                    DriverRoutingEndpoint.ENDPOINT_NAME,
                    new DriverRoutingEndpoint(rpcEnv, routingCoordinator)
            );
        }
        SparkRoutingBridge.install(routingCoordinator);
        return Map.of();
    }

    @Override
    public Object receive(Object message) {
        Objects.requireNonNull(message, "message must not be null");
        switch (message) {
            case RoutingMessages
                         .RegisterExecutorMessage(String executorId, String nodeId, int slotIndex) -> {
                routingCoordinator.onExecutorUp(
                        executorId,
                        new NodeSlot(nodeId, slotIndex)
                );
                if (routingCoordinator instanceof InMemoryRoutingCoordinator coordinator) {
                    XlakeConfig config = ConfigFactory.createDefault();
                    String basePath = config.get(XlakeOptions.STORAGE_MMAP_PATH);
                    coordinator.registerExecutorBasePath(executorId, basePath);
                }
                return null;
            }
            case RoutingMessages.ExecutorReadyMessage(String executorId) -> {
                routingCoordinator.onExecutorReady(executorId);
                return null;
            }
            case RoutingMessages.ExecutorLostMessage(String executorId) -> {
                routingCoordinator.onExecutorDown(executorId);
                return null;
            }
            case RoutingMessages.LookupShardOwnerMessage(int shardId) -> {
                return routingCoordinator.lookupOwner(new ShardId(shardId));
            }
            case RoutingMessages.LookupShardOwnersMessage(java.util.Set<Integer> shardIds) -> {
                return routingCoordinator.lookupOwners(
                        shardIds.stream()
                                .map(ShardId::new)
                                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new))
                );
            }
            default -> {
            }
        }
        throw new IllegalArgumentException("Unsupported routing message type: " + message.getClass().getName());
    }

    @Override
    public void shutdown() {
        SparkRoutingBridge.clear();
        this.rpcEnv = null;
    }
}
