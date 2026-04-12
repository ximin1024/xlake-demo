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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.rpc.RpcTimeout;
import org.apache.spark.util.RpcUtils;

import java.util.Map;
import java.util.Objects;

public final class ExecutorRoutingPlugin implements ExecutorPlugin {
    static final String CONF_REGISTER_MAX_RETRIES = "spark.xlake.routing.register.maxRetries";
    static final String CONF_REGISTER_BACKOFF_MS = "spark.xlake.routing.register.backoffMs";
    static final String CONF_REGISTER_FAIL_OPEN = "spark.xlake.routing.register.failOpen";

    private PluginContext pluginContext;
    private String executorId;
    private boolean registrationSucceeded;
    private transient RpcEnv rpcEnv;

    @Override
    public void init(PluginContext pluginContext, Map<String, String> extraConf) {
        this.pluginContext = Objects.requireNonNull(pluginContext, "pluginContext must not be null");
        this.executorId = requireExecutorId(pluginContext);
        int slotIndex = resolveSlotIndex(executorId);
        int maxRetries = pluginContext.conf().getInt(CONF_REGISTER_MAX_RETRIES, 3);
        long backoffMs = pluginContext.conf().getLong(CONF_REGISTER_BACKOFF_MS, 200L);
        boolean failOpen = pluginContext.conf().getBoolean(CONF_REGISTER_FAIL_OPEN, false);

        try {
            askWithRetry(new RoutingMessages.RegisterExecutorMessage(
                    executorId,
                    pluginContext.hostname(),
                    slotIndex
            ), maxRetries, backoffMs);
            askWithRetry(new RoutingMessages.ExecutorReadyMessage(executorId), maxRetries, backoffMs);
            registrationSucceeded = true;

            SparkEnv env = SparkEnv.get();
            if (env == null || env.rpcEnv() == null) {
                if (!failOpen) {
                    throw new IllegalStateException("SparkEnv.rpcEnv is not available for executor " + executorId);
                }
                return;
            }
            this.rpcEnv = env.rpcEnv();
            String endpointName = ExecutorShardEndpoint.ENDPOINT_PREFIX + executorId;
            rpcEnv.setupEndpoint(endpointName, new ExecutorShardEndpoint(rpcEnv));
            registerEndpointWithRetry(executorId, endpointName, maxRetries, backoffMs);
        } catch (Exception e) {
            registrationSucceeded = false;
            if (!failOpen) {
                throw new IllegalStateException("Failed to register executor routing state for " + executorId, e);
            }
        }
    }

    @Override
    public void shutdown() {
        if (pluginContext == null || executorId == null || !registrationSucceeded) {
            return;
        }
        try {
            pluginContext.send(new RoutingMessages.ExecutorLostMessage(executorId));
        } catch (Exception ignored) {
        }
    }

    private void registerEndpointWithRetry(String executorId,
                                           String endpointName,
                                           int maxRetries,
                                           long backoffMs) throws Exception {
        SparkConf conf = pluginContext.conf();
        RpcEndpointRef driverRef = RpcUtils.makeDriverRef(
                DriverRoutingEndpoint.ENDPOINT_NAME,
                conf,
                rpcEnv
        );

        RoutingMessages.RegisterExecutorEndpointMessage message
                = new RoutingMessages.RegisterExecutorEndpointMessage(
                executorId,
                rpcEnv.address().host(),
                rpcEnv.address().port(),
                endpointName
        );

        int attempts = Math.max(1, maxRetries);
        Exception last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                RpcTimeout timeout = RpcUtils.askRpcTimeout(pluginContext.conf());
                driverRef.askSync(message, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
                return;
            } catch (Exception e) {
                last = e;
                if (attempt == attempts) {
                    break;
                }
                sleepQuietly(backoffMs * attempt);
            }
        }
        throw last;
    }

    private void askWithRetry(Object message, int maxRetries, long backoffMs) throws Exception {
        int attempts = Math.max(1, maxRetries);
        Exception last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                pluginContext.ask(message);
                return;
            } catch (Exception e) {
                last = e;
                if (attempt == attempts) {
                    break;
                }
                sleepQuietly(backoffMs * attempt);
            }
        }
        throw last;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(Math.max(0L, millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying routing registration", e);
        }
    }

    private static String requireExecutorId(PluginContext pluginContext) {
        String executorId = pluginContext.executorID();
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalStateException("Spark executorID must not be blank");
        }
        return executorId;
    }

    static int resolveSlotIndex(String executorId) {
        try {
            return Integer.parseInt(executorId);
        } catch (NumberFormatException ignored) {
            return Math.floorMod(executorId.hashCode(), Integer.MAX_VALUE);
        }
    }
}
