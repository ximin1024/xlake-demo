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
package io.github.ximin.xlake.common;

import io.github.ximin.xlake.common.config.XlakeConfig;
import io.github.ximin.xlake.common.config.XlakeOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ZkCoordinator implements Coordinator {
    private final CuratorFramework zkClient;
    private final XlakeConfig config;
    private final static String COUNTER_PATH = "/uniq_id/";
    // Cache DistributedAtomicLong instances to avoid recreating them on every call
    private final Map<String, DistributedAtomicLong> dalCache = new ConcurrentHashMap<>();

    public ZkCoordinator(XlakeConfig config) {
        this.config = config;
        Duration connectionTimeout = config.get(XlakeOptions.ZOOKEEPER_CONNECTION_TIMEOUT);
        Duration sessionTimeout = config.get(XlakeOptions.ZOOKEEPER_SESSION_TIMEOUT);
        int retryCount = config.get(XlakeOptions.ZOOKEEPER_RETRY_COUNT);
        long retryBaseSleep = config.get(XlakeOptions.ZOOKEEPER_RETRY_BASE_SLEEP_MS);

        zkClient = CuratorFrameworkFactory.builder()
                .connectString(config.get(XlakeOptions.ZOOKEEPER_CONNECT_STRING))
                .retryPolicy(new ExponentialBackoffRetry((int) retryBaseSleep, retryCount))
                .connectionTimeoutMs((int) connectionTimeout.toMillis())
                .sessionTimeoutMs((int) sessionTimeout.toMillis())
                .namespace(config.get(XlakeOptions.ZOOKEEPER_NAMESPACE))
                .build();
    }

    public ZkCoordinator() {
        this(XlakeConfig.empty());
    }

    public void start() {
        zkClient.start();
    }

    @Override
    public void close() throws IOException {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    @Override
    public void watch(String nodePath) {
        try {
            zkClient.getData().usingWatcher((CuratorWatcher) event -> {
                log.info("Zxid: {}", event.getZxid());
                log.info("Watcher notification received. Event: {}", event);
            }).forPath(nodePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void newNode(String nodePath, byte[] value) {
        try {
            zkClient.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(nodePath, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteNode(String nodePath) {
        try {
            zkClient.delete().forPath(nodePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long uniqId(String uniqTable) {
        DistributedAtomicLong distributedAtomicLong = dalCache.computeIfAbsent(uniqTable, key -> {
            int retryCount = config.get(XlakeOptions.ZOOKEEPER_RETRY_COUNT);
            long retryBaseSleep = config.get(XlakeOptions.ZOOKEEPER_RETRY_BASE_SLEEP_MS);
            return new DistributedAtomicLong(zkClient, COUNTER_PATH + key,
                    new ExponentialBackoffRetry((int) retryBaseSleep, retryCount));
        });
        try {
            AtomicValue<Long> nextId = distributedAtomicLong.increment();
            if (nextId.succeeded()) {
                return nextId.postValue();
            }
            throw new RuntimeException("Error occurred when get unique id.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exist(String nodePath) {
        try {
            return zkClient.checkExists().forPath(nodePath) != null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(String nodePath, byte[] value) {
        try {
            zkClient.setData().forPath(nodePath, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
