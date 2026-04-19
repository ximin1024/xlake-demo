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
package io.github.ximin.xlake.common.config;

import java.time.Duration;

public final class XlakeOptions {

    public static final ConfigOption<String> CLUSTER_ID = ConfigOption.string(
            "xlake.cluster.id", "default"
    );

    public static final ConfigOption<String> ZOOKEEPER_CONNECT_STRING = ConfigOption.string(
            "xlake.zookeeper.connect-string", "127.0.0.1:2181"
    );

    public static final ConfigOption<Duration> ZOOKEEPER_CONNECTION_TIMEOUT = ConfigOption.duration(
            "xlake.zookeeper.connection-timeout", Duration.ofSeconds(10)
    );

    public static final ConfigOption<Duration> ZOOKEEPER_SESSION_TIMEOUT = ConfigOption.duration(
            "xlake.zookeeper.session-timeout", Duration.ofSeconds(60)
    );

    public static final ConfigOption<String> ZOOKEEPER_NAMESPACE = ConfigOption.string(
            "xlake.zookeeper.namespace", "ximin"
    );

    public static final ConfigOption<Integer> ZOOKEEPER_RETRY_COUNT = ConfigOption.int_(
            "xlake.zookeeper.retry-count", 3
    );

    public static final ConfigOption<Long> ZOOKEEPER_RETRY_BASE_SLEEP_MS = ConfigOption.long_(
            "xlake.zookeeper.retry-base-sleep-ms", 1000L
    );

    public static final ConfigOption<Integer> SHARD_COUNT = ConfigOption.int_(
            "xlake.shard.count", 1
    );

    public static final ConfigOption<String> SHARD_RESOLVER_CLASS = ConfigOption.string(
            "xlake.shard.resolver-class", "indi.ximin.xlake.backend.routing.ModHashShardResolver"
    );

    public static final ConfigOption<String> METASTORE_TYPE = ConfigOption.string(
            "xlake.metastore.type", "memory"
    );

    public static final ConfigOption<String> METASTORE_BASE_PATH = ConfigOption.string(
            "xlake.metastore.base-path", "/tmp/xlake/metastore"
    );

    public static final ConfigOption<Integer> METASTORE_PORT = ConfigOption.int_(
            "xlake.metastore.port", 50051
    );

    public static final ConfigOption<Integer> ROUTING_REGISTER_MAX_RETRIES = ConfigOption.int_(
            "spark.xlake.routing.register.maxRetries", 3
    );

    public static final ConfigOption<Long> ROUTING_REGISTER_BACKOFF_MS = ConfigOption.long_(
            "spark.xlake.routing.register.backoffMs", 200L
    );

    public static final ConfigOption<Boolean> ROUTING_REGISTER_FAIL_OPEN = ConfigOption.bool(
            "spark.xlake.routing.register.failOpen", false
    );

    public static final ConfigOption<Integer> ROUTING_FORWARD_MAX_RETRIES = ConfigOption.int_(
            "spark.xlake.routing.forward.maxRetries", 3
    );

    public static final ConfigOption<Long> ROUTING_FORWARD_BACKOFF_MS = ConfigOption.long_(
            "spark.xlake.routing.forward.backoffMs", 100L
    );

    public static final ConfigOption<String> STORAGE_MMAP_PATH = ConfigOption.string(
            "xlake.storage.mmap.path", "/tmp/xlake/mmap"
    );

    public static final ConfigOption<String> STORAGE_STORE_ID = ConfigOption.string(
            "xlake.storage.store-id", "default"
    );

    public static final ConfigOption<Long> STORAGE_MMAP_SIZE = ConfigOption.long_(
            "xlake.storage.mmap.size", 1024L * 1024L * 1024L
    );

    public static final ConfigOption<Integer> STORAGE_LMDB_MAP_SIZE_MB = ConfigOption.int_(
            "xlake.storage.lmdb.map-size-mb", 1024
    );

    public static final ConfigOption<Integer> STORAGE_LMDB_MAX_DBS = ConfigOption.int_(
            "xlake.storage.lmdb.max-dbs", 256
    );

    public static final ConfigOption<Long> STORAGE_FLUSH_THRESHOLD_BYTES = ConfigOption.long_(
            "xlake.storage.flush.threshold-bytes", 256L * 1024L * 1024L
    );

    public static final ConfigOption<Integer> STORAGE_COMPACTION_MAX_CONCURRENT = ConfigOption.int_(
            "xlake.storage.compaction.max-concurrent", 4
    );

    public static final ConfigOption<Integer> READ_BATCH_SIZE = ConfigOption.int_(
            "xlake.read.batch-size", 1024
    );

    public static final ConfigOption<Integer> STORAGE_FLUSH_PARQUET_BLOCK_SIZE_MB = ConfigOption.int_(
            "xlake.storage.flush.parquet-block-size-mb", 256
    );

    public static final ConfigOption<Integer> STORAGE_FLUSH_PARQUET_PAGE_SIZE_KB = ConfigOption.int_(
            "xlake.storage.flush.parquet-page-size-kb", 64
    );

    public static final ConfigOption<Boolean> STORAGE_FLUSH_PARQUET_DICTIONARY_ENABLED = ConfigOption.bool(
            "xlake.storage.flush.parquet-dictionary-enabled", true
    );

    public static final ConfigOption<String> METRICS_CLUSTER_ID = ConfigOption.string(
            "xlake.metrics.cluster-id", "default"
    );

    public static final ConfigOption<Integer> METRICS_PROMETHEUS_PORT = ConfigOption.int_(
            "xlake.metrics.prometheus-port", 9190
    );

    public static final ConfigOption<String> METRICS_PROMETHEUS_PATH = ConfigOption.string(
            "xlake.metrics.prometheus-path", "/metrics"
    );

    public static final ConfigOption<Boolean> METRICS_ENABLE_SYSTEM = ConfigOption.bool(
            "xlake.metrics.enable-system", true
    );

    public static final ConfigOption<Boolean> METRICS_ENABLE_JMX = ConfigOption.bool(
            "xlake.metrics.enable-jmx", false
    );

    public static final ConfigOption<String> METRICS_JMX_DOMAIN = ConfigOption.string(
            "xlake.metrics.jmx-domain", "xlake.metrics"
    );

    public static final ConfigOption<Integer> METRICS_SAMPLING_INTERVAL_SECONDS = ConfigOption.int_(
            "xlake.metrics.sampling-interval-seconds", 10
    );

    public static final ConfigOption<Boolean> METRICS_ENABLE_EXECUTOR_AGGREGATION = ConfigOption.bool(
            "xlake.metrics.enable-executor-aggregation", true
    );

    public static final ConfigOption<String> CATALOG_WAREHOUSE_PATH = ConfigOption.string(
            "xlake.catalog.warehouse-path", "/tmp/xlake/warehouse"
    );

    public static final ConfigOption<String> CATALOG_IMPLEMENTATION = ConfigOption.string(
            "xlake.catalog.implementation", "hadoop"
    );

    public static final ConfigOption<Integer> ROUTING_SHARD_COUNT = ConfigOption.int_(
            "spark.xlake.routing.shardCount", 1
    );

    public static final ConfigOption<Boolean> ROUTING_WRITE_ASSUME_PARTITION_IS_SHARD = ConfigOption.bool(
            "spark.xlake.routing.write.assumePartitionIsShard", false
    );

    public static final ConfigOption<Integer> ROUTING_WRITE_BATCH_ROWS = ConfigOption.int_(
            "spark.xlake.routing.write.batchRows", 1024
    );

    public static final ConfigOption<Long> LMDB_LEASE_TTL_MS = ConfigOption.long_(
            "xlake.lmdb.lease.ttl.ms", 10_000L
    );

    public static final ConfigOption<Boolean> STORAGE_WAL_ENABLED = ConfigOption.bool(
            "xlake.storage.wal.enabled", true
    );

    public static final ConfigOption<Integer> STORAGE_WAL_BUFFER_SIZE = ConfigOption.int_(
            "xlake.storage.wal.buffer-size", 8192
    );

    public static final ConfigOption<Long> STORAGE_WAL_SYNC_INTERVAL_MS = ConfigOption.long_(
            "xlake.storage.wal.sync-interval-ms", 1000L
    );

    public static final ConfigOption<Long> STORAGE_WAL_MAX_SIZE_BYTES = ConfigOption.long_(
            "xlake.storage.wal.max-size-bytes", 100L * 1024L * 1024L
    );

    public static final ConfigOption<String> STORAGE_WAL_HDFS_PATH = ConfigOption.string(
            "xlake.storage.wal.hdfs-path", ""
    );

    public static final ConfigOption<String> STORAGE_WAL_COMPRESSION_CODEC = ConfigOption.string(
            "xlake.storage.wal.compression-codec", "NONE"
    );

    public static final ConfigOption<String> STORAGE_WAL_FILE_FORMAT = ConfigOption.string(
            "xlake.storage.wal.file-format", "BINARY"
    );

    public static final ConfigOption<Long> STORAGE_WAL_APPEND_TIMEOUT_MS = ConfigOption
            .builder("xlake.storage.wal.append.timeout-ms", 30000L, Long.class)
            .validatePositive()
            .build();

    public static final ConfigOption<Integer> RECOVERY_MAX_RETRIES = ConfigOption.int_(
            "xlake.recovery.max-retries", 3
    );

    public static final ConfigOption<Long> RECOVERY_BACKOFF_MS = ConfigOption.long_(
            "xlake.recovery.backoff-ms", 5000L
    );

    public static final ConfigOption<Long> RECOVERY_TIMEOUT_MS = ConfigOption.long_(
            "xlake.recovery.timeout-ms", 300000L
    );

    private XlakeOptions() {}
}
