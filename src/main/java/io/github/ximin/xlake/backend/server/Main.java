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
package io.github.ximin.xlake.backend.server;

import io.github.ximin.xlake.backend.server.job.JobScheduler;
import io.github.ximin.xlake.backend.server.job.SparkJobScheduler;
import io.github.ximin.xlake.backend.server.load.BackpressureManager;
import io.github.ximin.xlake.backend.server.load.DefaultLoadCollector;
import io.github.ximin.xlake.backend.server.load.LoadCollector;
import io.github.ximin.xlake.backend.server.session.InMemorySessionStore;
import io.github.ximin.xlake.backend.server.session.SessionStore;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Main {
    private final SparkSession spark;
    private final JobScheduler jobScheduler;
    private final SessionStore sessionStore;
    private final BackpressureManager backpressureManager;
    private final LoadCollector loadCollector;
    private Server grpcServer;
    private ExecutorService executor;
    private int port;
    private final CountDownLatch shutdownLatch;
    private final AtomicBoolean started;
    private final AtomicBoolean shutdownHookRegistered;

    public Main(int port) {
        this(port, null);
    }

    public Main(int port, SparkSession spark) {
        this.port = port;
        this.shutdownLatch = new CountDownLatch(1);
        this.started = new AtomicBoolean(false);
        this.shutdownHookRegistered = new AtomicBoolean(false);
        this.spark = spark != null ? spark : createSparkSession();

        AtomicInteger queueLength = new AtomicInteger(0);
        this.loadCollector = new DefaultLoadCollector(queueLength);
        this.sessionStore = new InMemorySessionStore();
        this.jobScheduler = new SparkJobScheduler(this.spark, sessionStore, queueLength);
        this.backpressureManager = new BackpressureManager(loadCollector);

        this.executor = Executors.newFixedThreadPool(10);
        this.grpcServer = ServerBuilder.forPort(port)
                .executor(executor)
                .addService(new GrpcService(sessionStore, jobScheduler, backpressureManager))
                .build();
    }

    public void start() throws Exception {
        startAsync();
        shutdownLatch.await();
    }

    public synchronized void startAsync() throws Exception {
        if (started.get()) {
            return;
        }

        grpcServer.start();
        port = grpcServer.getPort();
        jobScheduler.start();
        started.set(true);

        if (shutdownHookRegistered.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        }

        log.info("Backend server started on port {}", port);
    }

    public synchronized void shutdown() {
        if (!started.getAndSet(false)) {
            return;
        }

        if (grpcServer != null) {
            grpcServer.shutdown();
        }

        if (executor != null) {
            executor.shutdown();
        }

        if (jobScheduler != null) {
            jobScheduler.shutdown();
        }

        if (spark != null) {
            spark.stop();
        }

        shutdownLatch.countDown();

        log.info("Backend server shutdown");
    }

    public int getPort() {
        return port;
    }

    private SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("XlakeBackendServer")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.plugins", "io.github.ximin.xlake.backend.spark.XlakeSparkPlugin")
                .config("spark.xlake.routing.shardCount", "1")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.executor.cores", "2")
                .config("spark.driver.maxResultSize", "2g")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
                .getOrCreate();
    }

    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9090;
        Main server = new Main(port);
        try {
            server.start();
        } catch (Exception e) {
            log.error("Server error", e);
        }
    }
}
