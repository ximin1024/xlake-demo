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

import org.apache.spark.sql.classic.SparkSession;

public class Main {
    private SparkSession spark;
    private SparkJobManager jobManager;
    private RpcServer rpcServer;
    private volatile boolean isRunning = false;

    public Main() {
        this.spark = createSparkSession();
        this.jobManager = new SparkJobManager(spark);
    }

    public void start() throws Exception {
        rpcServer = new RpcServer(9090);
        //rpcServer.registerService(new QueryService());
        rpcServer.start();

        jobManager.start();

        isRunning = true;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown();
        }));

        System.out.println("Spark Query Server started on port 9090");

        while (isRunning) {
            Thread.sleep(1000);
        }
    }

    private SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("SparkQueryServer")
                .master("local[*]") // 实际部署时使用集群模式
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.executor.cores", "2")
                .config("spark.driver.maxResultSize", "2g")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.catalogImplementation", "hive") // 支持Hive元数据
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
                .enableHiveSupport()
                .getOrCreate();
    }

    public void shutdown() {
        isRunning = false;

        if (rpcServer != null) {
            rpcServer.shutdown();
        }

        if (jobManager != null) {
            jobManager.shutdown();
        }

        if (spark != null) {
            spark.stop();
        }

        System.out.println("Spark Query Server shutdown completed");
    }

    public static void main(String[] args) throws Exception {
        Main server = new Main();
        server.start();
    }
}
