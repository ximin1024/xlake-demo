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


import com.google.common.base.Throwables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SparkJobManager {

    private final SparkSession spark;
    private final ExecutorService queryExecutor;
    private final Map<String, ManagedJob> activeJobs;
    private final Map<String, ManagedJobResult> completedJobs;
    private final ScheduledExecutorService scheduler;
    private final JobMetricsCollector metricsCollector;

    private final AtomicLong jobIdCounter = new AtomicLong(0);

    public SparkJobManager(SparkSession spark) {
        this.spark = spark;
        this.queryExecutor = Executors.newCachedThreadPool();
        this.activeJobs = new ConcurrentHashMap<>();
        this.completedJobs = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.metricsCollector = new JobMetricsCollector(spark);
    }

    public void start() {
        // 启动Job监控任务
        scheduler.scheduleAtFixedRate(() -> {
            monitorActiveJobs();
            cleanupOldJobs();
        }, 1, 5, TimeUnit.SECONDS);

        // 启动Metrics收集
//        scheduler.scheduleAtFixedRate(() -> {
//            metricsCollector.collect();
//        }, 5, 10, TimeUnit.SECONDS);
    }

    /**
     * 提交SQL查询Job
     */
    public String submitQuery(String sql, Map<String, String> options) {
        String jobId = generateJobId();

        ManagedJob job = new ManagedJob(jobId, sql, options);
        activeJobs.put(jobId, job);
        queryExecutor.submit(() -> executeQueryJob(job));

        return jobId;
    }

    private void executeQueryJob(ManagedJob job) {
        try {
            job.setStatus(JobStatus.RUNNING);
            job.setStartTime(System.currentTimeMillis());

            Dataset<Row> result = spark.sql(job.getSql());

            // 根据选项决定处理方式
            if (job.getOptions().getOrDefault("collect", "false").equals("true")) {
                // 收集结果到Driver
                List<Row> rows = result.collectAsList();
                job.setResult(new ListResult(job.toResult(),rows));
            } else if (job.getOptions().containsKey("outputPath")) {
                // 输出到文件系统
                String outputPath = job.getOptions().get("outputPath");
                result.write()
                        .mode(SaveMode.Overwrite)
                        .format(job.getOptions().getOrDefault("format", "parquet"))
                        .save(outputPath);
                job.setResult(new StringResult(job.toResult(),outputPath));
            }
//            else if (job.getOptions().containsKey("table")) {
//                // 创建临时表
//                String tableName = job.getOptions().get("table");
//                result.createOrReplaceTempView(tableName);
//                job.setResult(new TableResult(tableName));
//            } else {
//                // 默认：延迟计算，只记录逻辑计划
//                job.setResult(new LazyResult(result));
//            }

            job.setStatus(JobStatus.SUCCEEDED);
            job.setEndTime(System.currentTimeMillis());

            // 移动到完成列表
            moveToCompleted(job);

        } catch (Exception e) {
            job.setStatus(JobStatus.FAILED);
            job.setError(Throwables.getStackTraceAsString(e));
            job.setEndTime(System.currentTimeMillis());
            moveToCompleted(job);
        }
    }

    public JobStatus getJobStatus(String jobId) {
        ManagedJob job = activeJobs.get(jobId);
        if (job != null) {
            return job.getStatus();
        }

        ManagedJobResult result = completedJobs.get(jobId);
        if (result != null) {
            return result.status;
        }

        return JobStatus.NOT_FOUND;
    }

    public boolean cancelJob(String jobId) {
        ManagedJob job = activeJobs.get(jobId);
        if (job != null && job.isCancellable()) {
            job.cancel();
            return true;
        }
        return false;
    }

    public ManagedJobResult getJobResult(String jobId) {
        return completedJobs.get(jobId);
    }

    public List<ManagedJob> getActiveJobs() {
        return new ArrayList<>(activeJobs.values());
    }

    public JobMetrics getJobMetrics(String jobId) {
        ManagedJob job = activeJobs.get(jobId);
        if (job != null) {
            return job.getMetrics();
        }
        return null;
    }

    private void monitorActiveJobs() {
        activeJobs.values().forEach(job -> {
            if (job.getStatus() == JobStatus.RUNNING) {
                // 更新Metrics
                job.updateMetrics(metricsCollector.collect(job.getId()));

                // 检查超时
                if (job.isTimeout()) {
                    job.timeout();
                    moveToCompleted(job);
                }
            }
        });
    }

    private void cleanupOldJobs() {
        long cutoffTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);

        completedJobs.entrySet().removeIf(entry -> {
            ManagedJobResult result = entry.getValue();
            return result.getCompletionTime() < cutoffTime;
        });
    }

    private void moveToCompleted(ManagedJob job) {
        activeJobs.remove(job.getId());
        completedJobs.put(job.getId(), job.toResult());
    }

    private String generateJobId() {
        return "job_" + System.currentTimeMillis() + "_" + jobIdCounter.incrementAndGet();
    }

    public void shutdown() {
        queryExecutor.shutdown();
        scheduler.shutdown();

        try {
            if (!queryExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                queryExecutor.shutdownNow();
            }
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
