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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public class JobMetricsCollector extends SparkListener {

    private final SparkSession spark;
    private final MetricRegistry metricRegistry;
    private final Map<String, JobMetrics> jobMetrics;
    private final Map<String, StageMetrics> stageMetrics;
    private final Map<String, TaskMetricsData> taskMetrics;
    private final Map<String, ExecutorMetrics> executorMetrics;

    private final ScheduledExecutorService scheduler;
    private final List<Consumer<JobMetrics>> metricsConsumers;

    // 缓存相关
    private final Cache<String, CachedMetrics> metricsCache;
    private final long cacheExpiryMs;

    // 统计相关
    private final AtomicLong totalJobsProcessed;
    private final AtomicLong totalTasksProcessed;
    private final AtomicLong totalBytesProcessed;

    public JobMetricsCollector(SparkSession spark) {
        this.spark = spark;
        this.metricRegistry = new MetricRegistry();
        this.jobMetrics = new ConcurrentHashMap<>();
        this.stageMetrics = new ConcurrentHashMap<>();
        this.taskMetrics = new ConcurrentHashMap<>();
        this.executorMetrics = new ConcurrentHashMap<>();

        this.scheduler = Executors.newScheduledThreadPool(2);
        this.metricsConsumers = new CopyOnWriteArrayList<>();

        this.cacheExpiryMs = TimeUnit.MINUTES.toMillis(5);
        this.metricsCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                .build();

        this.totalJobsProcessed = new AtomicLong(0);
        this.totalTasksProcessed = new AtomicLong(0);
        this.totalBytesProcessed = new AtomicLong(0);

        registerSparkListener();
        initializeMetrics();
        startScheduledTasks();
    }

    public JobMetrics collect(String jobId) {
        // 首先检查缓存
        CachedMetrics cached = metricsCache.getIfPresent(jobId);
        if (cached != null && !cached.isExpired()) {
            return cached.metrics;
        }

        // 从各个数据源收集Metrics
        JobMetrics metrics = new JobMetrics();
        metrics.setJobId(jobId);

        // 1. 从Spark监听器收集
        JobMetrics jobData = jobMetrics.get(jobId);
        if (jobData != null) {
            List<StageMetrics> stageMetricsList = collectStageMetrics(jobId);
            jobData.setStages(stageMetricsList);

            // 收集Executor级别的指标
            List<ExecutorMetrics> executorMetricsList = collectExecutorMetrics(jobId);
            jobData.setExecutors(executorMetricsList);
        }

        // 2. 从Spark REST API收集（更详细的信息）
        try {
            enrichWithSparkApiMetrics(jobId, metrics);
        } catch (Exception e) {
            // 忽略API错误，使用已有数据
            log.error("Failed to enrich metrics from Spark API: {}", e.getMessage());
        }

        // 3. 收集系统资源指标
        collectSystemMetrics(metrics);

        // 4. 计算派生指标
        calculateDerivedMetrics(metrics);

        // 缓存结果
        metricsCache.put(jobId, new CachedMetrics(metrics, System.currentTimeMillis()));

        // 通知消费者
        notifyConsumers(metrics);

        return metrics;
    }


    public void registerConsumer(Consumer<JobMetrics> consumer) {
        metricsConsumers.add(consumer);
    }


    public MetricsReport getMetricsReport() {
        MetricsReport report = new MetricsReport();
        report.setTimestamp(System.currentTimeMillis());
        report.setTotalJobsProcessed(totalJobsProcessed.get());
        report.setTotalTasksProcessed(totalTasksProcessed.get());
        report.setTotalBytesProcessed(totalBytesProcessed.get());

        // Job统计
        Map<JobStatus, Integer> jobStats = new HashMap<>();
        for (JobMetrics job : jobMetrics.values()) {
            jobStats.merge(job.getStatus(), 1, Integer::sum);
        }
        report.setJobStatistics(jobStats);

        // 执行器统计
        report.setExecutorCount(executorMetrics.size());

        // 性能指标
        report.setAverageJobDuration(calculateAverageJobDuration());
        report.setThroughputBytesPerSec(calculateCurrentThroughput());
        report.setTaskSuccessRate(calculateTaskSuccessRate());

        return report;
    }


    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        String jobId = String.valueOf(jobStart.jobId());

        JobMetrics jobData = new JobMetrics();
        jobData.setJobId(jobId);
        jobData.setSubmitTime(System.currentTimeMillis());
        jobData.setStartTime(jobStart.time());
        jobData.setStatus(JobStatus.RUNNING);
        // todo 确认正确性
        jobData.setTotalTasks(jobStart.stageInfos().size());
        jobMetrics.put(jobId, jobData);
        totalJobsProcessed.incrementAndGet();

        // 记录Stage信息
        for (StageInfo stageInfo : JavaConverters.seqAsJavaList(jobStart.stageInfos())) {
            StageMetrics stageData = new StageMetrics();
            stageData.setStageId(stageInfo.stageId());
            stageData.setStageName(stageInfo.name());
            stageData.setJobId(jobId);
            stageData.setNumTasks(stageInfo.numTasks());
            stageData.setSubmitTime(System.currentTimeMillis());
            stageMetrics.put(String.valueOf(stageInfo.stageId()), stageData);
        }

        // 更新Metrics
        metricRegistry.counter("jobs.started").inc();
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        String jobId = String.valueOf(jobEnd.jobId());
        JobMetrics jobData = jobMetrics.get(jobId);

        if (jobData != null) {
            jobData.setEndTime(jobEnd.time());
            jobData.setDuration(jobEnd.time() - jobData.getStartTime());

            switch (jobEnd.jobResult()) {
//                case JobSucceeded s -> {
//                    jobData.setStatus(JobStatus.SUCCEEDED);
//                    metricRegistry.counter("jobs.succeeded").inc();
//                }
                case JobFailed _ -> {
                    jobData.setStatus(JobStatus.FAILED);
                    metricRegistry.counter("jobs.failed").inc();
                }
                case null -> throw new IllegalArgumentException("Job result is null");
                default -> throw new IllegalStateException("Unknown JobResult");
            }

            // 更新Histogram
            metricRegistry.histogram("job.duration").update(jobData.getDuration());
        }
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        StageInfo stageInfo = stageSubmitted.stageInfo();
        StageMetrics stageData = stageMetrics.get(String.valueOf(stageInfo.stageId()));
        if (stageData != null) {
            stageData.setSubmitTime((long) stageSubmitted.stageInfo().submissionTime().get());
        }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        StageInfo stageInfo = stageCompleted.stageInfo();
        StageMetrics stageData = stageMetrics.get(String.valueOf(stageInfo.stageId()));

        if (stageData != null) {
            stageData.setCompletionTime((long) stageInfo.completionTime().get());
            stageData.setDuration(stageData.getCompletionTime() - stageData.getSubmitTime());
//            stageData.completedTasks = stageInfo.numTasks() -
//                    stageInfo.numFailedTasks() -
//                    stageInfo.numActiveTasks();
//            stageData.failedTasks = stageInfo.numFailedTasks();

            // 收集Stage指标
            stageData.setInputBytes(stageInfo.taskMetrics().inputMetrics().bytesRead());
            stageData.setOutputBytes(stageInfo.taskMetrics().outputMetrics().bytesWritten());
            stageData.setShuffleReadBytes(stageInfo.taskMetrics().shuffleReadMetrics().totalBytesRead());
            stageData.setShuffleWriteBytes(stageInfo.taskMetrics().shuffleWriteMetrics().bytesWritten());

            // 更新Histogram
            metricRegistry.histogram("stage.duration").update(stageData.getDuration());

            // 更新全局统计
            totalBytesProcessed.addAndGet(stageData.getInputBytes() + stageData.getOutputBytes());
        }
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        String taskId = String.format("%d-%d", taskStart.stageId(), taskStart.taskInfo().taskId());
        TaskMetricsData taskData = new TaskMetricsData();
        taskData.setTaskId(taskId);
        taskData.setStageId(taskStart.stageId());
        taskData.setExecutorId(taskStart.taskInfo().executorId());
        taskData.setStatus("RUNNING");
        taskData.setSubmitTime(taskStart.taskInfo().launchTime());

        taskMetrics.put(taskId, taskData);
        totalTasksProcessed.incrementAndGet();

        metricRegistry.counter("tasks.started").inc();
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        String taskId = String.format("%d-%d", taskEnd.stageId(), taskEnd.taskInfo().taskId());
        TaskMetricsData taskData = taskMetrics.get(taskId);

        if (taskData != null) {
            taskData.setCompletedTime(taskEnd.taskInfo().finishTime());
            taskData.setDuration(taskData.getCompletedTime() - taskData.getSubmitTime());
            taskData.setStatus(taskEnd.taskInfo().status());

            // 收集Task指标
            TaskMetrics metrics = taskEnd.taskMetrics();
            if (metrics != null) {
                taskData.setGcTime(metrics.jvmGCTime());
                taskData.setResultSerializationTime(metrics.resultSerializationTime());
                taskData.setBytesRead(metrics.inputMetrics().bytesRead());
                taskData.setBytesWritten(metrics.outputMetrics().bytesWritten());
                taskData.setRecordsRead(metrics.inputMetrics().recordsRead());
                taskData.setRecordsWritten(metrics.outputMetrics().recordsWritten());
                taskData.setShuffleReadBytes(metrics.shuffleReadMetrics().totalBytesRead());
                taskData.setShuffleWriteBytes(metrics.shuffleWriteMetrics().bytesWritten());
                taskData.setMemoryBytesSpilled(metrics.memoryBytesSpilled());
                taskData.setDiskBytesSpilled(metrics.diskBytesSpilled());
                taskData.setPeakExecutionMemory(metrics.peakExecutionMemory());
            }

            if ("SUCCESS".equals(taskData.getStatus())) {
                metricRegistry.counter("tasks.succeeded").inc();
                metricRegistry.histogram("task.duration").update(taskData.getDuration());
                metricRegistry.meter("bytes.processed.rate").mark(taskData.getBytesRead());
            } else {
                metricRegistry.counter("tasks.failed").inc();
            }
        }
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        String executorId = executorAdded.executorId();
        ExecutorMetrics executorData = new ExecutorMetrics();
        executorData.setId(executorId);
        executorData.setHost(executorAdded.executorInfo().executorHost());
        // todo total memory
        //executorData.setTotalMemoryMb();
        executorData.setTotalCores(executorAdded.executorInfo().totalCores());
        executorData.setRegisterTime(System.currentTimeMillis());
        executorData.setLastHeartbeat(System.currentTimeMillis());
        executorMetrics.put(executorId, executorData);
        metricRegistry.counter("executors.added").inc();
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        String executorId = executorRemoved.executorId();
        executorMetrics.remove(executorId);
        metricRegistry.counter("executors.removed").inc();
    }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate metricsUpdate) {
        String executorId = metricsUpdate.execId();
        ExecutorMetrics executorData = executorMetrics.get(executorId);

        if (executorData != null) {

            //executorData.setUsedMemoryMb();metricsUpdate.executorUpdates().usedOnHeapStorageMemory() / (1024 * 1024);
            executorData.setLastHeartbeat(System.currentTimeMillis());

            // 更新其他指标,需要从metricsUpdate中提取更多信息
        }
    }

    private long calculateAverageJobDuration() {
        long totalDuration = 0;
        int count = 0;

        for (JobMetrics job : jobMetrics.values()) {
            if (job.getDuration() > 0) {
                totalDuration += job.getDuration();
                count++;
            }
        }

        return count > 0 ? totalDuration / count : 0;
    }

    private double calculateCurrentThroughput() {
        // 计算最近5分钟的吞吐量
        long fiveMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);

        long bytesProcessed = 0;
        for (JobMetrics job : jobMetrics.values()) {
            if (job.getEndTime() > fiveMinutesAgo) {
                // 这里需要根据实际情况计算字节数
                // bytesProcessed += job.bytesProcessed;
            }
        }

        return bytesProcessed / (5 * 60.0); // bytes per second
    }

    private double calculateTaskSuccessRate() {
        long totalTasks = totalTasksProcessed.get();
        long failedTasks = taskMetrics.values().stream()
                .filter(task -> "FAILED".equals(task.getStatus()))
                .count();

        return totalTasks > 0 ? 1.0 - (double) failedTasks / totalTasks : 1.0;
    }

    private List<StageMetrics> collectStageMetrics(String jobId) {
        List<StageMetrics> result = new ArrayList<>();

        // 找到属于这个Job的所有Stage
        stageMetrics.values().stream()
                .filter(stage -> jobId.equals(stage.getJobId()))
                .forEach(stageData -> {
                    // 收集Task级别的详细信息
                    List<TaskMetricsData> taskMetricsDataList = collectTaskMetrics(stageData.getStageId());
                    stageData.setTaskDetails(taskMetricsDataList);
                    result.add(stageData);
                });

        return result;
    }

    private List<TaskMetricsData> collectTaskMetrics(int stageId) {
        List<TaskMetricsData> result = new ArrayList<>();

        taskMetrics.values().stream()
                .filter(task -> task.getStageId() == stageId)
                .forEach(taskData -> result.add(TaskMetricsData.copyOf(taskData)));

        return result;
    }

    private List<ExecutorMetrics> collectExecutorMetrics(String jobId) {
        List<ExecutorMetrics> result = new ArrayList<>();

        // 找到执行这个Job的Executor
        Set<String> executorIds = new HashSet<>();
        taskMetrics.values().stream()
                .filter(task -> jobId.equals(task.getJobId()))
                .map(TaskMetricsData::getExecutorId)
                .forEach(executorIds::add);

        executorIds.forEach(executorId -> {
            ExecutorMetrics executorData = executorMetrics.get(executorId);
            if (executorData != null) {
                result.add(ExecutorMetrics.copyOf(executorData));
            }
        });

        return result;
    }

    /**
     * 从Spark REST API获取更详细的Metrics
     */
    private void enrichWithSparkApiMetrics(String jobId, JobMetrics metrics) {
        try {
            // 构造Spark REST API URL
            String sparkUrl = spark.sparkContext().conf().get("spark.master", "local");
            if (sparkUrl.contains("spark://")) {
                String[] parts = sparkUrl.split(":");
                String host = parts[1].substring(2); // 去掉"//"
                int port = Integer.parseInt(parts[2]);

                // 使用Spark REST API获取Job信息
                // 注意：实际实现中需要使用HTTP客户端调用REST API
                // 这里简化为伪代码
                /*
                SparkRestClient client = new SparkRestClient(host, port);
                SparkJobInfo sparkJobInfo = client.getJobInfo(jobId);

                if (sparkJobInfo != null) {
                    metrics.setExecutorRunTime(sparkJobInfo.getExecutorRunTime());
                    metrics.setExecutorCpuTime(sparkJobInfo.getExecutorCpuTime());
                    metrics.setResultSerializationTime(sparkJobInfo.getResultSerializationTime());
                    metrics.setJvmGcTime(sparkJobInfo.getJvmGcTime());
                    metrics.setResultSize(sparkJobInfo.getResultSize());
                    metrics.setDiskBytesSpilled(sparkJobInfo.getDiskBytesSpilled());
                    metrics.setMemoryBytesSpilled(sparkJobInfo.getMemoryBytesSpilled());
                    metrics.setPeakExecutionMemory(sparkJobInfo.getPeakExecutionMemory());
                    metrics.setRecordsRead(sparkJobInfo.getRecordsRead());
                    metrics.setBytesRead(sparkJobInfo.getBytesRead());
                    metrics.setRecordsWritten(sparkJobInfo.getRecordsWritten());
                    metrics.setBytesWritten(sparkJobInfo.getBytesWritten());
                    metrics.setShuffleBytesRead(sparkJobInfo.getShuffleBytesRead());
                    metrics.setShuffleBytesWritten(sparkJobInfo.getShuffleBytesWritten());
                    metrics.setShuffleRecordsRead(sparkJobInfo.getShuffleRecordsRead());
                    metrics.setShuffleRecordsWritten(sparkJobInfo.getShuffleRecordsWritten());
                }
                */
            }
        } catch (Exception e) {
            // 记录错误但不中断
            log.error("Error enriching metrics from Spark API: {}", e.getMessage());
        }
    }

    private void collectSystemMetrics(JobMetrics metrics) {
        Runtime runtime = Runtime.getRuntime();

        SystemMetrics systemMetrics = new SystemMetrics();
        systemMetrics.setTotalMemory(runtime.totalMemory());
        systemMetrics.setFreeMemory(runtime.freeMemory());
        systemMetrics.setMaxMemory(runtime.maxMemory());
        systemMetrics.setAvailableProcessors(runtime.availableProcessors());

        // JVM Metrics
        systemMetrics.setHeapUsed(ManagementFactory.getMemoryMXBean()
                .getHeapMemoryUsage().getUsed());
        systemMetrics.setHeapMax(ManagementFactory.getMemoryMXBean()
                .getHeapMemoryUsage().getMax());
        systemMetrics.setNonHeapUsed(ManagementFactory.getMemoryMXBean()
                .getNonHeapMemoryUsage().getUsed());

        // GC Metrics
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory
                .getGarbageCollectorMXBeans();
        long gcCount = 0;
        long gcTime = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            gcCount += gcBean.getCollectionCount();
            gcTime += gcBean.getCollectionTime();
        }
        systemMetrics.setGcCount(gcCount);
        systemMetrics.setGcTime(gcTime);

        // Thread Metrics
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        systemMetrics.setThreadCount(threadBean.getThreadCount());
        systemMetrics.setDaemonThreadCount(threadBean.getDaemonThreadCount());
        systemMetrics.setPeakThreadCount(threadBean.getPeakThreadCount());

        metrics.setSystemMetrics(systemMetrics);
    }

    private void calculateDerivedMetrics(JobMetrics metrics) {
        long totalTime = metrics.getDuration();
        long executorRunTime = metrics.getExecutorRunTime();

        if (totalTime > 0) {
            // 计算调度延迟
            long schedulingDelay = totalTime - executorRunTime;
            metrics.setSchedulingDelay(schedulingDelay);

            // 计算CPU利用率
            double cpuUtilization = (double) metrics.getExecutorCpuTime() /
                    (totalTime);
            metrics.setCpuUtilization(Math.min(cpuUtilization, 1.0));

            // 计算内存利用率
            if (metrics.getSystemMetrics() != null) {
                double memoryUtilization = (double) metrics.getSystemMetrics().getHeapUsed() /
                        metrics.getSystemMetrics().getHeapMax();
                metrics.setMemoryUtilization(memoryUtilization);
            }
        }

        // 计算吞吐量
        long bytesProcessed = metrics.getBytesRead() + metrics.getShuffleBytesRead();
        if (totalTime > 0) {
            double throughput = (double) bytesProcessed / totalTime * 1000; // bytes/ms to bytes/s
            metrics.setThroughputBytesPerSec(throughput);
        }

        // 计算效率指标
        if (metrics.getTotalTasks() > 0) {
            double taskSuccessRate = (double) metrics.getCompletedTasks() /
                    metrics.getTotalTasks();
            metrics.setTaskSuccessRate(taskSuccessRate);

            double avgTaskTime = (double) metrics.getExecutorRunTime() /
                    metrics.getTotalTasks();
            metrics.setAvgTaskTime(avgTaskTime);
        }
    }

    private void notifyConsumers(JobMetrics metrics) {
        metricsConsumers.forEach(consumer -> {
            try {
                consumer.accept(metrics);
            } catch (Exception e) {
                log.error("Error notifying metrics consumer: {}", e.getMessage());
            }
        });
    }


    private void registerSparkListener() {
        spark.sparkContext().addSparkListener(this);
    }


    private void initializeMetrics() {
        // 注册Job级别的Metrics
        metricRegistry.register("jobs.total", (Gauge<Long>) totalJobsProcessed::get);
        metricRegistry.register("jobs.active", (Gauge<Integer>) () ->
                (int) jobMetrics.values().stream()
                        .filter(job -> job.getStatus() == JobStatus.RUNNING)
                        .count());

        // 注册Task级别的Metrics
        metricRegistry.register("tasks.total", (Gauge<Long>) totalTasksProcessed::get);
        metricRegistry.register("tasks.active", (Gauge<Integer>) () ->
                (int) taskMetrics.values().stream()
                        .filter(task -> "RUNNING".equals(task.getStatus()))
                        .count());

        // 注册数据处理的Metrics
        metricRegistry.register("bytes.processed.total", (Gauge<Long>) totalBytesProcessed::get);
        metricRegistry.register("bytes.processed.rate", new Meter());

        // 注册执行器Metrics
        metricRegistry.register("executors.active", (Gauge<Integer>) executorMetrics::size);

        // 注册Histogram用于分析延迟
        metricRegistry.histogram("job.duration");
        metricRegistry.histogram("task.duration");
        metricRegistry.histogram("stage.duration");
    }

    private void startScheduledTasks() {
        // 定期清理过期的Metrics
        scheduler.scheduleAtFixedRate(this::cleanupExpiredMetrics, 5, 5, TimeUnit.MINUTES);

        // 定期收集和报告聚合Metrics
        scheduler.scheduleAtFixedRate(this::collectAggregateMetrics, 30, 30, TimeUnit.SECONDS);

        // 定期持久化Metrics
        scheduler.scheduleAtFixedRate(this::persistMetrics, 1, 1, TimeUnit.MINUTES);
    }

    private void cleanupExpiredMetrics() {
        long cutoffTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);

        // 清理过期的Job Metrics
        jobMetrics.entrySet().removeIf(entry ->
                entry.getValue().getEndTime() < cutoffTime &&
                        entry.getValue().getStatus() != JobStatus.RUNNING);

        // 清理过期的Stage Metrics
        stageMetrics.entrySet().removeIf(entry ->
                entry.getValue().getCompletionTime() < cutoffTime);

        // 清理过期的Task Metrics
        taskMetrics.entrySet().removeIf(entry ->
                entry.getValue().getCompletedTime() < cutoffTime);

        // 清理过期的Executor Metrics
        executorMetrics.entrySet().removeIf(entry ->
                System.currentTimeMillis() - entry.getValue().getLastHeartbeat() >
                        TimeUnit.MINUTES.toMillis(5));
    }

    private void collectAggregateMetrics() {
        // 计算所有活跃Job的聚合指标
        long totalActiveTasks = 0;
        long totalActiveBytes = 0;
        int activeJobs = 0;

        for (JobMetrics job : jobMetrics.values()) {
            if (job.getStatus() == JobStatus.RUNNING) {
                activeJobs++;
                totalActiveTasks += job.getTotalTasks();
                // todo 这里可以添加更多聚合计算
            }
        }

        // 更新Metrics
        metricRegistry.counter("jobs.active.count").inc(activeJobs);
        metricRegistry.histogram("jobs.active.tasks").update(totalActiveTasks);

        // 记录聚合指标到日志或监控系统
        if (activeJobs > 0) {
            System.out.printf("Active jobs: %d, Active tasks: %d%n",
                    activeJobs, totalActiveTasks);
        }
    }

    private void persistMetrics() {
        // 这里可以实现将Metrics持久化到数据库或文件系统,InfluxDB、Prometheus、Elasticsearch等
        try {
            String timestamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date());
            String filename = String.format("metrics-%s.json", timestamp);

            Map<String, Object> metricsSnapshot = new HashMap<>();
            metricsSnapshot.put("timestamp", System.currentTimeMillis());
            metricsSnapshot.put("totalJobs", totalJobsProcessed.get());
            metricsSnapshot.put("totalTasks", totalTasksProcessed.get());
            metricsSnapshot.put("totalBytes", totalBytesProcessed.get());
            metricsSnapshot.put("activeJobs", jobMetrics.size());
            metricsSnapshot.put("activeExecutors", executorMetrics.size());

            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(metricsSnapshot);

            Files.write(Paths.get(filename), json.getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        } catch (Exception e) {
            log.error("Failed to persist metrics: {}", e.getMessage());
        }
    }


    private static class CachedMetrics {
        JobMetrics metrics;
        long timestamp;

        CachedMetrics(JobMetrics metrics, long timestamp) {
            this.metrics = metrics;
            this.timestamp = timestamp;
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > TimeUnit.MINUTES.toMillis(5);
        }
    }
}
