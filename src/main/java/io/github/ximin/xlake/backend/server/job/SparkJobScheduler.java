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
package io.github.ximin.xlake.backend.server.job;

import com.google.common.base.Throwables;
import io.github.ximin.xlake.backend.server.metrics.JobMetricsCollector;
import io.github.ximin.xlake.backend.server.session.SessionStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SparkJobScheduler implements JobScheduler {

    private final SparkSession spark;
    private final ExecutorService queryExecutor;
    private final Map<String, ManagedJob> activeJobs;
    private final Map<String, ManagedJobResult> completedJobs;
    private final ScheduledExecutorService scheduler;
    private final JobMetricsCollector metricsCollector;
    private final AtomicLong jobIdCounter;
    private final SessionStore sessionStore;
    private final AtomicInteger queueLength;
    private final Map<String, XlakeTableTarget> xlakeTableTargets;

    private static final Pattern XLakeCreateTablePattern = Pattern.compile(
            "(?is)^CREATE\\s+TABLE(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s+([\\w.]+)\\s*\\(.*?\\)\\s+USING\\s+xlake\\s+OPTIONS\\s*\\((.*?)\\)\\s*$"
    );
    private static final Pattern XLakeOptionPathPattern = Pattern.compile("(?is)path\\s+'([^']+)'");
    private static final Pattern XLakeInsertValuesPattern = Pattern.compile(
            "(?is)^INSERT\\s+INTO\\s+([\\w.]+)\\s+VALUES\\s*(.+)$"
    );
    private static final Pattern TuplePattern = Pattern.compile("\\(([^()]*)\\)");

    public SparkJobScheduler(SparkSession spark, SessionStore sessionStore, AtomicInteger queueLength) {
        this.spark = spark;
        this.sessionStore = sessionStore;
        this.queueLength = queueLength;
        this.queryExecutor = Executors.newCachedThreadPool();
        this.activeJobs = new ConcurrentHashMap<>();
        this.completedJobs = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.metricsCollector = new JobMetricsCollector(spark);
        this.jobIdCounter = new AtomicLong(0);
        this.xlakeTableTargets = new ConcurrentHashMap<>();
    }

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            monitorActiveJobs();
            cleanupOldJobs();
        }, 5, 5, TimeUnit.SECONDS);
    }

    @Override
    public String submitJob(String sessionId, String sql, JobType type, Map<String, String> options) {
        String jobId = generateJobId();
        ManagedJob job = new ManagedJob(jobId, sessionId, sql, type, options);
        activeJobs.put(jobId, job);
        queueLength.incrementAndGet();

        try {
            queryExecutor.submit(() -> executeQueryJob(job, sessionId, type));
        } catch (RejectedExecutionException e) {
            job.setStatus(JobStatus.FAILED);
            job.setError("Job rejected: thread pool exhausted. Consider increasing capacity or reducing load.");
            job.setEndTime(System.currentTimeMillis());
            moveToCompleted(job);
            queueLength.decrementAndGet();
            log.warn("Job {} rejected due to thread pool exhaustion", jobId);
        }
        return jobId;
    }

    @Override
    public Optional<ManagedJob> getJob(String jobId) {
        return Optional.ofNullable(activeJobs.get(jobId));
    }

    @Override
    public JobStatus getJobStatus(String jobId) {
        ManagedJob job = activeJobs.get(jobId);
        if (job != null) {
            return job.getStatus();
        }
        ManagedJobResult result = completedJobs.get(jobId);
        if (result != null) {
            return result.getStatus();
        }
        return JobStatus.NOT_FOUND;
    }

    @Override
    public boolean cancelJob(String jobId) {
        ManagedJob job = activeJobs.get(jobId);
        if (job != null && job.isCancellable()) {
            job.cancel();
            return true;
        }
        return false;
    }

    @Override
    public ManagedJobResult getJobResult(String jobId) {
        return completedJobs.get(jobId);
    }

    @Override
    public List<ManagedJob> getActiveJobs() {
        return new ArrayList<>(activeJobs.values());
    }

    @Override
    public int getQueueLength() {
        return queueLength.get();
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
        queryExecutor.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!queryExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                queryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            queryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void executeQueryJob(ManagedJob job, String sessionId, JobType type) {
        try {
            job.setStatus(JobStatus.RUNNING);
            job.setStartTime(System.currentTimeMillis());
            queueLength.decrementAndGet();

            sessionStore.getSession(sessionId).ifPresent(session -> {
                session.markRunning();
                sessionStore.updateSession(session);
            });

            Dataset<Row> result = executeSql(job);

            if (job.getOptions().getOrDefault("collect", "false").equals("true")) {
                List<Row> rows = result.collectAsList();
                job.setResult(new ListResult(job.toResult(), rows));
            } else if (job.getOptions().containsKey("outputPath")) {
                String outputPath = job.getOptions().get("outputPath");
                result.write()
                        .mode(SaveMode.Overwrite)
                        .format(job.getOptions().getOrDefault("format", "parquet"))
                        .save(outputPath);
                job.setResult(new StringResult(job.toResult(), outputPath));
            } else {
                String message = switch (type) {
                    case WRITE -> "Write SQL executed successfully";
                    case DDL -> "DDL SQL executed successfully";
                    case READ -> "Read SQL executed successfully";
                };
                job.setResult(new StringResult(job.toResult(), message));
            }

            job.setStatus(JobStatus.SUCCEEDED);
            job.setEndTime(System.currentTimeMillis());

            sessionStore.getSession(sessionId).ifPresent(session -> {
                session.markCompleted(job.getResult());
                sessionStore.updateSession(session);
            });

        } catch (Exception e) {
            job.setStatus(JobStatus.FAILED);
            job.setError(Throwables.getStackTraceAsString(e));
            job.setEndTime(System.currentTimeMillis());

            sessionStore.getSession(sessionId).ifPresent(session -> {
                session.markFailed(e.getMessage());
                sessionStore.updateSession(session);
            });
        } finally {
            if (job.getStatus() == JobStatus.RUNNING) {
                queueLength.decrementAndGet();
            }
            moveToCompleted(job);
        }
    }

    private void monitorActiveJobs() {
        activeJobs.values().forEach(job -> {
            if (job.getStatus() == JobStatus.RUNNING) {
                job.updateMetrics(metricsCollector.collect(job.getId()));

                if (job.isTimeout()) {
                    job.timeout();
                    sessionStore.getSession(job.getSessionId()).ifPresent(session -> {
                        session.markTimeout();
                        sessionStore.updateSession(session);
                    });
                    moveToCompleted(job);
                }
            } else if (job.getStatus() == JobStatus.PENDING) {
                if (job.isPendingTimeout()) {
                    job.timeout();
                    sessionStore.getSession(job.getSessionId()).ifPresent(session -> {
                        session.markTimeout();
                        sessionStore.updateSession(session);
                    });
                    queueLength.decrementAndGet();
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
        ManagedJobResult result = job.getResult();
        if (result == null) {
            result = job.toResult();
        } else {
            // 重新创建结果对象，使用 job 的最新状态而不是 result 创建时的状态
            if (result instanceof ListResult) {
                ListResult listResult = (ListResult) result;
                result = new ListResult(
                    job.getId(),
                    job.getSql(),
                    job.getStatus(),
                    job.getStartTime(),
                    job.getEndTime(),
                    job.getError(),
                    listResult.getRows()
                );
            } else if (result instanceof StringResult) {
                StringResult stringResult = (StringResult) result;
                result = new StringResult(
                    job.getId(),
                    job.getSql(),
                    job.getStatus(),
                    job.getStartTime(),
                    job.getEndTime(),
                    job.getError(),
                    stringResult.getResult()
                );
            }
        }
        completedJobs.put(job.getId(), result);
    }

    private String generateJobId() {
        return "job_" + System.currentTimeMillis() + "_" + jobIdCounter.incrementAndGet();
    }

    private Dataset<Row> executeSql(ManagedJob job) {
        String sql = job.getSql().trim();

        Matcher createMatcher = XLakeCreateTablePattern.matcher(sql);
        if (createMatcher.matches()) {
            String tableName = normalizeTableName(createMatcher.group(1));
            Dataset<Row> dataset = spark.sql(sql);
            String path = extractRequiredPath(createMatcher.group(2));
            StructType schema = spark.table(tableName).schema();
            xlakeTableTargets.put(tableName, new XlakeTableTarget(tableName, path, schema));
            return dataset;
        }

        Matcher insertMatcher = XLakeInsertValuesPattern.matcher(sql);
        if (insertMatcher.matches()) {
            String tableName = normalizeTableName(insertMatcher.group(1));
            XlakeTableTarget tableTarget = xlakeTableTargets.get(tableName);
            if (tableTarget != null) {
                executeXlakeInsert(tableTarget, insertMatcher.group(2));
                spark.catalog().refreshTable(tableName);
                return spark.emptyDataFrame();
            }
        }

        return spark.sql(sql);
    }

    private String extractRequiredPath(String optionsBlock) {
        Matcher pathMatcher = XLakeOptionPathPattern.matcher(optionsBlock);
        if (!pathMatcher.find()) {
            throw new IllegalArgumentException("Missing xlake path option in CREATE TABLE statement");
        }
        return pathMatcher.group(1);
    }

    private void executeXlakeInsert(XlakeTableTarget tableTarget, String valuesBlock) {
        List<Row> rows = parseRows(valuesBlock, tableTarget.schema());
        spark.createDataFrame(rows, tableTarget.schema())
                .coalesce(1)
                .write()
                .format("xlake")
                .option("path", tableTarget.path())
                .option("table", tableTarget.tableName())
                .mode(SaveMode.Append)
                .save();
    }

    private List<Row> parseRows(String valuesBlock, StructType schema) {
        List<Row> rows = new ArrayList<>();
        Matcher tupleMatcher = TuplePattern.matcher(valuesBlock);
        while (tupleMatcher.find()) {
            String tupleBody = tupleMatcher.group(1);
            String[] rawValues = tupleBody.split("\\s*,\\s*");
            if (rawValues.length != schema.fields().length) {
                throw new IllegalArgumentException("INSERT value count does not match table schema");
            }

            Object[] converted = new Object[rawValues.length];
            StructField[] fields = schema.fields();
            for (int i = 0; i < rawValues.length; i++) {
                converted[i] = convertLiteral(rawValues[i], fields[i]);
            }
            rows.add(RowFactory.create(converted));
        }

        if (rows.isEmpty()) {
            throw new IllegalArgumentException("No VALUES tuples found in INSERT statement");
        }
        return rows;
    }

    private Object convertLiteral(String rawValue, StructField field) {
        String trimmed = rawValue.trim();
        if ("NULL".equalsIgnoreCase(trimmed)) {
            return null;
        }
        if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
            String unquoted = trimmed.substring(1, trimmed.length() - 1);
            if (field.dataType().equals(DataTypes.StringType)) {
                return unquoted;
            }
            if (field.dataType().equals(DataTypes.IntegerType)) {
                return Integer.parseInt(unquoted);
            }
            if (field.dataType().equals(DataTypes.LongType)) {
                return Long.parseLong(unquoted);
            }
            return unquoted;
        }
        if (field.dataType().equals(DataTypes.IntegerType)) {
            return Integer.parseInt(trimmed);
        }
        if (field.dataType().equals(DataTypes.LongType)) {
            return Long.parseLong(trimmed);
        }
        return trimmed;
    }

    private String normalizeTableName(String tableName) {
        return tableName.trim().toLowerCase();
    }

    private record XlakeTableTarget(String tableName, String path, StructType schema) {}
}
