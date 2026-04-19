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

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.github.ximin.xlake.backend.server.job.JobScheduler;
import io.github.ximin.xlake.backend.server.job.JobStatus;
import io.github.ximin.xlake.backend.server.job.JobType;
import io.github.ximin.xlake.backend.server.job.ListResult;
import io.github.ximin.xlake.backend.server.job.ManagedJobResult;
import io.github.ximin.xlake.backend.server.job.StringResult;
import io.github.ximin.xlake.backend.server.load.BackendLoadInfo;
import io.github.ximin.xlake.backend.server.load.BackpressureManager;
import io.github.ximin.xlake.backend.server.session.SessionStore;
import io.github.ximin.xlake.server.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * gRPC service implementation for QueryService.
 * Handles query submission, status checking, cancellation, and cluster status.
 */
@Slf4j
public class GrpcService extends QueryServiceGrpc.QueryServiceImplBase {

    private final SessionStore sessionStore;
    private final JobScheduler jobScheduler;
    private final BackpressureManager backpressureManager;

    public GrpcService(SessionStore sessionStore, JobScheduler jobScheduler,
                              BackpressureManager backpressureManager) {
        this.sessionStore = sessionStore;
        this.jobScheduler = jobScheduler;
        this.backpressureManager = backpressureManager;
    }

    @Override
    public void submitQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        try {
            SubmittedJob submittedJob = submitJob(request);
            responseObserver.onNext(buildSubmittedResponse(request.getId(), submittedJob.jobId()));
            responseObserver.onCompleted();

        } catch (IllegalStateException e) {
            responseObserver.onNext(buildErrorResponse(request.getId(), Status.Code.RESOURCE_EXHAUSTED,
                    e.getMessage()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error submitting query", e);
            responseObserver.onNext(buildErrorResponse(request.getId(), Status.Code.INTERNAL,
                    "Failed to submit query: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getQueryStatus(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        try {
            String jobId = request.getId();

            JobStatus jobStatus = jobScheduler.getJobStatus(jobId);
            if (jobStatus == JobStatus.NOT_FOUND) {
                responseObserver.onNext(buildErrorResponse(request.getId(), Status.Code.NOT_FOUND,
                        "Job not found: " + jobId));
                responseObserver.onCompleted();
                return;
            }
            JobState jobState = toJobState(jobStatus);

            QueryResponse response = QueryResponse.newBuilder()
                    .setRequestId(request.getId())
                    .setJobId(jobId)
                    .setState(jobState)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error getting query status", e);
            responseObserver.onNext(buildErrorResponse(request.getId(), Status.Code.INTERNAL,
                    "Failed to get query status: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    @Override
    public void cancelQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        try {
            String jobId = request.getId();

            boolean cancelled = jobScheduler.cancelJob(jobId);

            JobState jobState = cancelled ? JobState.CANCELLED : JobState.FAILED;

            QueryResponse response = QueryResponse.newBuilder()
                    .setRequestId(request.getId())
                    .setJobId(jobId)
                    .setState(jobState)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error cancelling query", e);
            responseObserver.onNext(buildErrorResponse(request.getId(), Status.Code.INTERNAL,
                    "Failed to cancel query: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    @Override
    public void executeQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        try {
            SubmittedJob submittedJob = submitJob(request);
            String requestId = request.getId();
            String jobId = submittedJob.jobId();

            responseObserver.onNext(buildSubmittedResponse(requestId, jobId));

            JobStatus finalStatus = waitForTerminalStatus(jobId, resolveWaitTimeoutMillis(submittedJob.options()));
            if (finalStatus == JobStatus.NOT_FOUND) {
                responseObserver.onNext(buildErrorResponse(requestId, Status.Code.NOT_FOUND,
                        "Job not found: " + jobId));
                responseObserver.onCompleted();
                return;
            }

            if (finalStatus != JobStatus.SUCCEEDED) {
                ManagedJobResult failedResult = jobScheduler.getJobResult(jobId);
                responseObserver.onNext(buildErrorResponse(requestId, toStatusCode(finalStatus),
                        errorMessageFor(finalStatus, failedResult)));
                responseObserver.onCompleted();
                return;
            }

            responseObserver.onNext(QueryResponse.newBuilder()
                    .setRequestId(requestId)
                    .setJobId(jobId)
                    .setState(JobState.SUCCEEDED)
                    .build());

            ManagedJobResult result = jobScheduler.getJobResult(jobId);
            if (result != null) {
                responseObserver.onNext(QueryResponse.newBuilder()
                        .setRequestId(requestId)
                        .setJobId(jobId)
                        .setResultChunk(toResultChunk(result))
                        .build());
            }
            responseObserver.onCompleted();

        } catch (IllegalStateException e) {
            responseObserver.onNext(buildErrorResponse(request.getId(), Status.Code.RESOURCE_EXHAUSTED,
                    e.getMessage()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error executing query", e);
            responseObserver.onNext(buildErrorResponse(request.getId(), Status.Code.INTERNAL,
                    "Failed to execute query: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getQueryResult(QueryRequest request, StreamObserver<ResultChunk> responseObserver) {
        try {
            String jobId = request.getId();
            JobStatus status = jobScheduler.getJobStatus(jobId);
            if (status == JobStatus.NOT_FOUND) {
                responseObserver.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("Job not found: " + jobId)
                        .asRuntimeException());
                return;
            }

            if (status != JobStatus.SUCCEEDED) {
                ManagedJobResult result = jobScheduler.getJobResult(jobId);
                responseObserver.onError(io.grpc.Status.FAILED_PRECONDITION
                        .withDescription(errorMessageFor(status, result))
                        .asRuntimeException());
                return;
            }

            ManagedJobResult result = jobScheduler.getJobResult(jobId);
            if (result == null) {
                responseObserver.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("No query result found for job: " + jobId)
                        .asRuntimeException());
                return;
            }

            responseObserver.onNext(toResultChunk(result));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error getting query result", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Failed to get query result: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    public void getClusterStatus(Empty request, StreamObserver<ClusterStatus> responseObserver) {
        try {
            BackendLoadInfo loadInfo = backpressureManager.getCurrentLoad();

            long totalMemoryMb = 16384L;
            int totalCores = 8;

            ClusterStatus clusterStatus = ClusterStatus.newBuilder()
                    .addExecutors(ExecutorStatus.newBuilder()
                            .setId("backend-" + UUID.randomUUID().toString().substring(0, 8))
                            .setState("ALIVE")
                            .setMemoryTotalMb(totalMemoryMb)
                            .setMemoryUsedMb((long) (loadInfo.getMemoryUsage() * totalMemoryMb))
                            .setCoresTotal(totalCores)
                            .setCoresUsed((int) (loadInfo.getCpuUsage() * totalCores)))
                    .setApplicationId("xlake-backend")
                    .setApplicationState("RUNNING")
                    .setNumRunningJobs(jobScheduler.getQueueLength())
                    .setNumPendingJobs(loadInfo.getQueueLength())
                    .setTotalMemoryMb(totalMemoryMb)
                    .setUsedMemoryMb((long) (loadInfo.getMemoryUsage() * totalMemoryMb))
                    .setTotalCores(totalCores)
                    .setUsedCores((int) (loadInfo.getCpuUsage() * totalCores))
                    .setSchedulerMode("FAIR")
                    .build();

            responseObserver.onNext(clusterStatus);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error getting cluster status", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Failed to get cluster status: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    private JobType toJobType(QueryType type) {
        if (type == null) {
            return JobType.READ;
        }
        switch (type) {
            case DDL:
                return JobType.DDL;
            case DML:
                return JobType.WRITE;
            case SQL:
            case ANALYZE:
            case EXPLAIN:
            case SHOW:
            case DESCRIBE:
            default:
                return JobType.READ;
        }
    }

    private JobState toJobState(JobStatus status) {
        if (status == null) {
            return JobState.PENDING;
        }
        switch (status) {
            case PENDING:
                return JobState.PENDING;
            case RUNNING:
                return JobState.RUNNING;
            case SUCCEEDED:
                return JobState.SUCCEEDED;
            case FAILED:
                return JobState.FAILED;
            case CANCELLED:
                return JobState.CANCELLED;
            case TIMEOUT:
                return JobState.TIMED_OUT;
            case NOT_FOUND:
            default:
                return JobState.PENDING;
        }
    }

    private Status toStatus(Status.Code code, String message) {
        return Status.newBuilder()
                .setCode(code)
                .setMessage(message)
                .build();
    }

    private SubmittedJob submitJob(QueryRequest request) {
        ensureAdmissionAllowed();

        String frontendSessionId = resolveFrontendSessionId(request);
        String sql = request.getSql();
        JobType jobType = toJobType(request.getType());
        Map<String, String> options = extractOptions(request);

        String sessionId = sessionStore.createSession(frontendSessionId, sql, jobType, options);
        String jobId = jobScheduler.submitJob(sessionId, sql, jobType, options);
        return new SubmittedJob(sessionId, jobId, options);
    }

    private void ensureAdmissionAllowed() {
        BackpressureManager.BackpressureStatus bpStatus = backpressureManager.getStatus();
        if (bpStatus == BackpressureManager.BackpressureStatus.REJECTING) {
            throw new IllegalStateException("Server is under high load, please retry later");
        }
        if (bpStatus == BackpressureManager.BackpressureStatus.WARNING ||
                bpStatus == BackpressureManager.BackpressureStatus.THROTTLING) {
            log.warn("Processing query under backpressure status: {}", bpStatus);
        }
    }

    private String resolveFrontendSessionId(QueryRequest request) {
        if (request.hasSession() && !request.getSession().getSessionId().isEmpty()) {
            return request.getSession().getSessionId();
        }
        return UUID.randomUUID().toString();
    }

    private Map<String, String> extractOptions(QueryRequest request) {
        Map<String, String> options = new HashMap<>();
        if (request.getParametersCount() > 0) {
            options.putAll(request.getParametersMap());
        }
        if (request.hasOptions()) {
            QueryRequest.ExecutionOptions executionOptions = request.getOptions();
            if (executionOptions.getTimeoutSeconds() > 0) {
                options.put("timeout", String.valueOf(executionOptions.getTimeoutSeconds() * 1000L));
            }
            if (executionOptions.getCollectToDriver()) {
                options.put("collect", "true");
            }
            if (!executionOptions.getOutputFormat().isEmpty()) {
                options.put("format", executionOptions.getOutputFormat());
            }
            if (!executionOptions.getOutputPath().isEmpty()) {
                options.put("outputPath", executionOptions.getOutputPath());
            }
            if (executionOptions.getMaxRows() > 0) {
                options.put("maxRows", String.valueOf(executionOptions.getMaxRows()));
            }
        }
        return options;
    }

    private QueryResponse buildSubmittedResponse(String requestId, String jobId) {
        return QueryResponse.newBuilder()
                .setRequestId(requestId)
                .setJobId(jobId)
                .setState(JobState.SUBMITTED)
                .build();
    }

    private QueryResponse buildErrorResponse(String requestId, Status.Code code, String message) {
        return QueryResponse.newBuilder()
                .setRequestId(requestId != null ? requestId : "")
                .setError(toStatus(code, message))
                .build();
    }

    private long resolveWaitTimeoutMillis(Map<String, String> options) {
        try {
            return Long.parseLong(options.getOrDefault("timeout", "60000"));
        } catch (NumberFormatException e) {
            return 60000L;
        }
    }

    private JobStatus waitForTerminalStatus(String jobId, long timeoutMillis) throws InterruptedException {
        long startedAt = System.currentTimeMillis();
        while (System.currentTimeMillis() - startedAt < timeoutMillis) {
            JobStatus status = jobScheduler.getJobStatus(jobId);
            if (isTerminalStatus(status)) {
                return status;
            }
            Thread.sleep(50L);
        }
        return jobScheduler.getJobStatus(jobId);
    }

    private boolean isTerminalStatus(JobStatus status) {
        return status == JobStatus.SUCCEEDED ||
                status == JobStatus.FAILED ||
                status == JobStatus.CANCELLED ||
                status == JobStatus.TIMEOUT ||
                status == JobStatus.NOT_FOUND;
    }

    private Status.Code toStatusCode(JobStatus status) {
        return switch (status) {
            case CANCELLED -> Status.Code.CANCELLED;
            case TIMEOUT -> Status.Code.DEADLINE_EXCEEDED;
            case NOT_FOUND -> Status.Code.NOT_FOUND;
            case FAILED, PENDING, RUNNING, SUCCEEDED -> Status.Code.INTERNAL;
        };
    }

    private String errorMessageFor(JobStatus status, ManagedJobResult result) {
        if (result != null && result.getError() != null && !result.getError().isEmpty()) {
            return result.getError();
        }
        return switch (status) {
            case CANCELLED -> "Job was cancelled";
            case TIMEOUT -> "Job execution timed out";
            case NOT_FOUND -> "Job not found";
            case FAILED -> "Job execution failed";
            case PENDING -> "Job is still pending";
            case RUNNING -> "Job is still running";
            case SUCCEEDED -> "Job execution succeeded";
        };
    }

    private ResultChunk toResultChunk(ManagedJobResult result) {
        if (result instanceof ListResult listResult) {
            return buildListResultChunk(result, listResult.getRows());
        }
        if (result instanceof StringResult stringResult) {
            return buildStringResultChunk(result, stringResult.getResult());
        }
        return buildStringResultChunk(result, "");
    }

    private ResultChunk buildListResultChunk(ManagedJobResult result, java.util.List<Row> rows) {
        String payload = rows.stream()
                .map(Row::toString)
                .collect(Collectors.joining("\n"));
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);

        ResultChunk.Metadata.Builder metadataBuilder = ResultChunk.Metadata.newBuilder()
                .setTotalRows(rows.size())
                .setTotalBytes(bytes.length);

        if (!rows.isEmpty()) {
            try {
                StructType schema = rows.get(0).schema();
                for (StructField field : schema.fields()) {
                    metadataBuilder.addColumns(ColumnSchema.newBuilder()
                            .setName(field.name())
                            .setType(field.dataType().simpleString())
                            .setNullable(field.nullable())
                            .build());
                }
            } catch (Exception ignored) {
                // Rows may not expose schema; omit column metadata in that case.
            }
        }
        applyTimingMetadata(result, metadataBuilder);

        return ResultChunk.newBuilder()
                .setChunkId(0)
                .setIsFirst(true)
                .setIsLast(true)
                .setTotalChunks(1)
                .setMetadata(metadataBuilder.build())
                .setFormat(ResultChunk.DataFormat.RAW_BYTES)
                .setData(ByteString.copyFrom(bytes))
                .build();
    }

    private ResultChunk buildStringResultChunk(ManagedJobResult result, String payload) {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        ResultChunk.Metadata.Builder metadataBuilder = ResultChunk.Metadata.newBuilder()
                .setTotalRows(payload.isEmpty() ? 0 : 1)
                .setTotalBytes(bytes.length);
        applyTimingMetadata(result, metadataBuilder);

        return ResultChunk.newBuilder()
                .setChunkId(0)
                .setIsFirst(true)
                .setIsLast(true)
                .setTotalChunks(1)
                .setMetadata(metadataBuilder.build())
                .setFormat(ResultChunk.DataFormat.RAW_BYTES)
                .setData(ByteString.copyFrom(bytes))
                .build();
    }

    private void applyTimingMetadata(ManagedJobResult result, ResultChunk.Metadata.Builder metadataBuilder) {
        if (result.getStartTime() > 0) {
            metadataBuilder.setStartTime(toTimestamp(result.getStartTime()));
        }
        if (result.getEndTime() > 0) {
            metadataBuilder.setEndTime(toTimestamp(result.getEndTime()));
        }
    }

    private Timestamp toTimestamp(long epochMillis) {
        return Timestamp.newBuilder()
                .setSeconds(epochMillis / 1000)
                .setNanos((int) ((epochMillis % 1000) * 1_000_000))
                .build();
    }

    private record SubmittedJob(String sessionId, String jobId, Map<String, String> options) {}
}
