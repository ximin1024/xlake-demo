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

import com.google.protobuf.Empty;
import io.github.ximin.xlake.backend.server.job.JobScheduler;
import io.github.ximin.xlake.backend.server.job.JobStatus;
import io.github.ximin.xlake.backend.server.job.JobType;
import io.github.ximin.xlake.backend.server.job.ListResult;
import io.github.ximin.xlake.backend.server.job.ManagedJobResult;
import io.github.ximin.xlake.backend.server.job.StringResult;
import io.github.ximin.xlake.backend.server.load.BackendLoadInfo;
import io.github.ximin.xlake.backend.server.load.BackpressureManager;
import io.github.ximin.xlake.backend.server.session.SessionStore;
import io.github.ximin.xlake.server.ClusterStatus;
import io.github.ximin.xlake.server.JobState;
import io.github.ximin.xlake.server.QueryRequest;
import io.github.ximin.xlake.server.QueryResponse;
import io.github.ximin.xlake.server.ResultChunk;
import io.grpc.stub.StreamObserver;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyMap;

@ExtendWith(MockitoExtension.class)
@DisplayName("GrpcService 端对端测试")
class GrpcServiceTest {

    @Mock
    private SessionStore sessionStore;

    @Mock
    private JobScheduler jobScheduler;

    @Mock
    private BackpressureManager backpressureManager;

    @Mock
    private StreamObserver<QueryResponse> responseObserver;

    @Mock
    private StreamObserver<ClusterStatus> clusterStatusObserver;

    @Mock
    private StreamObserver<ResultChunk> resultChunkObserver;

    private GrpcService grpcService;

    @BeforeEach
    void setUp() {
        grpcService = new GrpcService(sessionStore, jobScheduler, backpressureManager);
    }

    @Nested
    @DisplayName("submitQuery 测试")
    class SubmitQueryTests {

        @Test
        @DisplayName("正常提交查询应返回 SUBMITTED 状态")
        void submitQuery_normalRequest_shouldReturnSubmitted() {
            // Given
            when(backpressureManager.getStatus())
                    .thenReturn(BackpressureManager.BackpressureStatus.NORMAL);
            when(sessionStore.createSession(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("session-123");
            when(jobScheduler.submitJob(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("job-456");

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("request-001")
                    .setSql("SELECT * FROM test")
                    .build();

            // When
            grpcService.submitQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getRequestId()).isEqualTo("request-001");
            assertThat(response.getJobId()).isEqualTo("job-456");
            assertThat(response.getState()).isEqualTo(JobState.SUBMITTED);
        }

        @Test
        @DisplayName("backpressure 为 REJECTING 时应拒绝查询")
        void submitQuery_backpressureRejecting_shouldReject() {
            // Given
            when(backpressureManager.getStatus())
                    .thenReturn(BackpressureManager.BackpressureStatus.REJECTING);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("request-002")
                    .setSql("SELECT * FROM test")
                    .build();

            // When
            grpcService.submitQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            // Since state and error are in oneof content, only error is set for error responses
            assertThat(response.getError()).isNotNull();
            assertThat(response.getError().getCode())
                    .isEqualTo(io.github.ximin.xlake.server.Status.Code.RESOURCE_EXHAUSTED);
        }

        @Test
        @DisplayName("backpressure 为 WARNING 时应继续处理")
        void submitQuery_backpressureWarning_shouldContinueProcessing() {
            // Given
            when(backpressureManager.getStatus())
                    .thenReturn(BackpressureManager.BackpressureStatus.WARNING);
            when(sessionStore.createSession(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("session-123");
            when(jobScheduler.submitJob(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("job-789");

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("request-003")
                    .setSql("SELECT * FROM test")
                    .build();

            // When
            grpcService.submitQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getState()).isEqualTo(JobState.SUBMITTED);
        }

        @Test
        @DisplayName("带 session id 的请求应使用提供的 session id")
        void submitQuery_withSessionId_shouldUseProvidedSessionId() {
            // Given
            when(backpressureManager.getStatus())
                    .thenReturn(BackpressureManager.BackpressureStatus.NORMAL);
            when(sessionStore.createSession(eq("frontend-session-001"), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("backend-session-123");
            when(jobScheduler.submitJob(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("job-001");

            QueryRequest.SessionContext sessionContext = QueryRequest.SessionContext.newBuilder()
                    .setSessionId("frontend-session-001")
                    .build();

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("request-004")
                    .setSql("SELECT 1")
                    .setSession(sessionContext)
                    .build();

            // When
            grpcService.submitQuery(request, responseObserver);

            // Then
            verify(sessionStore).createSession(eq("frontend-session-001"), anyString(), any(JobType.class), anyMap());
        }

        @Test
        @DisplayName("带参数的请求应正确传递参数")
        void submitQuery_withParameters_shouldPassParameters() {
            // Given
            when(backpressureManager.getStatus())
                    .thenReturn(BackpressureManager.BackpressureStatus.NORMAL);
            when(sessionStore.createSession(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("session-123");
            when(jobScheduler.submitJob(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("job-001");

            Map<String, String> expectedParams = new HashMap<>();
            expectedParams.put("collect", "true");
            expectedParams.put("timeout", "30000");

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("request-005")
                    .setSql("SELECT * FROM test")
                    .putParameters("collect", "true")
                    .putParameters("timeout", "30000")
                    .build();

            // When
            grpcService.submitQuery(request, responseObserver);

            // Then
            verify(sessionStore).createSession(anyString(), eq("SELECT * FROM test"), any(JobType.class),
                    eq(expectedParams));
        }

        @Test
        @DisplayName("submitJob 异常时应返回错误响应")
        void submitQuery_jobSchedulerThrowsException_shouldReturnError() {
            // Given
            when(backpressureManager.getStatus())
                    .thenReturn(BackpressureManager.BackpressureStatus.NORMAL);
            when(sessionStore.createSession(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("session-123");
            when(jobScheduler.submitJob(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenThrow(new RuntimeException("Job scheduler error"));

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("request-006")
                    .setSql("SELECT * FROM test")
                    .build();

            // When
            grpcService.submitQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            // Since state and error are in oneof content, error response should have error set
            assertThat(response.getError()).isNotNull();
            assertThat(response.getError().getMessage()).contains("Job scheduler error");
        }
    }

    @Nested
    @DisplayName("getQueryStatus 测试")
    class GetQueryStatusTests {

        @Test
        @DisplayName("获取 PENDING 状态作业应返回 PENDING")
        void getQueryStatus_pendingJob_shouldReturnPending() {
            // Given
            when(jobScheduler.getJobStatus("job-001")).thenReturn(JobStatus.PENDING);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("job-001")
                    .build();

            // When
            grpcService.getQueryStatus(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getJobId()).isEqualTo("job-001");
            assertThat(response.getState()).isEqualTo(JobState.PENDING);
        }

        @Test
        @DisplayName("获取 RUNNING 状态作业应返回 RUNNING")
        void getQueryStatus_runningJob_shouldReturnRunning() {
            // Given
            when(jobScheduler.getJobStatus("job-002")).thenReturn(JobStatus.RUNNING);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("job-002")
                    .build();

            // When
            grpcService.getQueryStatus(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getState()).isEqualTo(JobState.RUNNING);
        }

        @Test
        @DisplayName("获取 SUCCEEDED 状态作业应返回 SUCCEEDED")
        void getQueryStatus_succeededJob_shouldReturnSucceeded() {
            // Given
            when(jobScheduler.getJobStatus("job-003")).thenReturn(JobStatus.SUCCEEDED);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("job-003")
                    .build();

            // When
            grpcService.getQueryStatus(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getState()).isEqualTo(JobState.SUCCEEDED);
        }

        @Test
        @DisplayName("获取 FAILED 状态作业应返回 FAILED")
        void getQueryStatus_failedJob_shouldReturnFailed() {
            // Given
            when(jobScheduler.getJobStatus("job-004")).thenReturn(JobStatus.FAILED);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("job-004")
                    .build();

            // When
            grpcService.getQueryStatus(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getState()).isEqualTo(JobState.FAILED);
        }

        @Test
        @DisplayName("获取 TIMEOUT 状态作业应返回 TIMED_OUT")
        void getQueryStatus_timeoutJob_shouldReturnTimedOut() {
            // Given
            when(jobScheduler.getJobStatus("job-005")).thenReturn(JobStatus.TIMEOUT);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("job-005")
                    .build();

            // When
            grpcService.getQueryStatus(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getState()).isEqualTo(JobState.TIMED_OUT);
        }

        @Test
        @DisplayName("获取不存在的作业应返回 NOT_FOUND 错误")
        void getQueryStatus_notFoundJob_shouldReturnNotFoundError() {
            // Given
            when(jobScheduler.getJobStatus("nonexistent-job")).thenReturn(JobStatus.NOT_FOUND);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("nonexistent-job")
                    .build();

            // When
            grpcService.getQueryStatus(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getError().getCode()).isEqualTo(io.github.ximin.xlake.server.Status.Code.NOT_FOUND);
        }
    }

    @Nested
    @DisplayName("cancelQuery 测试")
    class CancelQueryTests {

        @Test
        @DisplayName("取消存在的作业应返回 CANCELLED")
        void cancelQuery_existingJob_shouldReturnCancelled() {
            // Given
            when(jobScheduler.cancelJob("job-001")).thenReturn(true);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("job-001")
                    .build();

            // When
            grpcService.cancelQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getState()).isEqualTo(JobState.CANCELLED);
        }

        @Test
        @DisplayName("取消不存在的作业应返回 FAILED")
        void cancelQuery_nonExistentJob_shouldReturnFailed() {
            // Given
            when(jobScheduler.cancelJob("nonexistent-job")).thenReturn(false);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("nonexistent-job")
                    .build();

            // When
            grpcService.cancelQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getState()).isEqualTo(JobState.FAILED);
        }

        @Test
        @DisplayName("取消已完成的作业应返回 FAILED")
        void cancelQuery_completedJob_shouldReturnFailed() {
            // Given
            when(jobScheduler.cancelJob("completed-job")).thenReturn(false);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("completed-job")
                    .build();

            // When
            grpcService.cancelQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            assertThat(response.getState()).isEqualTo(JobState.FAILED);
        }
    }

    @Nested
    @DisplayName("executeQuery 测试")
    class ExecuteQueryTests {

        @Test
        @DisplayName("正常执行查询应返回 SUBMITTED、SUCCEEDED 和结果")
        void executeQuery_normalRequest_shouldReturnSubmittedSucceededAndResult() {
            // Given
            when(backpressureManager.getStatus())
                    .thenReturn(BackpressureManager.BackpressureStatus.NORMAL);
            when(sessionStore.createSession(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("session-exec-001");
            when(jobScheduler.submitJob(anyString(), anyString(), any(JobType.class), anyMap()))
                    .thenReturn("job-exec-001");
            when(jobScheduler.getJobStatus("job-exec-001")).thenReturn(JobStatus.SUCCEEDED);
            when(jobScheduler.getJobResult("job-exec-001")).thenReturn(
                    new ListResult(
                            new ManagedJobResult("job-exec-001", "SELECT 1", JobStatus.SUCCEEDED, 1L, 2L, null),
                            List.of(RowFactory.create(1, "hello"))
                    )
            );

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("exec-request-001")
                    .setSql("SELECT 1")
                    .setOptions(QueryRequest.ExecutionOptions.newBuilder().setCollectToDriver(true).build())
                    .build();

            // When
            grpcService.executeQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver, times(3)).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            assertThat(responseCaptor.getAllValues()).extracting(QueryResponse::getJobId)
                    .contains("job-exec-001");
            assertThat(responseCaptor.getAllValues().get(0).getState()).isEqualTo(JobState.SUBMITTED);
            assertThat(responseCaptor.getAllValues().get(1).getState()).isEqualTo(JobState.SUCCEEDED);
            assertThat(responseCaptor.getAllValues().get(2).hasResultChunk()).isTrue();
        }

        @Test
        @DisplayName("executeQuery 在 REJECTING 时应拒绝")
        void executeQuery_backpressureRejecting_shouldReject() {
            // Given
            when(backpressureManager.getStatus())
                    .thenReturn(BackpressureManager.BackpressureStatus.REJECTING);

            QueryRequest request = QueryRequest.newBuilder()
                    .setId("exec-request-002")
                    .setSql("SELECT 1")
                    .build();

            // When
            grpcService.executeQuery(request, responseObserver);

            // Then
            ArgumentCaptor<QueryResponse> responseCaptor = ArgumentCaptor.forClass(QueryResponse.class);
            verify(responseObserver).onNext(responseCaptor.capture());
            verify(responseObserver).onCompleted();

            QueryResponse response = responseCaptor.getValue();
            // Since state and error are in oneof content, only error is set for error responses
            assertThat(response.getError()).isNotNull();
        }
    }

    @Nested
    @DisplayName("getQueryResult 测试")
    class GetQueryResultTests {

        @Test
        @DisplayName("成功作业应返回 ResultChunk")
        void getQueryResult_succeededJob_shouldReturnResultChunk() {
            when(jobScheduler.getJobStatus("job-001")).thenReturn(JobStatus.SUCCEEDED);
            when(jobScheduler.getJobResult("job-001")).thenReturn(
                    new StringResult(
                            new ManagedJobResult("job-001", "SELECT 1", JobStatus.SUCCEEDED, 1L, 2L, null),
                            "payload"
                    )
            );

            grpcService.getQueryResult(QueryRequest.newBuilder().setId("job-001").build(), resultChunkObserver);

            ArgumentCaptor<ResultChunk> captor = ArgumentCaptor.forClass(ResultChunk.class);
            verify(resultChunkObserver).onNext(captor.capture());
            verify(resultChunkObserver).onCompleted();
            assertThat(captor.getValue().getData().toStringUtf8()).isEqualTo("payload");
        }

        @Test
        @DisplayName("不存在的作业应调用 onError")
        void getQueryResult_missingJob_shouldCallOnError() {
            when(jobScheduler.getJobStatus("missing-job")).thenReturn(JobStatus.NOT_FOUND);

            grpcService.getQueryResult(QueryRequest.newBuilder().setId("missing-job").build(), resultChunkObserver);

            verify(resultChunkObserver).onError(any());
            verify(resultChunkObserver, never()).onNext(any());
        }
    }

    @Nested
    @DisplayName("getClusterStatus 测试")
    class GetClusterStatusTests {

        @Test
        @DisplayName("获取集群状态应返回正常状态信息")
        void getClusterStatus_normal_shouldReturnClusterStatus() {
            // Given
            BackendLoadInfo loadInfo = BackendLoadInfo.builder()
                    .queueLength(5)
                    .cpuUsage(0.3)
                    .memoryUsage(0.4)
                    .diskIoUsage(0.2)
                    .networkUsage(0.1)
                    .build();
            when(backpressureManager.getCurrentLoad()).thenReturn(loadInfo);
            when(jobScheduler.getQueueLength()).thenReturn(3);

            // When
            grpcService.getClusterStatus(Empty.getDefaultInstance(), clusterStatusObserver);

            // Then
            ArgumentCaptor<ClusterStatus> statusCaptor = ArgumentCaptor.forClass(ClusterStatus.class);
            verify(clusterStatusObserver).onNext(statusCaptor.capture());
            verify(clusterStatusObserver).onCompleted();

            ClusterStatus status = statusCaptor.getValue();
            assertThat(status.getApplicationId()).isEqualTo("nebulake-backend");
            assertThat(status.getApplicationState()).isEqualTo("RUNNING");
            assertThat(status.getNumRunningJobs()).isEqualTo(3);
            assertThat(status.getExecutorsList()).hasSize(1);
            assertThat(status.getSchedulerMode()).isEqualTo("FAIR");
        }

        @Test
        @DisplayName("获取集群状态时异常应调用 onError")
        void getClusterStatus_exception_shouldCallOnError() {
            // Given
            when(backpressureManager.getCurrentLoad())
                    .thenThrow(new RuntimeException("Load collection failed"));

            // When
            grpcService.getClusterStatus(Empty.getDefaultInstance(), clusterStatusObserver);

            // Then
            verify(clusterStatusObserver).onError(any());
            verify(clusterStatusObserver, never()).onNext(any());
        }
    }
}
