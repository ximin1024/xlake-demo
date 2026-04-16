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

import io.github.ximin.xlake.backend.server.job.JobType;
import io.github.ximin.xlake.backend.server.session.BackendSession;
import io.github.ximin.xlake.server.JobState;
import io.github.ximin.xlake.server.QueryRequest;
import io.github.ximin.xlake.server.QueryResponse;
import io.github.ximin.xlake.server.QueryServiceGrpc;
import io.github.ximin.xlake.server.QueryType;
import io.github.ximin.xlake.server.ResultChunk;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@DisplayName("Backend Server E2E 测试 - Main 启动真实 gRPC Server")
class BackendServerE2ETest {

    @TempDir
    Path tempDir;

    private Main server;
    private SparkSession spark;
    private ManagedChannel channel;
    private QueryServiceGrpc.QueryServiceBlockingStub blockingStub;

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdown();
        }
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("E2E: executeQuery 应返回 inline SELECT 结果")
    void executeQuery_shouldReturnInlineSelectResult() throws Exception {
        startServer();

        BackendSession backendSession = new BackendSession(
                "session-inline",
                "session-inline",
                "SELECT 1 as id, 'hello' as name",
                JobType.READ,
                Map.of("collect", "true")
        );

        List<QueryResponse> responses = execute(buildQueryRequest(
                "request-inline",
                backendSession,
                QueryType.SQL,
                backendSession.getSql(),
                true
        ));

        assertThat(responses).extracting(QueryResponse::getJobId).isNotEmpty();
        assertThat(responses.stream().anyMatch(r -> r.hasState() && r.getState() == JobState.SUBMITTED)).isTrue();
        assertThat(responses.stream().anyMatch(r -> r.hasState() && r.getState() == JobState.SUCCEEDED)).isTrue();

        String payload = responses.stream()
                .filter(QueryResponse::hasResultChunk)
                .map(r -> r.getResultChunk().getData().toString(StandardCharsets.UTF_8))
                .findFirst()
                .orElseThrow();

        assertThat(payload).contains("1");
        assertThat(payload).contains("hello");
    }

    @Test
    @Timeout(120)
    @DisplayName("E2E: submit/status/result 应走通 xlake CREATE/INSERT/SELECT")
    void submitStatusAndResult_shouldRoundTripReadWriteSql() throws Exception {
        startServer();

        String tableName = "default.tbl_" + UUID.randomUUID().toString().replace('-', '_');
        String tablePath = tempDir.resolve("roundtrip-store").toUri().toString();

        executeAndAssertSuccess("create-table", new BackendSession(
                "session-create-table",
                "session-create-table",
                "CREATE TABLE IF NOT EXISTS " + tableName + " (key STRING, value STRING) USING xlake OPTIONS (path '" + tablePath + "', table '" + tableName + "')",
                JobType.DDL,
                Map.of()
        ), QueryType.DDL, false);

        QueryResponse insertResponse = blockingStub.submitQuery(buildQueryRequest(
                "insert-request",
                new BackendSession(
                        "session-insert",
                        "session-insert",
                        "INSERT INTO " + tableName + " VALUES ('k1','v1'), ('k2','v2'), ('k3','v3')",
                        JobType.WRITE,
                        Map.of()
                ),
                QueryType.DML,
                "INSERT INTO " + tableName + " VALUES ('k1','v1'), ('k2','v2'), ('k3','v3')",
                false
        ));

        assertThat(insertResponse.getState()).isEqualTo(JobState.SUBMITTED);
        assertThat(waitForTerminalState(insertResponse.getJobId())).isEqualTo(JobState.SUCCEEDED);

        QueryResponse selectSubmit = blockingStub.submitQuery(buildQueryRequest(
                "select-request",
                new BackendSession(
                        "session-select",
                        "session-select",
                        "SELECT key, value FROM " + tableName + " ORDER BY key",
                        JobType.READ,
                        Map.of("collect", "true")
                ),
                QueryType.SQL,
                "SELECT key, value FROM " + tableName + " ORDER BY key",
                true
        ));

        assertThat(waitForTerminalState(selectSubmit.getJobId())).isEqualTo(JobState.SUCCEEDED);

        String resultPayload = readResultPayload(selectSubmit.getJobId());
        assertThat(resultPayload).contains("k1");
        assertThat(resultPayload).contains("v1");
        assertThat(resultPayload).contains("k2");
        assertThat(resultPayload).contains("k3");
    }

    @Test
    @Timeout(120)
    @DisplayName("E2E: xlake 路由读写流程应支持过滤查询")
    void executeQuery_shouldCoverRoutedReadWriteFlow() throws Exception {
        startServer();

        String tableName = "default.route_" + UUID.randomUUID().toString().replace('-', '_');
        String tablePath = tempDir.resolve("routing-store").toUri().toString();

        executeAndAssertSuccess("create-table-routing", new BackendSession(
                "session-routing-create",
                "session-routing-create",
                "CREATE TABLE IF NOT EXISTS " + tableName + " (key STRING, value STRING) USING xlake OPTIONS (path '" + tablePath + "', table '" + tableName + "')",
                JobType.DDL,
                Map.of()
        ), QueryType.DDL, false);

        executeAndAssertSuccess("insert-routing-a", new BackendSession(
                "session-routing-insert-a",
                "session-routing-insert-a",
                "INSERT INTO " + tableName + " VALUES ('k1','v1'), ('k2','v2'), ('k3','v3')",
                JobType.WRITE,
                Map.of()
        ), QueryType.DML, false);

        executeAndAssertSuccess("insert-routing-b", new BackendSession(
                "session-routing-insert-b",
                "session-routing-insert-b",
                "INSERT INTO " + tableName + " VALUES ('k4','v4'), ('k5','v5')",
                JobType.WRITE,
                Map.of()
        ), QueryType.DML, false);

        List<QueryResponse> responses = execute(buildQueryRequest(
                "request-routing-read",
                new BackendSession(
                        "session-routing-read",
                        "session-routing-read",
                        "SELECT key, value FROM " + tableName + " WHERE key >= 'k2' AND key < 'k5' ORDER BY key",
                        JobType.READ,
                        Map.of("collect", "true")
                ),
                QueryType.SQL,
                "SELECT key, value FROM " + tableName + " WHERE key >= 'k2' AND key < 'k5' ORDER BY key",
                true
        ));

        assertThat(responses.stream().anyMatch(r -> r.hasState() && r.getState() == JobState.SUCCEEDED)).isTrue();
        String payload = responses.stream()
                .filter(QueryResponse::hasResultChunk)
                .map(r -> r.getResultChunk().getData().toString(StandardCharsets.UTF_8))
                .findFirst()
                .orElseThrow();

        assertThat(payload).contains("k2");
        assertThat(payload).contains("k3");
        assertThat(payload).contains("k4");
        assertThat(payload).doesNotContain("k1");
        assertThat(payload).doesNotContain("k5");
    }

    private void startServer() throws Exception {
        Path warehouseDir = tempDir.resolve("spark-warehouse");

        spark = SparkSession.builder()
                .appName("BackendServerE2ETest")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.plugins", "io.github.ximin.xlake.backend.spark.XlakeSparkPlugin")
                .config("spark.xlake.routing.shardCount", "1")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
                .config("spark.driver.extraJavaOptions",
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED")
                .getOrCreate();

        server = new Main(0, spark);
        server.startAsync();

        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        blockingStub = QueryServiceGrpc.newBlockingStub(channel);
    }

    private void executeAndAssertSuccess(String requestId,
                                         BackendSession session,
                                         QueryType queryType,
                                         boolean collectToDriver) {
        List<QueryResponse> responses = execute(buildQueryRequest(
                requestId,
                session,
                queryType,
                session.getSql(),
                collectToDriver
        ));

        assertThat(responses.stream().anyMatch(r -> r.hasState() && r.getState() == JobState.SUCCEEDED)).isTrue();
        assertThat(responses.stream().noneMatch(QueryResponse::hasError)).isTrue();
    }

    private List<QueryResponse> execute(QueryRequest request) {
        Iterator<QueryResponse> iterator = blockingStub.executeQuery(request);
        List<QueryResponse> responses = new ArrayList<>();
        iterator.forEachRemaining(responses::add);
        return responses;
    }

    private QueryRequest buildQueryRequest(String requestId,
                                           BackendSession session,
                                           QueryType queryType,
                                           String sql,
                                           boolean collectToDriver) {
        QueryRequest.ExecutionOptions.Builder optionsBuilder = QueryRequest.ExecutionOptions.newBuilder()
                .setCollectToDriver(collectToDriver)
                .setTimeoutSeconds(60);

        QueryRequest.SessionContext sessionContext = QueryRequest.SessionContext.newBuilder()
                .setSessionId(session.getSessionId())
                .build();

        return QueryRequest.newBuilder()
                .setId(requestId)
                .setSql(sql)
                .setType(queryType)
                .setSession(sessionContext)
                .setOptions(optionsBuilder.build())
                .putAllParameters(new HashMap<>(session.getOptions()))
                .build();
    }

    private JobState waitForTerminalState(String jobId) throws InterruptedException {
        long startedAt = System.currentTimeMillis();
        while (System.currentTimeMillis() - startedAt < 60_000) {
            QueryResponse statusResponse = blockingStub.getQueryStatus(QueryRequest.newBuilder()
                    .setId(jobId)
                    .build());
            if (statusResponse.hasError()) {
                throw new IllegalStateException(statusResponse.getError().getMessage());
            }
            if (statusResponse.getState() == JobState.SUCCEEDED ||
                    statusResponse.getState() == JobState.FAILED ||
                    statusResponse.getState() == JobState.CANCELLED ||
                    statusResponse.getState() == JobState.TIMED_OUT) {
                return statusResponse.getState();
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Timed out waiting for job to finish: " + jobId);
    }

    private String readResultPayload(String jobId) {
        Iterator<ResultChunk> iterator = blockingStub.getQueryResult(QueryRequest.newBuilder()
                .setId(jobId)
                .build());

        StringBuilder payload = new StringBuilder();
        iterator.forEachRemaining(chunk -> payload.append(chunk.getData().toString(StandardCharsets.UTF_8)));
        return payload.toString();
    }
}
