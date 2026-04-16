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
package io.github.ximin.xlake.backend.server.session;

import io.github.ximin.xlake.backend.server.job.JobType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BackendSessionTest {

    private static final String SESSION_ID = "session-123";
    private static final String FRONTEND_SESSION_ID = "frontend-456";
    private static final String SQL = "SELECT * FROM table";
    private static final JobType JOB_TYPE = JobType.READ;
    private static final Map<String, String> OPTIONS = new HashMap<>();

    private BackendSession session;

    @BeforeEach
    void setUp() {
        OPTIONS.put("key", "value");
        session = new BackendSession(SESSION_ID, FRONTEND_SESSION_ID, SQL, JOB_TYPE, OPTIONS);
    }

    @Test
    void constructor_shouldInitializeAllFields() {
        assertThat(session.getSessionId()).isEqualTo(SESSION_ID);
        assertThat(session.getFrontendSessionId()).isEqualTo(FRONTEND_SESSION_ID);
        assertThat(session.getSql()).isEqualTo(SQL);
        assertThat(session.getJobType()).isEqualTo(JOB_TYPE);
        assertThat(session.getOptions()).isEqualTo(OPTIONS);
    }

    @Test
    void constructor_defaultStateShouldBePending() {
        assertThat(session.getState()).isEqualTo(SessionState.PENDING);
    }

    @Test
    void markRunning_shouldSetStateToRunningAndSetStartTime() {
        session.markRunning();

        assertThat(session.getState()).isEqualTo(SessionState.RUNNING);
        assertThat(session.getStartTime()).isGreaterThan(0);
    }

    @Test
    void markCompleted_shouldSetStateToCompletedAndResultAndEndTime() {
        Object expectedResult = new Object();

        session.markRunning();
        session.markCompleted(expectedResult);

        assertThat(session.getState()).isEqualTo(SessionState.COMPLETED);
        assertThat(session.getResult()).isSameAs(expectedResult);
        assertThat(session.getEndTime()).isGreaterThan(0);
    }

    @Test
    void markFailed_shouldSetStateToFailedAndErrorAndEndTime() {
        String expectedError = "Execution failed";

        session.markRunning();
        session.markFailed(expectedError);

        assertThat(session.getState()).isEqualTo(SessionState.FAILED);
        assertThat(session.getError()).isEqualTo(expectedError);
        assertThat(session.getEndTime()).isGreaterThan(0);
    }

    @Test
    void markCancelled_shouldSetStateToCancelledAndEndTime() {
        session.markCancelled();

        assertThat(session.getState()).isEqualTo(SessionState.CANCELLED);
        assertThat(session.getEndTime()).isGreaterThan(0);
    }

    @Test
    void markTimeout_shouldSetStateToTimeoutAndEndTime() {
        session.markTimeout();

        assertThat(session.getState()).isEqualTo(SessionState.TIMEOUT);
        assertThat(session.getEndTime()).isGreaterThan(0);
    }

    @Test
    void getDuration_shouldReturnZeroWhenNotStarted() {
        assertThat(session.getDuration()).isEqualTo(0);
    }

    @Test
    void getDuration_shouldReturnPositiveValueWhenCompleted() {
        session.markRunning();
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        session.markCompleted(null);

        assertThat(session.getDuration()).isGreaterThan(0);
    }
}
