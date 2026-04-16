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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class InMemorySessionStoreTest {
    private InMemorySessionStore store;

    @BeforeEach
    void setUp() {
        store = new InMemorySessionStore();
    }

    @Test
    void createSession_shouldCreateSessionAndReturnSessionId() {
        String frontendSessionId = "frontend-1";
        String sql = "SELECT * FROM table";
        JobType type = JobType.READ;
        Map<String, String> options = new HashMap<>();

        String sessionId = store.createSession(frontendSessionId, sql, type, options);

        assertThat(sessionId).isNotNull();
        assertThat(sessionId).startsWith("sess_");
    }

    @Test
    void getSession_shouldReturnSessionWhenExists() {
        String frontendSessionId = "frontend-1";
        String sql = "SELECT * FROM table";
        JobType type = JobType.READ;
        Map<String, String> options = new HashMap<>();

        String sessionId = store.createSession(frontendSessionId, sql, type, options);
        Optional<BackendSession> result = store.getSession(sessionId);

        assertThat(result).isPresent();
        assertThat(result.get().getFrontendSessionId()).isEqualTo(frontendSessionId);
        assertThat(result.get().getSql()).isEqualTo(sql);
        assertThat(result.get().getJobType()).isEqualTo(type);
        assertThat(result.get().getOptions()).isEqualTo(options);
    }

    @Test
    void getSession_shouldReturnEmptyWhenNotExists() {
        Optional<BackendSession> result = store.getSession("non-existent-session");

        assertThat(result).isEmpty();
    }

    @Test
    void updateSession_shouldUpdateExistingSession() {
        String sessionId = store.createSession("frontend-1", "SELECT 1", JobType.READ, new HashMap<>());
        BackendSession session = store.getSession(sessionId).get();

        session.setState(SessionState.RUNNING);
        session.markRunning();
        store.updateSession(session);

        BackendSession updated = store.getSession(sessionId).get();
        assertThat(updated.getState()).isEqualTo(SessionState.RUNNING);
        assertThat(updated.getStartTime()).isGreaterThan(0);
    }

    @Test
    void removeSession_shouldRemoveSession() {
        String sessionId = store.createSession("frontend-1", "SELECT 1", JobType.READ, new HashMap<>());

        store.removeSession(sessionId);

        assertThat(store.getSession(sessionId)).isEmpty();
    }

    @Test
    void removeSession_shouldRemoveFromFrontendMapping() {
        String frontendSessionId = "frontend-1";
        String sessionId = store.createSession(frontendSessionId, "SELECT 1", JobType.READ, new HashMap<>());

        store.removeSession(sessionId);

        List<BackendSession> sessions = store.getSessionsByFrontendId(frontendSessionId);
        assertThat(sessions).isEmpty();
    }

    @Test
    void getActiveSessions_shouldReturnOnlyPendingAndRunningSessions() {
        String sessionId1 = store.createSession("frontend-1", "SELECT 1", JobType.READ, new HashMap<>());
        String sessionId2 = store.createSession("frontend-2", "SELECT 2", JobType.READ, new HashMap<>());
        String sessionId3 = store.createSession("frontend-3", "SELECT 3", JobType.READ, new HashMap<>());

        store.getSession(sessionId1).get().setState(SessionState.RUNNING);
        store.getSession(sessionId2).get().setState(SessionState.COMPLETED);
        store.getSession(sessionId3).get().setState(SessionState.PENDING);

        List<BackendSession> activeSessions = store.getActiveSessions();

        assertThat(activeSessions).hasSize(2);
        assertThat(activeSessions.stream().map(BackendSession::getSessionId))
                .containsExactlyInAnyOrder(sessionId1, sessionId3);
    }

    @Test
    void getActiveSessions_shouldReturnEmptyWhenNoActiveSessions() {
        String sessionId = store.createSession("frontend-1", "SELECT 1", JobType.READ, new HashMap<>());
        store.getSession(sessionId).get().setState(SessionState.COMPLETED);

        List<BackendSession> activeSessions = store.getActiveSessions();

        assertThat(activeSessions).isEmpty();
    }

    @Test
    void getSessionsByFrontendId_shouldReturnSessionsForGivenFrontend() {
        String frontendSessionId = "frontend-1";
        store.createSession(frontendSessionId, "SELECT 1", JobType.READ, new HashMap<>());
        store.createSession(frontendSessionId, "SELECT 2", JobType.READ, new HashMap<>());
        store.createSession("frontend-2", "SELECT 3", JobType.READ, new HashMap<>());

        List<BackendSession> sessions = store.getSessionsByFrontendId(frontendSessionId);

        assertThat(sessions).hasSize(2);
        assertThat(sessions).allMatch(s -> s.getFrontendSessionId().equals(frontendSessionId));
    }

    @Test
    void getSessionsByFrontendId_shouldReturnEmptyListForNonexistentFrontend() {
        List<BackendSession> sessions = store.getSessionsByFrontendId("nonexistent-frontend");

        assertThat(sessions).isEmpty();
    }

    @Test
    void getActiveSessionCount_shouldReturnCorrectCount() {
        String sessionId1 = store.createSession("frontend-1", "SELECT 1", JobType.READ, new HashMap<>());
        String sessionId2 = store.createSession("frontend-2", "SELECT 2", JobType.READ, new HashMap<>());
        String sessionId3 = store.createSession("frontend-3", "SELECT 3", JobType.READ, new HashMap<>());

        store.getSession(sessionId1).get().setState(SessionState.RUNNING);
        store.getSession(sessionId2).get().setState(SessionState.PENDING);
        store.getSession(sessionId3).get().setState(SessionState.COMPLETED);

        int activeCount = store.getActiveSessionCount();

        assertThat(activeCount).isEqualTo(2);
    }

    @Test
    void sessionIdGeneration_shouldBeUnique() {
        Set<String> sessionIds = IntStream.range(0, 100)
                .mapToObj(i -> store.createSession("frontend-" + i, "SELECT " + i, JobType.READ, new HashMap<>()))
                .collect(Collectors.toSet());

        assertThat(sessionIds).hasSize(100);
    }

    @Test
    void sessionIdGeneration_shouldContainTimestampAndCounter() {
        String sessionId = store.createSession("frontend-1", "SELECT 1", JobType.READ, new HashMap<>());

        assertThat(sessionId).matches("sess_\\d+_\\d+");
    }
}
