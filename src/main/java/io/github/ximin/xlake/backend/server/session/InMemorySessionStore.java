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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class InMemorySessionStore implements SessionStore {
    private final Map<String, BackendSession> sessions;
    private final Map<String, List<String>> frontendToSessions;
    private final AtomicLong sessionIdCounter;

    public InMemorySessionStore() {
        this.sessions = new ConcurrentHashMap<>();
        this.frontendToSessions = new ConcurrentHashMap<>();
        this.sessionIdCounter = new AtomicLong(0);
    }

    @Override
    public synchronized String createSession(String frontendSessionId, String sql,
                                             JobType type, Map<String, String> options) {
        String sessionId = generateSessionId();
        BackendSession session = new BackendSession(sessionId, frontendSessionId, sql, type, options);
        sessions.put(sessionId, session);

        frontendToSessions.computeIfAbsent(frontendSessionId, k -> new CopyOnWriteArrayList<>())
                          .add(sessionId);

        return sessionId;
    }

    @Override
    public synchronized Optional<BackendSession> getSession(String sessionId) {
        return Optional.ofNullable(sessions.get(sessionId));
    }

    @Override
    public void updateSession(BackendSession session) {
        sessions.put(session.getSessionId(), session);
    }

    @Override
    public synchronized void removeSession(String sessionId) {
        BackendSession session = sessions.remove(sessionId);
        if (session != null && session.getFrontendSessionId() != null) {
            List<String> sessionIds = frontendToSessions.get(session.getFrontendSessionId());
            if (sessionIds != null) {
                sessionIds.remove(sessionId);
            }
        }
    }

    @Override
    public List<BackendSession> getActiveSessions() {
        return sessions.values().stream()
                .filter(s -> s.getState() == SessionState.PENDING ||
                            s.getState() == SessionState.RUNNING)
                .collect(Collectors.toList());
    }

    @Override
    public List<BackendSession> getSessionsByFrontendId(String frontendSessionId) {
        List<String> sessionIds = frontendToSessions.get(frontendSessionId);
        if (sessionIds == null) {
            return Collections.emptyList();
        }
        return sessionIds.stream()
                .map(sessions::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public int getActiveSessionCount() {
        return getActiveSessions().size();
    }

    private String generateSessionId() {
        return "sess_" + System.currentTimeMillis() + "_" + sessionIdCounter.incrementAndGet();
    }
}
