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
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public class BackendSession {
    @Getter
    private final String sessionId;
    @Getter
    private final String frontendSessionId;
    @Getter
    private final String sql;
    @Getter
    private final JobType jobType;
    @Getter
    private final Map<String, String> options;
    @Setter
    @Getter
    private SessionState state;
    @Getter
    private final long createTime;
    @Setter
    @Getter
    private long startTime;
    @Setter
    @Getter
    private long endTime;
    @Setter
    @Getter
    private Object result;
    @Setter
    @Getter
    private String error;

    public BackendSession(String sessionId, String frontendSessionId, String sql,
                          JobType jobType, Map<String, String> options) {
        this.sessionId = sessionId;
        this.frontendSessionId = frontendSessionId;
        this.sql = sql;
        this.jobType = jobType;
        this.options = options;
        this.state = SessionState.PENDING;
        this.createTime = System.currentTimeMillis();
    }

    public void markRunning() {
        this.state = SessionState.RUNNING;
        this.startTime = System.currentTimeMillis();
    }

    public void markCompleted(Object result) {
        this.state = SessionState.COMPLETED;
        this.result = result;
        this.endTime = System.currentTimeMillis();
    }

    public void markFailed(String error) {
        this.state = SessionState.FAILED;
        this.error = error;
        this.endTime = System.currentTimeMillis();
    }

    public void markCancelled() {
        this.state = SessionState.CANCELLED;
        this.endTime = System.currentTimeMillis();
    }

    public void markTimeout() {
        this.state = SessionState.TIMEOUT;
        this.endTime = System.currentTimeMillis();
    }

    public long getDuration() {
        if (startTime > 0 && endTime > 0) {
            return endTime - startTime;
        }
        return 0;
    }
}
