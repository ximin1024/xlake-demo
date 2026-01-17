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

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public class ManagedJob {
    @Getter
    private final String id;
    @Getter
    private final String sql;
    @Getter
    private final Map<String, String> options;
    @Setter
    @Getter
    private JobStatus status;
    @Setter
    private long startTime;
    @Setter
    private long endTime;
    @Setter
    private String error;
    @Setter
    private ManagedJobResult result;
    @Getter
    private JobMetrics metrics;
    private volatile boolean cancelled;

    private final long timeoutMs;
    private final long submitTime;

    public ManagedJob(String id, String sql, Map<String, String> options) {
        this.id = id;
        this.sql = sql;
        this.options = options;
        this.status = JobStatus.PENDING;
        this.submitTime = System.currentTimeMillis();
        this.timeoutMs = Long.parseLong(options.getOrDefault("timeout", "3600000")); // 默认1小时
        this.metrics = new JobMetrics();
    }

    public void cancel() {
        this.cancelled = true;
        this.status = JobStatus.CANCELLED;
    }

    public boolean isCancellable() {
        return status == JobStatus.PENDING || status == JobStatus.RUNNING;
    }

    public boolean isTimeout() {
        return status == JobStatus.RUNNING &&
                System.currentTimeMillis() - startTime > timeoutMs;
    }

    public void timeout() {
        this.status = JobStatus.TIMEOUT;
    }

    public ManagedJobResult toResult() {
        return new ManagedJobResult(id, sql, status, startTime, endTime, error);
    }

    public void updateMetrics(JobMetrics newMetrics) {
        this.metrics = newMetrics;
    }
}
