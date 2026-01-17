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

import lombok.Data;

@Data
public class ExecutorMetrics {
    private String id;
    private String host;
    private int port;
    private long totalMemoryMb;
    private long usedMemoryMb;
    private int totalCores;
    private int usedCores;
    private int totalTasks;
    private int completedTasks;
    private int failedTasks;
    private long totalDurationMs;
    private long gcCount;
    private long gcTimeMs;
    private long bytesReceived;
    private long bytesSent;
    private long registerTime;
    private long lastHeartbeat;

    public static ExecutorMetrics copyOf(ExecutorMetrics original) {
        if (original == null) {
            throw new IllegalArgumentException("Original ExecutorMetrics must not be null");
        }

        ExecutorMetrics copy = new ExecutorMetrics();
        copy.setId(original.getId());
        copy.setHost(original.getHost());
        copy.setPort(original.getPort());
        copy.setTotalMemoryMb(original.getTotalMemoryMb());
        copy.setUsedMemoryMb(original.getUsedMemoryMb());
        copy.setTotalCores(original.getTotalCores());
        copy.setUsedCores(original.getUsedCores());
        copy.setTotalTasks(original.getTotalTasks());
        copy.setCompletedTasks(original.getCompletedTasks());
        copy.setFailedTasks(original.getFailedTasks());
        copy.setTotalDurationMs(original.getTotalDurationMs());
        copy.setGcCount(original.getGcCount());
        copy.setGcTimeMs(original.getGcTimeMs());
        copy.setBytesReceived(original.getBytesReceived());
        copy.setBytesSent(original.getBytesSent());
        copy.setRegisterTime(original.getRegisterTime());
        copy.setLastHeartbeat(original.getLastHeartbeat());

        return copy;
    }
}
