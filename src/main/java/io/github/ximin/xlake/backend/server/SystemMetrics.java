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
public class SystemMetrics {
    private long totalMemory;
    private long freeMemory;
    private long maxMemory;
    private int availableProcessors;
    private long heapUsed;
    private long heapMax;
    private long nonHeapUsed;
    private long gcCount;
    private long gcTime;
    private int threadCount;
    private int daemonThreadCount;
    private int peakThreadCount;

    public static SystemMetrics copyOf(SystemMetrics original) {
        if (original == null) {
            throw new IllegalArgumentException("Original SystemMetrics must not be null");
        }
        SystemMetrics copy = new SystemMetrics();
        copy.setTotalMemory(original.getTotalMemory());
        copy.setFreeMemory(original.getFreeMemory());
        copy.setMaxMemory(original.getMaxMemory());
        copy.setAvailableProcessors(original.getAvailableProcessors());
        copy.setHeapUsed(original.getHeapUsed());
        copy.setHeapMax(original.getHeapMax());
        copy.setNonHeapUsed(original.getNonHeapUsed());
        copy.setGcCount(original.getGcCount());
        copy.setGcTime(original.getGcTime());
        copy.setThreadCount(original.getThreadCount());
        copy.setDaemonThreadCount(original.getDaemonThreadCount());
        copy.setPeakThreadCount(original.getPeakThreadCount());
        return copy;
    }
}
