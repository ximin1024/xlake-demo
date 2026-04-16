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
package io.github.ximin.xlake.backend.server.load;

import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DefaultLoadCollector implements LoadCollector {
    private final OperatingSystemMXBean osBean;
    private final Method systemCpuLoadMethod;
    private final AtomicInteger queueLengthSupplier;

    public DefaultLoadCollector(AtomicInteger queueLengthSupplier) {
        this.osBean = ManagementFactory.getOperatingSystemMXBean();
        this.queueLengthSupplier = queueLengthSupplier;
        Method method = null;
        try {
            method = OperatingSystemMXBean.class.getMethod("getSystemCpuLoad");
        } catch (NoSuchMethodException e) {
            try {
                method = OperatingSystemMXBean.class.getMethod("getCpuLoad");
            } catch (NoSuchMethodException e2) {
                log.warn("Neither getSystemCpuLoad nor getCpuLoad method available");
            }
        }
        this.systemCpuLoadMethod = method;
    }

    @Override
    public BackendLoadInfo collect() {
        return BackendLoadInfo.builder()
                .queueLength(queueLengthSupplier.get())
                .cpuUsage(getCpuUsage())
                .memoryUsage(getMemoryUsage())
                .diskIoUsage(getDiskIoUsage())
                .networkUsage(getNetworkUsage())
                .build();
    }

    private double getCpuUsage() {
        if (systemCpuLoadMethod == null) {
            return 0.0;
        }
        try {
            Object result = systemCpuLoadMethod.invoke(osBean);
            if (result instanceof Double) {
                return (Double) result;
            }
        } catch (Exception e) {
            log.warn("Failed to get CPU usage", e);
        }
        return 0.0;
    }

    private double getMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        return (double) usedMemory / totalMemory;
    }

    private double getDiskIoUsage() {
        log.warn("Disk I/O metrics not implemented yet, returning 0.0");
        return 0.0;
    }

    private double getNetworkUsage() {
        log.warn("Network usage metrics not implemented yet, returning 0.0");
        return 0.0;
    }
}
