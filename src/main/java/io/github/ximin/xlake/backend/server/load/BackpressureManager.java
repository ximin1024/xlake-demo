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

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class BackpressureManager {
    private final LoadCollector loadCollector;

    private final double queueThreshold;
    private final double cpuThreshold;
    private final double memoryThreshold;
    private final double diskThreshold;
    private final double networkThreshold;

    private final double queueWeight;
    private final double cpuWeight;
    private final double memoryWeight;
    private final double diskWeight;
    private final double networkWeight;

    private final double warningThreshold;
    private final double throttleThreshold;
    private final double rejectThreshold;

    private final AtomicReference<BackendLoadInfo> lastLoad;

    public BackpressureManager(LoadCollector loadCollector,
                               double warningThreshold, double throttleThreshold, double rejectThreshold) {
        this.loadCollector = loadCollector;

        this.queueThreshold = 100;
        this.cpuThreshold = 0.9;
        this.memoryThreshold = 0.9;
        this.diskThreshold = 0.8;
        this.networkThreshold = 0.8;

        // Fix: Adjusted weights so max can reach 1.0
        this.queueWeight = 0.5;
        this.cpuWeight = 0.15;
        this.memoryWeight = 0.15;
        this.diskWeight = 0.1;
        this.networkWeight = 0.1;

        this.warningThreshold = warningThreshold;
        this.throttleThreshold = throttleThreshold;
        this.rejectThreshold = rejectThreshold;

        this.lastLoad = new AtomicReference<>(new BackendLoadInfo());
    }

    public BackpressureManager(LoadCollector loadCollector) {
        this(loadCollector, 0.7, 0.9, 0.95);
    }

    public synchronized BackendLoadInfo getCurrentLoad() {
        BackendLoadInfo load = loadCollector.collect();
        lastLoad.set(load);
        return load;
    }

    public boolean shouldReject() {
        BackendLoadInfo load = getCurrentLoad();
        double loadScore = calculateLoadScore(load);
        return loadScore >= rejectThreshold;
    }

    public boolean shouldThrottle() {
        BackendLoadInfo load = getCurrentLoad();
        double loadScore = calculateLoadScore(load);
        return loadScore >= throttleThreshold;
    }

    public boolean shouldWarn() {
        BackendLoadInfo load = getCurrentLoad();
        double loadScore = calculateLoadScore(load);
        return loadScore >= warningThreshold && loadScore < throttleThreshold;
    }

    public BackpressureStatus getStatus() {
        BackendLoadInfo load = getCurrentLoad();
        double loadScore = calculateLoadScore(load);

        if (loadScore < warningThreshold) {
            return BackpressureStatus.NORMAL;
        } else if (loadScore < throttleThreshold) {
            return BackpressureStatus.WARNING;
        } else if (loadScore < rejectThreshold) {
            return BackpressureStatus.THROTTLING;
        } else {
            return BackpressureStatus.REJECTING;
        }
    }

    private double calculateLoadScore(BackendLoadInfo load) {
        if (load == null) {
            return 0.0;
        }
        return load.calculateLoadScore(queueWeight, cpuWeight, memoryWeight, diskWeight, networkWeight);
    }

    public enum BackpressureStatus {
        NORMAL,
        WARNING,
        THROTTLING,
        REJECTING
    }
}
