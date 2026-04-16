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

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BackendLoadInfo {
    private int queueLength;
    private double cpuUsage;
    private double memoryUsage;
    private double diskIoUsage;
    private double networkUsage;
    private long lastUpdated;

    public double calculateLoadScore(double queueWeight, double cpuWeight, double memoryWeight,
            double diskWeight, double networkWeight) {
        double normalizedQueue = Math.min(queueLength / 100.0, 1.0);
        // Cap all metrics at 1.0 to prevent unbounded values
        double normalizedCpu = Math.min(cpuUsage, 1.0);
        double normalizedMemory = Math.min(memoryUsage, 1.0);
        double normalizedDisk = Math.min(diskIoUsage, 1.0);
        double normalizedNetwork = Math.min(networkUsage, 1.0);
        return normalizedQueue * queueWeight + normalizedCpu * cpuWeight + normalizedMemory * memoryWeight
                + normalizedDisk * diskWeight + normalizedNetwork * networkWeight;
    }

    public void update(int queueLength, double cpuUsage, double memoryUsage, double diskIoUsage,
            double networkUsage) {
        this.queueLength = queueLength;
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
        this.diskIoUsage = diskIoUsage;
        this.networkUsage = networkUsage;
        this.lastUpdated = System.currentTimeMillis();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int queueLength;
        private double cpuUsage;
        private double memoryUsage;
        private double diskIoUsage;
        private double networkUsage;

        public Builder queueLength(int queueLength) {
            this.queueLength = queueLength;
            return this;
        }

        public Builder cpuUsage(double cpuUsage) {
            this.cpuUsage = cpuUsage;
            return this;
        }

        public Builder memoryUsage(double memoryUsage) {
            this.memoryUsage = memoryUsage;
            return this;
        }

        public Builder diskIoUsage(double diskIoUsage) {
            this.diskIoUsage = diskIoUsage;
            return this;
        }

        public Builder networkUsage(double networkUsage) {
            this.networkUsage = networkUsage;
            return this;
        }

        public BackendLoadInfo build() {
            BackendLoadInfo info = new BackendLoadInfo();
            info.queueLength = this.queueLength;
            info.cpuUsage = this.cpuUsage;
            info.memoryUsage = this.memoryUsage;
            info.diskIoUsage = this.diskIoUsage;
            info.networkUsage = this.networkUsage;
            info.lastUpdated = System.currentTimeMillis();
            return info;
        }
    }
}
