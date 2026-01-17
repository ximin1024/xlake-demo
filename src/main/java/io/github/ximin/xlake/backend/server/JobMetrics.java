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

import java.util.List;
import java.util.stream.Collectors;

@Data
public class JobMetrics {
    private String jobId;
    private String sql;
    private JobStatus status;
    private long submitTime;
    private long startTime;
    private long endTime;
    private long duration;
    private long schedulingDelay;
    private long executorRunTime;
    private long executorCpuTime;
    private long resultSerializationTime;
    private long jvmGcTime;
    private long resultSize;
    private long diskBytesSpilled;
    private long memoryBytesSpilled;
    private long peakExecutionMemory;
    private long recordsRead;
    private long bytesRead;
    private long recordsWritten;
    private long bytesWritten;
    private long shuffleBytesRead;
    private long shuffleBytesWritten;
    private long shuffleRecordsRead;
    private long shuffleRecordsWritten;
    private int totalTasks;
    private int completedTasks;
    private int failedTasks;
    private int killedTasks;
    private double cpuUtilization;
    private double memoryUtilization;
    private double throughputBytesPerSec;
    private double taskSuccessRate;
    private double avgTaskTime;
    private List<StageMetrics> stages;
    private List<ExecutorMetrics> executors;
    private SystemMetrics systemMetrics;

    public static JobMetrics copyOf(JobMetrics original) {
        if (original == null) {
            throw new IllegalArgumentException("Original JobMetrics must not be null");
        }

        JobMetrics copy = new JobMetrics();

        // --- 复制基本类型和不可变引用 ---
        copy.setJobId(original.getJobId());
        copy.setSql(original.getSql());
        copy.setStatus(original.getStatus()); // enum or immutable
        copy.setSubmitTime(original.getSubmitTime());
        copy.setStartTime(original.getStartTime());
        copy.setEndTime(original.getEndTime());
        copy.setDuration(original.getDuration());
        copy.setSchedulingDelay(original.getSchedulingDelay());
        copy.setExecutorRunTime(original.getExecutorRunTime());
        copy.setExecutorCpuTime(original.getExecutorCpuTime());
        copy.setResultSerializationTime(original.getResultSerializationTime());
        copy.setJvmGcTime(original.getJvmGcTime());
        copy.setResultSize(original.getResultSize());
        copy.setDiskBytesSpilled(original.getDiskBytesSpilled());
        copy.setMemoryBytesSpilled(original.getMemoryBytesSpilled());
        copy.setPeakExecutionMemory(original.getPeakExecutionMemory());
        copy.setRecordsRead(original.getRecordsRead());
        copy.setBytesRead(original.getBytesRead());
        copy.setRecordsWritten(original.getRecordsWritten());
        copy.setBytesWritten(original.getBytesWritten());
        copy.setShuffleBytesRead(original.getShuffleBytesRead());
        copy.setShuffleBytesWritten(original.getShuffleBytesWritten());
        copy.setShuffleRecordsRead(original.getShuffleRecordsRead());
        copy.setShuffleRecordsWritten(original.getShuffleRecordsWritten());
        copy.setTotalTasks(original.getTotalTasks());
        copy.setCompletedTasks(original.getCompletedTasks());
        copy.setFailedTasks(original.getFailedTasks());
        copy.setKilledTasks(original.getKilledTasks());
        copy.setCpuUtilization(original.getCpuUtilization());
        copy.setMemoryUtilization(original.getMemoryUtilization());
        copy.setThroughputBytesPerSec(original.getThroughputBytesPerSec());
        copy.setTaskSuccessRate(original.getTaskSuccessRate());
        copy.setAvgTaskTime(original.getAvgTaskTime());

        // 1. stages: List<StageMetrics>
        if (original.getStages() != null) {
            List<StageMetrics> copiedStages = original.getStages().stream()
                    .map(StageMetrics::copyOf)
                    .collect(Collectors.toList());
            copy.setStages(copiedStages);
        } else {
            copy.setStages(null);
        }

        // 2. executors: List<ExecutorMetrics>
        if (original.getExecutors() != null) {
            List<ExecutorMetrics> copiedExecutors = original.getExecutors().stream()
                    .map(ExecutorMetrics::copyOf)
                    .collect(Collectors.toList());
            copy.setExecutors(copiedExecutors);
        } else {
            copy.setExecutors(null);
        }

        // 3. systemMetrics: SystemMetrics
        if (original.getSystemMetrics() != null) {
            copy.setSystemMetrics(SystemMetrics.copyOf(original.getSystemMetrics()));
        } else {
            copy.setSystemMetrics(null);
        }

        return copy;
    }
}
