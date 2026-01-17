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
public class StageMetrics {
    private int stageId;
    private String jobId;
    private String stageName;
    private int numTasks;
    private int completedTasks;
    private int failedTasks;
    private long inputBytes;
    private long outputBytes;
    private long shuffleReadBytes;
    private long shuffleWriteBytes;
    private long duration;
    private long submitTime;
    private long completionTime;
    private List<TaskMetricsData> taskDetails;

    public static StageMetrics copyOf(StageMetrics original) {
        if (original == null) {
            throw new IllegalArgumentException("Original StageMetrics must not be null");
        }

        StageMetrics copy = new StageMetrics();
        copy.setStageId(original.getStageId());
        copy.setJobId(original.getJobId());
        copy.setStageName(original.getStageName());
        copy.setNumTasks(original.getNumTasks());
        copy.setCompletedTasks(original.getCompletedTasks());
        copy.setFailedTasks(original.getFailedTasks());
        copy.setInputBytes(original.getInputBytes());
        copy.setOutputBytes(original.getOutputBytes());
        copy.setShuffleReadBytes(original.getShuffleReadBytes());
        copy.setShuffleWriteBytes(original.getShuffleWriteBytes());
        copy.setDuration(original.getDuration());
        copy.setSubmitTime(original.getSubmitTime());
        copy.setCompletionTime(original.getCompletionTime());

        if (original.getTaskDetails() != null) {
            List<TaskMetricsData> copiedTasks = original.getTaskDetails().stream()
                    .map(TaskMetricsData::copyOf)
                    .collect(Collectors.toList());
            copy.setTaskDetails(copiedTasks);
        } else {
            copy.setTaskDetails(null);
        }

        return copy;
    }
}
