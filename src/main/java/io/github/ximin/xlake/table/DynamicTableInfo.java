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
package io.github.ximin.xlake.table;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DynamicTableInfo implements Serializable {

    private final List<Snapshot> snapshots;
    private final Map<Long, List<String>> snapshotFiles;
    private final long currentSequenceNumber;
    private final long lastCommitTimestamp;
    private final Map<String, String> statistics;

    private DynamicTableInfo(Builder builder) {
        this.snapshots = builder.snapshots != null ? List.copyOf(builder.snapshots) : List.of();
        this.snapshotFiles = builder.snapshotFiles != null ? Map.copyOf(builder.snapshotFiles) : Map.of();
        this.currentSequenceNumber = builder.currentSequenceNumber;
        this.lastCommitTimestamp = builder.lastCommitTimestamp;
        this.statistics = builder.statistics != null ? Map.copyOf(builder.statistics) : Map.of();
    }

    public List<Snapshot> snapshots() {
        return snapshots;
    }

    public int numSnapshots() {
        return snapshots.size();
    }

    public Snapshot currentSnapshot() {
        return snapshots.isEmpty() ? null : snapshots.get(snapshots.size() - 1);
    }

    public long currentSnapshotId() {
        Snapshot current = currentSnapshot();
        return current != null ? current.snapshotId() : -1L;
    }

    public void refresh() {
    }

    public Snapshot oldestSnapshot() {
        return snapshots.isEmpty() ? null : snapshots.get(0);
    }

    public boolean isEmpty() {
        return snapshots.isEmpty();
    }

    public Map<Long, List<String>> snapshotFiles() {
        return snapshotFiles;
    }

    public long currentSequenceNumber() {
        return currentSequenceNumber;
    }

    public long lastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    public Map<String, String> statistics() {
        return statistics;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<Snapshot> snapshots;
        private Map<Long, List<String>> snapshotFiles;
        private long currentSequenceNumber;
        private long lastCommitTimestamp;
        private Map<String, String> statistics;

        public Builder withSnapshots(List<Snapshot> snapshots) {
            this.snapshots = snapshots;
            return this;
        }

        public Builder withSnapshotFiles(Map<Long, List<String>> snapshotFiles) {
            this.snapshotFiles = snapshotFiles;
            return this;
        }

        public Builder withCurrentSequenceNumber(long seqNum) {
            this.currentSequenceNumber = seqNum;
            return this;
        }

        public Builder withLastCommitTimestamp(long timestamp) {
            this.lastCommitTimestamp = timestamp;
            return this;
        }

        public Builder withStatistics(Map<String, String> statistics) {
            this.statistics = statistics;
            return this;
        }

        public DynamicTableInfo build() {
            return new DynamicTableInfo(this);
        }
    }
}
