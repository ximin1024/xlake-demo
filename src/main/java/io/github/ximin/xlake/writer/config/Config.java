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
package io.github.ximin.xlake.writer.config;

import io.github.ximin.xlake.common.config.XlakeConfig;
import io.github.ximin.xlake.common.config.XlakeOptions;

public class Config {

    public static class ReadConf implements java.io.Serializable {
        private final long snapshotId;
        private final int batchSize;

        private ReadConf(Builder builder) {
            this.snapshotId = builder.snapshotId;
            this.batchSize = builder.batchSize;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public int batchSize() {
            return batchSize;
        }

        public static ReadConf fromConfig(XlakeConfig config) {
            return ReadConf.builder()
                    .snapshotId(0L)  // snapshotId is typically set by caller, not from config
                    .batchSize(config.get(XlakeOptions.READ_BATCH_SIZE))
                    .build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private long snapshotId = 0L;
            private int batchSize = 1024;

            public Builder snapshotId(long snapshotId) {
                this.snapshotId = snapshotId;
                return this;
            }

            public Builder batchSize(int batchSize) {
                this.batchSize = batchSize;
                return this;
            }

            public ReadConf build() {
                return new ReadConf(this);
            }
        }
    }

    @lombok.Builder
    @lombok.Getter
    public static class WriteConf implements java.io.Serializable {
        private String mmapPath;
        private long mmapSize;

        public static WriteConf fromConfig(XlakeConfig config) {
            return WriteConf.builder()
                    .mmapPath(config.get(XlakeOptions.STORAGE_MMAP_PATH))
                    .mmapSize(config.get(XlakeOptions.STORAGE_MMAP_SIZE))
                    .build();
        }
    }

    public static class CompactionConf implements java.io.Serializable {
        private final long thresholdBytes;
        private final int maxConcurrentCompaction;

        private CompactionConf(Builder builder) {
            this.thresholdBytes = builder.thresholdBytes;
            this.maxConcurrentCompaction = builder.maxConcurrentCompaction;
        }

        public long thresholdBytes() {
            return thresholdBytes;
        }

        public int maxConcurrentCompaction() {
            return maxConcurrentCompaction;
        }

        public static CompactionConf fromConfig(XlakeConfig config) {
            return CompactionConf.builder()
                    .thresholdBytes(config.get(XlakeOptions.STORAGE_FLUSH_THRESHOLD_BYTES))
                    .maxConcurrentCompaction(config.get(XlakeOptions.STORAGE_COMPACTION_MAX_CONCURRENT))
                    .build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private long thresholdBytes = 256L * 1024 * 1024;
            private int maxConcurrentCompaction = 4;

            public Builder thresholdBytes(long thresholdBytes) {
                this.thresholdBytes = thresholdBytes;
                return this;
            }

            public Builder maxConcurrentCompaction(int maxConcurrentCompaction) {
                this.maxConcurrentCompaction = maxConcurrentCompaction;
                return this;
            }

            public CompactionConf build() {
                return new CompactionConf(this);
            }
        }
    }
}
