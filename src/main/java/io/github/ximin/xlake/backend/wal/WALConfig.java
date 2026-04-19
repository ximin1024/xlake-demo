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
package io.github.ximin.xlake.backend.wal;

public record WALConfig(String logDir,
                        String logFilePrefix,
                        long segmentSize,
                        boolean syncOnWrite,
                        int bufferSize,
                        String compressionCodec,
                        String storeId,
                        String executorId,
                        String hdfsBasePath,
                        long syncIntervalMs,
                        long appendTimeoutMs) {
    public static final WALConfig DEFAULT = new WALConfig(
            "wal_logs",
            "wal",
            1024 * 1024 * 100,
            true,
            8192,
            "NONE",
            "default",
            "local",
            "",
            1000L,
            30000L
    );

    public WALConfig(String logDir, String logFilePrefix, long segmentSize, boolean syncOnWrite, int bufferSize) {
        this(logDir, logFilePrefix, segmentSize, syncOnWrite, bufferSize, "NONE", "default", "local", "", 1000L, 30000L);
    }

    public WALConfig(String logDir, String logFilePrefix, long segmentSize, boolean syncOnWrite, int bufferSize, String compressionCodec) {
        this(logDir, logFilePrefix, segmentSize, syncOnWrite, bufferSize, compressionCodec, "default", "local", "", 1000L, 30000L);
    }

    public WALConfig(String logDir, String logFilePrefix, long segmentSize, boolean syncOnWrite, int bufferSize,
                     String compressionCodec, String storeId, String executorId, String hdfsBasePath, long syncIntervalMs) {
        this(logDir, logFilePrefix, segmentSize, syncOnWrite, bufferSize, compressionCodec, storeId, executorId, hdfsBasePath, syncIntervalMs, 30000L);
    }

    public boolean isHdfsEnabled() {
        return hdfsBasePath != null && !hdfsBasePath.isBlank();
    }
}
