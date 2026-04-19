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

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

@Slf4j
public class HdfsWALWriter extends AbstractWALWriter {

    private final String executorId;
    private final String hdfsBasePath;
    private final Configuration hadoopConfig;

    private FSDataOutputStream currentStream;
    private String currentFilePath;

    public HdfsWALWriter(String storeId, String executorId, String hdfsBasePath,
                         long maxSizeBytes, long syncIntervalMs, Configuration hadoopConfig) {
        super(storeId, maxSizeBytes, syncIntervalMs);
        this.executorId = executorId;
        this.hdfsBasePath = hdfsBasePath;
        this.hadoopConfig = hadoopConfig;
    }

    @Override
    protected boolean isStreamOpen() {
        return currentStream != null;
    }

    @Override
    protected void doWrite(byte[] data) throws IOException {
        currentStream.write(data);
    }

    @Override
    protected void doSync() throws IOException {
        if (currentStream == null) {
            return;
        }
        try {
            currentStream.hsync();
        } catch (IOException hsyncError) {
            try {
                currentStream.hflush();
            } catch (IOException hflushError) {
                hsyncError.addSuppressed(hflushError);
                throw hsyncError;
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        if (currentStream != null) {
            try {
                currentStream.close();
            } finally {
                currentStream = null;
            }
        }
    }

    @Override
    protected void doCreateFile() throws IOException {
        String filePath = generateFilePath();
        currentFilePath = filePath;
        Path hdfsPath = new Path(filePath);

        FileSystem fs = hdfsPath.getFileSystem(hadoopConfig);
        fs.mkdirs(hdfsPath.getParent());

        currentStream = fs.create(hdfsPath, false);

        byte[] headerBytes = serializeHeader(executorId);
        doWrite(headerBytes);
        currentFileSize.set(headerBytes.length);

        recordCount.set(0);
        log.info("Created new WAL file on HDFS: {}", filePath);
    }

    private String generateFilePath() {
        long timestamp = System.currentTimeMillis();
        long seq = fileSequence.getAndIncrement();
        return String.format("%s/%s/wal_%s_%d_%d.nwl",
                hdfsBasePath, executorId, storeId, timestamp, seq);
    }
}
