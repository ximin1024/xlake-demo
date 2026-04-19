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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Slf4j
public class LocalWALWriter extends AbstractWALWriter {

    private final String localBasePath;

    private FileChannel currentChannel;
    private String currentFilePath;

    public LocalWALWriter(String storeId, String localBasePath, long maxSizeBytes, long syncIntervalMs) {
        super(storeId, maxSizeBytes, syncIntervalMs);
        this.localBasePath = localBasePath;
    }

    @Override
    protected boolean isStreamOpen() {
        return currentChannel != null && currentChannel.isOpen();
    }

    @Override
    protected void doWrite(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        while (buffer.hasRemaining()) {
            currentChannel.write(buffer);
        }
    }

    @Override
    protected void doSync() throws IOException {
        if (currentChannel != null && currentChannel.isOpen()) {
            currentChannel.force(true);
        }
    }

    @Override
    protected void doClose() throws IOException {
        if (currentChannel != null) {
            try {
                currentChannel.close();
            } finally {
                currentChannel = null;
            }
        }
    }

    @Override
    protected void doCreateFile() throws IOException {
        String filePath = generateFilePath();
        currentFilePath = filePath;
        Path path = Paths.get(filePath);

        Files.createDirectories(path.getParent());

        currentChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);

        byte[] headerBytes = serializeHeader("local");
        doWrite(headerBytes);
        currentFileSize.set(headerBytes.length);

        recordCount.set(0);
        log.info("Created new local WAL file: {}", filePath);
    }

    private String generateFilePath() {
        long timestamp = System.currentTimeMillis();
        long seq = fileSequence.getAndIncrement();
        return String.format("%s/wal_%s_%d_%d.nwl",
                localBasePath, storeId, timestamp, seq);
    }
}
