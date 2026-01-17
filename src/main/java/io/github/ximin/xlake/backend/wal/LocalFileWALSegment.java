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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

public class LocalFileWALSegment implements WALSegment, AutoCloseable {
    private final Path filePath;
    private final WALConfig config;
    private FileChannel fileChannel;
    private long currentPosition;

    public LocalFileWALSegment(Path filePath, WALConfig config) throws IOException {
        this.filePath = filePath;
        this.config = config;
        this.fileChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        this.currentPosition = fileChannel.size();
    }

    @Override
    public synchronized void append(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        fileChannel.write(buffer, currentPosition);
        currentPosition += buffer.remaining();
    }

    @Override
    public void sync() throws IOException {
        fileChannel.force(true);
    }

    @Override
    public void recover(Consumer<byte[]> recordConsumer) throws IOException {
        long position = 0;
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

        while (position < fileChannel.size()) {
            // 读取记录大小
            sizeBuffer.clear();
            fileChannel.read(sizeBuffer, position);
            sizeBuffer.flip();
            int recordSize = sizeBuffer.getInt();
            position += 4;

            // 读取记录数据
            ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
            fileChannel.read(recordBuffer, position);
            position += recordSize;

            recordConsumer.accept(recordBuffer.array());
        }
    }

    @Override
    public void close() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            sync();
            fileChannel.close();
        }
    }
}
