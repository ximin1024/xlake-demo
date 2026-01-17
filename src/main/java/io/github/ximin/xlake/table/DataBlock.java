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

import lombok.Getter;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.Objects;

public sealed interface DataBlock<K extends Comparable<K>>
        permits
        DataBlock.MemoryMappedDataBlock, DataBlock.ParquetFileDataBlock {

    Range<K> getRange();

    long getSize();

    Path getFilePath();

    /**
     * 获取数据块的总大小（字节）,一个数据块对应一个文件
     */
    long getTotalSizeInBytes();

    default boolean containsKey(K key) {
        return getRange().contains(key);
    }

    /**
     * 检查数据块是否与指定的文件位置相关
     */
    default boolean isInFile(Path filePath) {
        return this.getFilePath().equals(filePath);
    }

    record MemoryMappedDataBlock<K extends Comparable<K>>(
            Range<K> range,
            long size,
            Path filePath,           // 内存映射文件路径
            long fileOffset,         // 在内存映射文件中的偏移量
            @Getter long memoryAddress,      // 共享内存地址
            @Getter long dataSize,           // 数据大小（字节）
            @Getter String sharedMemoryKey   // 共享内存唯一标识
    ) implements DataBlock<K> {

        public MemoryMappedDataBlock {
            Objects.requireNonNull(range, "Range cannot be null");
            Objects.requireNonNull(filePath, "File path cannot be null");
            Objects.requireNonNull(sharedMemoryKey, "Shared memory key cannot be null");
            if (size < 0) throw new IllegalArgumentException("Size cannot be negative");
            if (fileOffset < 0) throw new IllegalArgumentException("File offset cannot be negative");
            if (memoryAddress < 0) throw new IllegalArgumentException("Memory address cannot be negative");
            if (dataSize < 0) throw new IllegalArgumentException("Data size cannot be negative");
        }

        @Override
        public Range<K> getRange() {
            return null;
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public Path getFilePath() {
            return null;
        }

        @Override
        public long getTotalSizeInBytes() {
            return dataSize;
        }
    }

    record ParquetFileDataBlock<K extends Comparable<K>>(
            Range<K> range,
            long size,
            Path filePath,           // Parquet文件路径
            long blockSize       // 数据块大小（字节）
    ) implements DataBlock<K> {

        public ParquetFileDataBlock {
            Objects.requireNonNull(range, "Range cannot be null");
            Objects.requireNonNull(filePath, "File path cannot be null");
            if (size < 0) throw new IllegalArgumentException("Size cannot be negative");
            if (blockSize < 0) throw new IllegalArgumentException("Block size cannot be negative");
        }

        @Override
        public Range<K> getRange() {
            return null;
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public Path getFilePath() {
            return null;
        }

        @Override
        public long getTotalSizeInBytes() {
            return blockSize;
        }
    }

    record Range<K extends Comparable<K>>(K start, K end) implements Serializable {

        public Range {
            Objects.requireNonNull(start, "Start cannot be null");
            Objects.requireNonNull(end, "End cannot be null");
            if (start.compareTo(end) > 0) {
                throw new IllegalArgumentException("Start cannot be greater than end");
            }
        }

        public boolean contains(K key) {
            Objects.requireNonNull(key, "Key cannot be null");
            return key.compareTo(start) >= 0 && key.compareTo(end) < 0;
        }

        public boolean overlaps(Range<K> other) {
            Objects.requireNonNull(other, "Other range cannot be null");
            return this.start.compareTo(other.end) < 0 && other.start.compareTo(this.end) < 0;
        }

        @Override
        public String toString() {
            return "[" + start + ", " + end + ")";
        }

        @SuppressWarnings("unchecked")
        public long size() {
            if (start instanceof Number startNum && end instanceof Number endNum) {
                return endNum.longValue() - startNum.longValue();
            }
            // 对于非数值类型，返回-1表示无法计算
            return -1;
        }
    }
}
