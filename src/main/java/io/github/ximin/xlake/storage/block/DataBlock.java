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
package io.github.ximin.xlake.storage.block;

import io.github.ximin.xlake.storage.spi.Storage;
import java.io.Serializable;

/**
 * 表数据段抽象。
 *
 * <p>DataBlock 描述表中“一段可被访问的数据表达”，既可以对应热层的 LMDB/mmap 实例，
 * 也可以对应冷层的 Parquet/ORC 文件。这里强调的是逻辑段语义，而不是某一种具体文件实现。</p>
 */
public sealed interface DataBlock
        extends Serializable
        permits HotDataBlock, ColdDataBlock {

    /**
     * 数据块唯一标识。
     */
    String blockId();

    /**
     * 所属表标识。
     */
    String tableId();

    /**
     * 数据块类型。
     */
    Kind kind();

    default Layer layer() {
        return switch (kind()) {
            case MUTABLE_HOT, IMMUTABLE_HOT -> Layer.HOT;
            case IMMUTABLE_COLD -> Layer.COLD;
        };
    }

    /**
     * 数据块格式。
     */
    Format format();

    /**
     * 数据块物理位置。
     */
    Location location();

    /**
     * 数据块主键范围。
     */
    KeyRange keyRange();

    /**
     * 数据块内记录数。
     */
    long rowCount();

    /**
     * 数据块占用的物理字节数。
     */
    long sizeBytes();

    /**
     * 数据块对应的 schema 版本。
     */
    long schemaVersion();

    /**
     * 数据块可见性状态。
     */
    Visibility visibility();

    /**
     * 最小提交序号。
     */
    long minSequenceNumber();

    /**
     * 最大提交序号。
     */
    long maxSequenceNumber();

    /**
     * 与现有读规划代码兼容的别名方法。
     */
    default long getSize() {
        return sizeBytes();
    }

    /**
     * 与现有代码兼容的范围别名。
     */
    default KeyRange getRange() {
        return keyRange();
    }

    /**
     * 与现有代码兼容的物理位置别名。
     */
    default Location getFilePath() {
        return location();
    }

    /**
     * 与现有代码兼容的总大小别名。
     */
    default long getTotalSizeInBytes() {
        return sizeBytes();
    }

    enum Layer {
        HOT,
        COLD
    }

    /**
     * 数据块种类。
     */
    enum Kind {
        MUTABLE_HOT,
        IMMUTABLE_HOT,
        IMMUTABLE_COLD
    }

    /**
     * 数据块格式。
     */
    enum Format {
        LMDB,
        PARQUET,
        ORC,
        UNKNOWN
    }

    /**
     * 数据块可见性。
     */
    enum Visibility {
        ACTIVE,
        FLUSHING,
        ARCHIVED,
        DELETED
    }

    /**
     * 数据块物理位置描述。
     */
    record Location(
            String storageName,
            Storage.StoragePath storagePath,
            long offset,
            long length
    ) implements Serializable {
    }

    /**
     * 二进制主键范围。
     */
    record KeyRange(
            byte[] startKey,
            boolean startInclusive,
            byte[] endKey,
            boolean endInclusive
    ) implements Serializable {
        public boolean contains(byte[] key) {
            if (key == null) {
                return false;
            }
            boolean lower = startKey == null || startKey.length == 0
                    || compare(key, startKey) > 0
                    || startInclusive && compare(key, startKey) == 0;
            boolean upper = endKey == null || endKey.length == 0
                    || compare(key, endKey) < 0
                    || endInclusive && compare(key, endKey) == 0;
            return lower && upper;
        }

        public boolean overlaps(KeyRange other) {
            if (other == null) {
                return true;
            }
            if (endKey != null && endKey.length > 0 && other.startKey != null && other.startKey.length > 0) {
                int cmp = compare(endKey, other.startKey);
                if (cmp < 0 || cmp == 0 && (!endInclusive || !other.startInclusive)) {
                    return false;
                }
            }
            if (other.endKey != null && other.endKey.length > 0 && startKey != null && startKey.length > 0) {
                int cmp = compare(other.endKey, startKey);
                if (cmp < 0 || cmp == 0 && (!other.endInclusive || !startInclusive)) {
                    return false;
                }
            }
            return true;
        }

        private static int compare(byte[] left, byte[] right) {
            int minLength = Math.min(left.length, right.length);
            for (int i = 0; i < minLength; i++) {
                int cmp = Byte.compareUnsigned(left[i], right[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(left.length, right.length);
        }
    }
}
