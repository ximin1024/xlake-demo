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
package io.github.ximin.xlake.storage.block.read;

import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.storage.block.ColdDataBlock;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.table.op.KvScan;
import io.github.ximin.xlake.table.op.Scan;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class ColdBlockFilters {
    private ColdBlockFilters() {
    }

    static List<ColdDataBlock> visibleBlocks(List<DataBlock> blocks, long requestedSnapshotId, Expression predicate) {
        return blocks.stream()
                .filter(ColdDataBlock.class::isInstance)
                .map(ColdDataBlock.class::cast)
                .filter(block -> isSnapshotVisible(block, requestedSnapshotId))
                .filter(block -> matchesPartitionValues(block, predicate))
                .collect(Collectors.toList());
    }

    static long requestedSnapshotId(Scan scan) {
        if (scan instanceof KvScan kvScan) {
            return kvScan.snapshotId();
        }
        return -1L;
    }

    private static boolean isSnapshotVisible(ColdDataBlock block, long requestedSnapshotId) {
        if (block.visibility() == DataBlock.Visibility.DELETED || block.visibility() == DataBlock.Visibility.FLUSHING) {
            return false;
        }
        if (requestedSnapshotId < 0 || block.snapshotId() < 0) {
            return true;
        }
        return block.snapshotId() <= requestedSnapshotId;
    }

    private static boolean matchesPartitionValues(ColdDataBlock block, Expression predicate) {
        if (predicate == null || block.partitionValues().isEmpty()) {
            return true;
        }
        Map<String, Comparable> row = block.partitionValues().entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
        try {
            // todo 这里还没有将row转成RecordView，所以现返回false
            // return predicate.evaluate(row);
            return false;
        } catch (RuntimeException ignored) {
            return true;
        }
    }
}
