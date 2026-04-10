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
package io.github.ximin.xlake.storage.block.catalog;

import io.github.ximin.xlake.storage.block.ColdDataBlock;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.block.HotDataBlock;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryBlockCatalog implements BlockCatalog {
    private final String tableId;
    private final ConcurrentNavigableMap<String, DataBlock> blocks = new ConcurrentSkipListMap<>();

    public InMemoryBlockCatalog(String tableId) {
        this.tableId = tableId;
    }

    @Override
    public String tableId() {
        return tableId;
    }

    @Override
    public void upsert(DataBlock block) {
        blocks.put(block.blockId(), block);
    }

    @Override
    public Optional<DataBlock> find(String blockId) {
        return Optional.ofNullable(blocks.get(blockId));
    }

    @Override
    public void updateVisibility(String blockId, DataBlock.Visibility visibility) {
        blocks.computeIfPresent(blockId, (ignored, block) -> switch (block) {
            case HotDataBlock hot -> new HotDataBlock(
                    hot.blockId(),
                    hot.tableId(),
                    hot.kind(),
                    hot.format(),
                    hot.location(),
                    hot.keyRange(),
                    hot.rowCount(),
                    hot.sizeBytes(),
                    hot.schemaVersion(),
                    visibility,
                    hot.minSequenceNumber(),
                    hot.maxSequenceNumber(),
                    hot.mappedOffset(),
                    hot.mappedLength(),
                    hot.regionKey()
            );
            case ColdDataBlock cold -> new ColdDataBlock(
                    cold.blockId(),
                    cold.tableId(),
                    cold.kind(),
                    cold.format(),
                    cold.fileFormat(),
                    cold.location(),
                    cold.keyRange(),
                    cold.rowCount(),
                    cold.sizeBytes(),
                    cold.schemaVersion(),
                    cold.snapshotId(),
                    cold.partitionValues(),
                    cold.statistics(),
                    visibility,
                    cold.minSequenceNumber(),
                    cold.maxSequenceNumber()
            );
        });
    }

    @Override
    public void remove(String blockId) {
        blocks.remove(blockId);
    }

    @Override
    public List<DataBlock> currentBlocks() {
        return blocks.descendingMap().values().stream()
                .filter(block -> block.visibility() != DataBlock.Visibility.DELETED)
                .toList();
    }
}
