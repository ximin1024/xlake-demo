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
package io.github.ximin.xlake.storage.table.read;

import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.block.read.DataBlockReader;
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.table.op.Find;
import io.github.ximin.xlake.table.op.Read;
import io.github.ximin.xlake.table.op.Scan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BlockRoutingTableReader implements TableReader {
    private final TableStore tableStore;
    private final List<DataBlockReader> readers;

    public BlockRoutingTableReader(TableStore tableStore, List<DataBlockReader> readers) {
        this.tableStore = tableStore;
        this.readers = List.copyOf(readers);
    }

    @Override
    public Read.Result read(Read read) throws IOException {
        if (read instanceof Find find) {
            return find(find);
        }
        if (read instanceof Scan scan) {
            return scan(scan);
        }
        return Scan.Result.error("Unsupported read type: " + read.getClass().getName(), null);
    }

    @Override
    public Find.Result find(Find find) throws IOException {
        List<DataBlock> blocks = plannedBlocks(find.getDataBlocks());
        boolean sawUnsupported = false;
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed = blocksFor(reader, blocks);
            if (routed.isEmpty()) {
                continue;
            }
            Find.Result result = reader.find(find, routed);
            if (!result.success()) {
                sawUnsupported = true;
                continue;
            }
            if (result.foundRecord().isPresent()) {
                return result;
            }
        }
        if (sawUnsupported) {
            return Find.Result.error("No available reader can complete the find request", null);
        }
        return Find.Result.notFound();
    }

    @Override
    public Scan.Result scan(Scan scan) throws IOException {
        List<DataBlock> blocks = plannedBlocks(scan.getDataBlocks());
        List<Object> rows = new ArrayList<>();
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed = blocksFor(reader, blocks);
            if (routed.isEmpty()) {
                continue;
            }
            Scan.Result result = reader.scan(scan, routed);
            if (!result.success()) {
                return result;
            }
            rows.addAll(result.data());
        }
        return Scan.Result.ok(rows, false);
    }

    private List<DataBlock> plannedBlocks(List<DataBlock> blocks) {
        if (blocks == null || blocks.isEmpty()) {
            return tableStore.currentDataBlocks();
        }
        return blocks;
    }

    private List<DataBlock> blocksFor(DataBlockReader reader, List<DataBlock> blocks) {
        return blocks.stream()
                .filter(reader::supports)
                .toList();
    }
}
