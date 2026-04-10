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

import io.github.ximin.xlake.storage.block.ColdDataBlock;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.table.op.Find;
import io.github.ximin.xlake.table.op.Scan;
import java.io.IOException;
import java.util.List;

public class ColdDataBlockReader implements DataBlockReader {
    private final List<DataBlockReader> readers;

    public ColdDataBlockReader() {
        this(List.of(
                new ParquetBlockReader(),
                new OrcBlockReader()
        ));
    }

    public ColdDataBlockReader(List<DataBlockReader> readers) {
        this.readers = List.copyOf(readers);
    }

    @Override
    public boolean supports(DataBlock block) {
        return block instanceof ColdDataBlock;
    }

    @Override
    public Find.Result find(Find find, List<DataBlock> blocks) throws IOException {
        List<ColdDataBlock> visibleBlocks = ColdBlockFilters.visibleBlocks(blocks, find.snapshotId(), find.filter());
        if (visibleBlocks.isEmpty()) {
            return Find.Result.notFound();
        }
        boolean supported = false;
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed = visibleBlocks.stream()
                    .filter(reader::supports)
                    .map(DataBlock.class::cast)
                    .toList();
            if (routed.isEmpty()) {
                continue;
            }
            supported = true;
            Find.Result result = reader.find(find, routed);
            if (!result.success()) {
                return result;
            }
            if (result.foundRecord().isPresent()) {
                return result;
            }
        }
        if (supported) {
            return Find.Result.notFound();
        }
        return Find.Result.error("No available cold reader for requested file format", null);
    }

    @Override
    public Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException {
        List<ColdDataBlock> visibleBlocks = ColdBlockFilters.visibleBlocks(
                blocks,
                ColdBlockFilters.requestedSnapshotId(scan),
                scan.getPushedPredicate()
        );
        if (visibleBlocks.isEmpty()) {
            return Scan.Result.ok(List.of(), false);
        }
        boolean supported = false;
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed = visibleBlocks.stream()
                    .filter(reader::supports)
                    .map(DataBlock.class::cast)
                    .toList();
            if (routed.isEmpty()) {
                continue;
            }
            supported = true;
            Scan.Result result = reader.scan(scan, routed);
            if (!result.success()) {
                return result;
            }
            if (!result.data().isEmpty()) {
                return result;
            }
        }
        if (supported) {
            return Scan.Result.ok(List.of(), false);
        }
        return Scan.Result.error("No available cold reader for requested file format", null);
    }
}
