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

abstract class AbstractColdFormatBlockReader implements DataBlockReader {
    private final DataBlock.Format fileFormat;

    protected AbstractColdFormatBlockReader(DataBlock.Format fileFormat) {
        this.fileFormat = fileFormat;
    }

    @Override
    public boolean supports(DataBlock block) {
        return block instanceof ColdDataBlock coldBlock
                && coldBlock.layer() == DataBlock.Layer.COLD
                && coldBlock.fileFormat() == fileFormat;
    }

    @Override
    public final Find.Result find(Find find, List<DataBlock> blocks) throws IOException {
        List<ColdDataBlock> visibleBlocks = ColdBlockFilters.visibleBlocks(blocks, find.snapshotId(), find.filter());
        if (visibleBlocks.isEmpty()) {
            return Find.Result.notFound();
        }
        return findVisible(find, visibleBlocks);
    }

    @Override
    public final Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException {
        List<ColdDataBlock> visibleBlocks = ColdBlockFilters.visibleBlocks(
                blocks,
                ColdBlockFilters.requestedSnapshotId(scan),
                scan.getPushedPredicate()
        );
        if (visibleBlocks.isEmpty()) {
            return Scan.Result.ok(List.of(), false);
        }
        return scanVisible(scan, visibleBlocks);
    }

    protected abstract Find.Result findVisible(Find find, List<ColdDataBlock> blocks) throws IOException;

    protected abstract Scan.Result scanVisible(Scan scan, List<ColdDataBlock> blocks) throws IOException;
}
