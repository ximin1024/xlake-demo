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
import io.github.ximin.xlake.storage.lmdb.LmdbInstance;
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.storage.table.key.KeyCodec;
import io.github.ximin.xlake.storage.table.record.KeyValueRecordView;
import io.github.ximin.xlake.storage.table.record.ProjectionAdapter;
import io.github.ximin.xlake.table.op.Find;
import io.github.ximin.xlake.table.op.Read;
import io.github.ximin.xlake.table.op.Scan;
import io.github.ximin.xlake.table.record.RecordView;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LmdbTableReader implements TableReader, DataBlockReader {
    private final TableStore tableStore;
    private final KeyCodec keyCodec;
    private final ProjectionAdapter projectionAdapter;

    public LmdbTableReader(TableStore tableStore) {
        this(tableStore, tableStore.getKeyCodec(), new ProjectionAdapter());
    }

    public LmdbTableReader(TableStore tableStore, KeyCodec keyCodec) {
        this(tableStore, keyCodec, new ProjectionAdapter());
    }

    public LmdbTableReader(TableStore tableStore,
                           KeyCodec keyCodec,
                           ProjectionAdapter projectionAdapter) {
        this.tableStore = tableStore;
        this.keyCodec = keyCodec;
        this.projectionAdapter = projectionAdapter;
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
        return find(find, find.getDataBlocks());
    }

    @Override
    public Find.Result find(Find find, List<DataBlock> blocks) throws IOException {
        byte[] encodedKey = keyCodec.encodePrimaryKey(find.primaryKeyValues());
        for (LmdbInstance instance : resolveInstances(blocks)) {
            byte[] value = instance.get(encodedKey);
            if (value != null) {
                return Find.Result.found(new KeyValueRecordView(encodedKey, value));
            }
        }
        return Find.Result.notFound();
    }

    @Override
    public Scan.Result scan(Scan scan) throws IOException {
        return scan(scan, scan.getDataBlocks());
    }

    @Override
    public Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException {
        List<RecordView> rows = new ArrayList<>();
        List<LmdbInstance> instances = resolveInstances(blocks, scan.keyRange());
        for (LmdbInstance instance : instances) {
            try (var iterator = instance.closeableIterator()) {
                while (iterator.hasNext()) {
                    Pair<byte[], byte[]> pair = iterator.next();
                    RecordView view = new KeyValueRecordView(pair.getLeft(), pair.getRight());
                    if ((scan.keyRange() == null || scan.keyRange().contains(pair.getLeft()))
                            && matchesPredicate(view, scan)) {
                        rows.add(projectionAdapter.project(view, scan.projections()));
                    }
                }
            }
        }
        return Scan.Result.ok(new ArrayList<>(rows), false);
    }

    @Override
    public boolean supports(DataBlock block) {
        return block.layer() == DataBlock.Layer.HOT && block.format() == DataBlock.Format.LMDB;
    }

    private List<LmdbInstance> resolveInstances(List<DataBlock> plannedBlocks) {
        return resolveInstances(plannedBlocks, null);
    }

    private List<LmdbInstance> resolveInstances(List<DataBlock> plannedBlocks, DataBlock.KeyRange keyRange) {
        if (plannedBlocks == null || plannedBlocks.isEmpty()) {
            return filterByKeyRange(tableStore.getAllInstancesDescending(), keyRange);
        }
        List<LmdbInstance> instances = new ArrayList<>();
        for (DataBlock block : plannedBlocks) {
            if (!supports(block)) {
                continue;
            }
            if (keyRange != null && block.keyRange() != null && !block.keyRange().overlaps(keyRange)) {
                continue;
            }
            LmdbInstance instance = tableStore.getInstance(block.blockId());
            if (instance != null) {
                instances.add(instance);
            }
        }
        instances.sort(Comparator.comparing(LmdbInstance::instanceId).reversed());
        return instances;
    }

    private List<LmdbInstance> filterByKeyRange(List<LmdbInstance> instances, DataBlock.KeyRange keyRange) {
        if (keyRange == null) {
            return instances;
        }
        return instances.stream()
                .filter(instance -> {
                    DataBlock.KeyRange instanceRange = new DataBlock.KeyRange(instance.minKey(), true, instance.maxKey(), true);
                    return instanceRange.overlaps(keyRange);
                })
                .toList();
    }

    private boolean matchesPredicate(RecordView view, Scan scan) {
        if (scan.getPushedPredicate() == null) {
            return true;
        }
        return scan.getPushedPredicate().evaluate(view);
    }
}
