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
package io.github.ximin.xlake.storage.table;

import io.github.ximin.xlake.storage.block.ColdDataBlock;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.block.HotDataBlock;
import io.github.ximin.xlake.storage.block.catalog.BlockCatalog;
import io.github.ximin.xlake.storage.block.catalog.InMemoryBlockCatalog;
import io.github.ximin.xlake.storage.block.read.ColdDataBlockReader;
import io.github.ximin.xlake.storage.lmdb.LmdbInstance;
import io.github.ximin.xlake.storage.spi.Storage;
import io.github.ximin.xlake.storage.table.key.DefaultKeyCodec;
import io.github.ximin.xlake.storage.table.key.KeyCodec;
import io.github.ximin.xlake.storage.table.key.PrimaryKeyKeyCodec;
import io.github.ximin.xlake.storage.table.read.BlockRoutingTableReader;
import io.github.ximin.xlake.storage.table.read.LmdbTableReader;
import io.github.ximin.xlake.storage.table.read.TableReader;
import io.github.ximin.xlake.storage.table.write.LmdbTableWriter;
import io.github.ximin.xlake.storage.table.write.TableWriter;
import io.github.ximin.xlake.table.PrimaryKey;
import io.github.ximin.xlake.table.TableId;
import io.github.ximin.xlake.table.op.Read;
import io.github.ximin.xlake.table.op.Write;
import io.github.ximin.xlake.table.op.WriteBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkEnv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
public class TableStore implements TableStorage {

    @Getter
    private final String tableIdentifier;
    private final TableId tableId;
    private final int writableNum;
    @Getter
    private final KeyCodec keyCodec;
    private final TableReader reader;
    private final TableWriter writer;
    private final BlockCatalog blockCatalog;
    private final ConcurrentNavigableMap<String, LmdbInstance> readOnlyInstances = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<String, LmdbInstance> writableInstances = new ConcurrentSkipListMap<>();

    public TableStore(String tableIdentifier, int writableNum) {
        this(tableIdentifier, writableNum, new DefaultKeyCodec());
    }

    public TableStore(String tableIdentifier, int writableNum, PrimaryKey primaryKey) {
        this(tableIdentifier, writableNum, createKeyCodec(primaryKey));
    }

    public TableStore(String tableIdentifier, int writableNum, List<String> primaryKeyFields) {
        this(tableIdentifier, writableNum, createKeyCodec(primaryKeyFields));
    }

    public TableStore(String tableIdentifier, int writableNum, KeyCodec keyCodec) {
        this.tableIdentifier = tableIdentifier;
        this.tableId = parseTableId(tableIdentifier);
        this.writableNum = writableNum;
        this.keyCodec = keyCodec;
        this.writer = new LmdbTableWriter();
        this.blockCatalog = new InMemoryBlockCatalog(tableIdentifier);
        this.reader = new BlockRoutingTableReader(this, List.of(
                new LmdbTableReader(this),
                new ColdDataBlockReader()
        ));
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    public LmdbInstance getCurrentWritableInstance() {
        Map.Entry<String, LmdbInstance> entry = writableInstances.firstEntry();
        return entry == null ? null : entry.getValue();
    }

    public void addWritableInstance(LmdbInstance instance) {
        if (writableInstances.size() >= writableNum) {
            log.warn("Table[{}] writable instances limit reached ({}), reject add: {}",
                    tableIdentifier, writableNum, instance.instanceId());
            // 可以选择抛出异常或直接返回，这里选择返回
            return;
        }

        LmdbInstance prev = writableInstances.putIfAbsent(instance.instanceId(), instance);
        if (prev != null) {
            log.warn("Instance {} already exists in writable map", instance.instanceId());
            return;
        }
        blockCatalog.upsert(toHotDataBlock(instance));
    }

    public void addReadOnlyInstance(LmdbInstance instance) {
        readOnlyInstances.put(instance.instanceId(), instance);
        blockCatalog.upsert(toHotDataBlock(instance));
    }

    // 轮换可写实例：将最旧的实例从 writable 移至 readonly。
    public void rotateWritableInstance() {
        // pollFirstEntry 是原子操作，安全地移除并返回第一个元素
        Map.Entry<String, LmdbInstance> entry = writableInstances.pollFirstEntry();
        if (entry != null) {
            LmdbInstance old = entry.getValue();
            // 如果 instanceId 排序就是时间排序，则这里自然保持了顺序
            readOnlyInstances.put(entry.getKey(), old);
            blockCatalog.upsert(toHotDataBlock(old));
            log.info("Rotated instance {} to readonly", entry.getKey());
        }
    }

    public boolean isWritable() {
        return !writableInstances.isEmpty();
    }

    public int remainingWritable() {
        return writableNum - writableInstances.size();
    }

    @Override
    public Write.Result write(Write write) throws IOException {
        Objects.requireNonNull(write, "write cannot be null");
        return writer.write(write);
    }

    public Write.Result write(WriteBuilder builder, LmdbInstance instance) throws IOException {
        return writer.write(builder, new LmdbTableWriter.LmdbWriteContext(instance));
    }

    @Override
    public Read.Result read(Read read) throws IOException {
        Objects.requireNonNull(read, "read cannot be null");
        return reader.read(read);
    }

    @Override
    public List<DataBlock> currentDataBlocks() {
        return blockCatalog.currentBlocks();
    }

    @Override
    public void flush() throws IOException {
        for (LmdbInstance instance : writableInstances.values()) {
            if (!instance.readOnly()) {
                instance.markAsReadOnly();
            }
        }
    }

    public void forEachInstance(Consumer<LmdbInstance> consumer) {
        Objects.requireNonNull(consumer);
        readOnlyInstances.values().forEach(consumer);
        writableInstances.values().forEach(consumer);
    }

    public Stream<LmdbInstance> streamInstances() {
        return Stream.concat(
                readOnlyInstances.values().stream(),
                writableInstances.values().stream()
        );
    }

    public LmdbInstance getInstance(String instanceId) {
        LmdbInstance instance = writableInstances.get(instanceId);
        if (instance != null) {
            return instance;
        }
        return readOnlyInstances.get(instanceId);
    }

    // 按照 instanceId 倒序排列（最新的在前）。
    public List<LmdbInstance> getAllInstancesDescending() {
        List<LmdbInstance> all = new ArrayList<>();
        // descendingMap() 获取倒序视图
        all.addAll(writableInstances.descendingMap().values());
        all.addAll(readOnlyInstances.descendingMap().values());
        return all;
    }

    public List<LmdbInstance> getReadOnlyInstances() {
        return new ArrayList<>(readOnlyInstances.values());
    }

    public void removeInstance(LmdbInstance wrapper) {
        writableInstances.remove(wrapper.instanceId());
        readOnlyInstances.remove(wrapper.instanceId());
        blockCatalog.remove(wrapper.instanceId());
    }

    public void registerColdBlock(ColdDataBlock dataBlock) {
        blockCatalog.upsert(dataBlock);
    }

    public void markBlockVisibility(String blockId, DataBlock.Visibility visibility) {
        blockCatalog.updateVisibility(blockId, visibility);
    }

    public BlockCatalog blockCatalog() {
        return blockCatalog;
    }

    public void close() {
        getAllInstancesDescending().forEach(instance -> {
            try {
                instance.close();
            } catch (Exception ignored) {
            }
        });
    }

    private HotDataBlock toHotDataBlock(LmdbInstance instance) {
        return new HotDataBlock(
                instance.instanceId(),
                tableIdentifier,
                instance.readOnly() ? DataBlock.Kind.IMMUTABLE_HOT : DataBlock.Kind.MUTABLE_HOT,
                DataBlock.Format.LMDB,
                new DataBlock.Location(
                        resolveLocalHost(),
                        new Storage.StoragePath("file", instance.path().toString()),
                        0L,
                        instance.getTotalUsageSize()
                ),
                new DataBlock.KeyRange(
                        instance.minKey(),
                        true,
                        instance.maxKey(),
                        true
                ),
                instance.getTotalRecords(),
                instance.getTotalUsageSize(),
                0L,
                instance.isMarkedForDeletion() ? DataBlock.Visibility.FLUSHING : DataBlock.Visibility.ACTIVE,
                0L,
                0L,
                0L,
                instance.getTotalUsageSize(),
                instance.instanceId()
        );
    }

    private static String resolveLocalHost() {
        return SparkEnv.get() != null && SparkEnv.get().rpcEnv() != null && SparkEnv.get().rpcEnv().address() != null
                ? SparkEnv.get().rpcEnv().address().host()
                : "local";
    }

    private static TableId parseTableId(String tableIdentifier) {
        String[] parts = tableIdentifier.split("\\.");
        if (parts.length >= 3) {
            return new TableId(parts[1], parts[2]);
        }
        if (parts.length == 2) {
            return new TableId(parts[0], parts[1]);
        }
        return new TableId("default", tableIdentifier);
    }

    private static KeyCodec createKeyCodec(PrimaryKey primaryKey) {
        if (primaryKey == null || primaryKey.isEmpty()) {
            return new DefaultKeyCodec();
        }
        return new PrimaryKeyKeyCodec(primaryKey.fields());
    }

    private static KeyCodec createKeyCodec(List<String> primaryKeyFields) {
        if (primaryKeyFields == null || primaryKeyFields.isEmpty()) {
            return new DefaultKeyCodec();
        }
        return new PrimaryKeyKeyCodec(primaryKeyFields);
    }
}
