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
package io.github.ximin.xlake.storage.flush;

import io.github.ximin.xlake.storage.block.ColdDataBlock;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.lmdb.LmdbInstance;
import io.github.ximin.xlake.storage.spi.Storage;
import io.github.ximin.xlake.storage.table.TableStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DefaultFlushCoordinator implements FlushCoordinator {
    private static final Schema KEY_VALUE_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "NebulakeKeyValueRecord",
              "fields": [
                {"name": "key", "type": "bytes"},
                {"name": "value", "type": "bytes"}
              ]
            }
            """);
    private final Function<String, TableStore> tableStoreProvider;

    public DefaultFlushCoordinator(Function<String, TableStore> tableStoreProvider) {
        this.tableStoreProvider = tableStoreProvider;
    }

    @Override
    public FlushPlan plan(String tableId, LmdbInstance instance, String remoteBasePath) {
        Storage.StoragePath targetPath = resolveStoragePath(remoteBasePath, instance.instanceId() + ".parquet");
        return new FlushPlan(
                "flush-" + instance.instanceId(),
                tableId,
                List.of(instance.instanceId()),
                new DataBlock.Location(
                        "remote",
                        targetPath,
                        0L,
                        instance.getTotalUsageSize()
                ),
                DataBlock.Format.PARQUET,
                new DataBlock.KeyRange(
                        instance.minKey(),
                        true,
                        instance.maxKey(),
                        true
                ),
                instance.getTotalRecords(),
                0L,
                System.currentTimeMillis()
        );
    }

    @Override
    public void beforeFlush(String tableId, LmdbInstance instance) {
        instance.markForDeletion();
        tableStoreProvider.apply(tableId).markBlockVisibility(instance.instanceId(), DataBlock.Visibility.FLUSHING);
    }

    @Override
    public void afterFlush(String tableId, LmdbInstance instance, FlushPlan plan) {
        TableStore tableStore = tableStoreProvider.apply(tableId);
        Storage.StoragePath storagePath = plan.targetLocation().storagePath();
        String targetUri = toUri(storagePath);
        writeParquet(instance, storagePath, targetUri);
        long fileSize = fileSize(storagePath, targetUri);
        tableStore.registerColdBlock(new ColdDataBlock(
                plan.planId(),
                plan.tableId(),
                DataBlock.Kind.IMMUTABLE_COLD,
                plan.targetFormat(),
                plan.targetFormat(),
                plan.targetLocation(),
                plan.targetKeyRange(),
                plan.estimatedRows(),
                fileSize,
                plan.schemaVersion(),
                -1L,
                Map.of(),
                new ColdDataBlock.Statistics(
                        plan.estimatedRows(),
                        fileSize,
                        0L,
                        0L
                ),
                DataBlock.Visibility.ARCHIVED,
                0L,
                0L
        ));
        tableStore.markBlockVisibility(instance.instanceId(), DataBlock.Visibility.DELETED);
    }

    @Override
    public void onFlushFailure(String tableId, LmdbInstance instance, Throwable cause) {
        tableStoreProvider.apply(tableId).markBlockVisibility(instance.instanceId(), DataBlock.Visibility.ACTIVE);
    }

    private void writeParquet(LmdbInstance instance, Storage.StoragePath storagePath, String targetUri) {
        try {
            if ("file".equals(storagePath.scheme())) {
                Path outputPath = Paths.get(URI.create(targetUri));
                if (outputPath.getParent() != null) {
                    Files.createDirectories(outputPath.getParent());
                }
            }
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(targetUri))
                    .withSchema(KEY_VALUE_SCHEMA)
                    .withConf(new Configuration())
                    .build()) {
                try (var iterator = instance.closeableIterator()) {
                    while (iterator.hasNext()) {
                        Pair<byte[], byte[]> pair = iterator.next();
                        GenericRecord record = new GenericData.Record(KEY_VALUE_SCHEMA);
                        record.put("key", java.nio.ByteBuffer.wrap(pair.getLeft()));
                        record.put("value", java.nio.ByteBuffer.wrap(pair.getRight()));
                        writer.write(record);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write parquet for instance " + instance.instanceId(), e);
        }
    }

    private long fileSize(Storage.StoragePath storagePath, String targetUri) {
        try {
            if (!"file".equals(storagePath.scheme())) {
                return 0L;
            }
            Path outputPath = Paths.get(URI.create(targetUri));
            return Files.exists(outputPath) ? Files.size(outputPath) : 0L;
        } catch (Exception e) {
            throw new RuntimeException("Failed to read parquet file size: " + targetUri, e);
        }
    }

    private static Storage.StoragePath resolveStoragePath(String base, String relative) {
        if (base == null || base.isBlank()) {
            throw new IllegalArgumentException("remoteBasePath is required");
        }
        URI uri;
        try {
            uri = URI.create(base);
        } catch (IllegalArgumentException ignored) {
            uri = null;
        }
        if (uri == null || uri.getScheme() == null) {
            String normalizedBase = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
            return new Storage.StoragePath("file", normalizedBase + "/" + relative);
        }

        String scheme = uri.getScheme();
        String locationPrefix;
        if ("file".equals(scheme)) {
            locationPrefix = uri.getPath();
        } else {
            String authority = uri.getRawAuthority();
            String path = uri.getRawPath();
            String prefix = authority != null ? "//" + authority : "";
            locationPrefix = prefix + (path != null ? path : "");
        }
        if (locationPrefix == null || locationPrefix.isEmpty()) {
            locationPrefix = "/";
        }
        String normalizedPrefix = locationPrefix.endsWith("/") ? locationPrefix.substring(0, locationPrefix.length() - 1) : locationPrefix;
        return new Storage.StoragePath(scheme, normalizedPrefix + "/" + relative);
    }

    private static String toUri(Storage.StoragePath path) {
        return path.scheme() + ":" + path.location();
    }
}
