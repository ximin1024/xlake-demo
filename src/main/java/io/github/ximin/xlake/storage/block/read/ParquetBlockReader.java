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
import io.github.ximin.xlake.storage.table.key.DefaultKeyCodec;
import io.github.ximin.xlake.storage.table.key.KeyCodec;
import io.github.ximin.xlake.storage.table.record.KeyValueRecordView;
import io.github.ximin.xlake.storage.table.record.ProjectionAdapter;
import io.github.ximin.xlake.table.op.Find;
import io.github.ximin.xlake.table.op.Scan;
import io.github.ximin.xlake.table.record.RecordView;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ParquetBlockReader extends AbstractColdFormatBlockReader {
    private final ProjectionAdapter projectionAdapter;
    private final KeyCodec keyCodec;

    public ParquetBlockReader() {
        this(new ProjectionAdapter(), new DefaultKeyCodec());
    }

    public ParquetBlockReader(ProjectionAdapter projectionAdapter) {
        this(projectionAdapter, new DefaultKeyCodec());
    }

    public ParquetBlockReader(ProjectionAdapter projectionAdapter, KeyCodec keyCodec) {
        super(DataBlock.Format.PARQUET);
        this.projectionAdapter = projectionAdapter;
        this.keyCodec = keyCodec;
    }

    @Override
    protected Find.Result findVisible(Find find, List<ColdDataBlock> blocks) throws IOException {
        byte[] encodedKey = keyCodec.encodePrimaryKey(find.primaryKeyValues());
        for (ColdDataBlock block : blocks) {
            if (block.keyRange() != null && !block.keyRange().contains(encodedKey)) {
                continue;
            }
            Path path = new Path(block.location().storagePath().location());
            try (var reader = AvroParquetReader.<GenericRecord>builder(path)
                    .withConf(new Configuration())
                    .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    byte[] key = toBytes(record.get("key"));
                    if (!java.util.Arrays.equals(encodedKey, key)) {
                        continue;
                    }
                    RecordView view = new KeyValueRecordView(key, toBytes(record.get("value")));
                    if (find.filter() != null && !find.filter().evaluate(view)) {
                        continue;
                    }
                    return Find.Result.found(projectionAdapter.project(view, find.projection()));
                }
            } catch (Exception e) {
                return Find.Result.error("Parquet find failed for " + path + ": " + e.getMessage(), e);
            }
        }
        return Find.Result.notFound();
    }

    @Override
    protected Scan.Result scanVisible(Scan scan, List<ColdDataBlock> blocks) throws IOException {
        List<RecordView> rows = new ArrayList<>();
        for (ColdDataBlock block : blocks) {
            if (scan.keyRange() != null && block.keyRange() != null && !block.keyRange().overlaps(scan.keyRange())) {
                continue;
            }
            Path path = new Path(block.location().storagePath().location());
            try (var reader = AvroParquetReader.<GenericRecord>builder(path)
                    .withConf(new Configuration())
                    .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    byte[] key = toBytes(record.get("key"));
                    byte[] value = toBytes(record.get("value"));
                    if (!inRange(key, scan)) {
                        continue;
                    }
                    RecordView view = new KeyValueRecordView(key, value);
                    if (!matchesPredicate(view, scan)) {
                        continue;
                    }
                    rows.add(projectionAdapter.project(view, scan.projections()));
                }
            } catch (Exception e) {
                return Scan.Result.error("Parquet scan failed for " + path + ": " + e.getMessage(), e);
            }
        }
        return Scan.Result.ok(rows, false);
    }

    private boolean matchesPredicate(RecordView view, Scan scan) {
        if (scan.getPushedPredicate() == null) {
            return true;
        }
        return scan.getPushedPredicate().evaluate(view);
    }

    private boolean inRange(byte[] key, Scan scan) {
        return scan.keyRange() == null || scan.keyRange().contains(key);
    }

    private byte[] toBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer byteBuffer) {
            ByteBuffer duplicate = byteBuffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }
        if (value instanceof CharSequence sequence) {
            return sequence.toString().getBytes(StandardCharsets.UTF_8);
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }
}
