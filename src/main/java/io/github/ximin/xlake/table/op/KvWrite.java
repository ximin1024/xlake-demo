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
package io.github.ximin.xlake.table.op;

import io.github.ximin.xlake.storage.table.record.KvRecord;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.table.record.RecordConverter;
import io.github.ximin.xlake.writer.Writer;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class KvWrite implements Write {
    private final static OpType TYPE = OpType.KV_WRITE;
    private final XlakeTable table;
    private final List<Map<String, Object>> data;
    private final byte[] key;
    private final byte[] value;
    @Getter
    private final Integer partitionHint;
    private final Writer writer;

    public KvWrite(XlakeTable table, List<Map<String, Object>> data, Writer writer) {
        this(table, data, null, null, null, writer);
    }

    public KvWrite(XlakeTable table, List<Map<String, Object>> data, Integer partitionHint, Writer writer) {
        this(table, data, null, null, partitionHint, writer);
    }

    public KvWrite(XlakeTable table, byte[] key, byte[] value, Integer partitionHint, Writer writer) {
        this(table, null, key, value, partitionHint, writer);
    }

    private KvWrite(XlakeTable table, List<Map<String, Object>> data, byte[] key, byte[] value, Integer partitionHint, Writer writer) {
        this.table = table;
        this.data = data;
        this.key = key;
        this.value = value;
        this.partitionHint = partitionHint;
        this.writer = writer;
    }

    @Override
    public Write.Result exec() {
        if (isDirectKvOperation()) {
            return execDirectKvWrite();
        }
        return execMapDataWrite();
    }

    public Insert.Result execAsInsert() {
        Write.Result result = exec();
        return new Insert.Result(result.success(), result.count(), result.message().orElse(null));
    }

    private boolean isDirectKvOperation() {
        return key != null && value != null;
    }

    private Write.Result execDirectKvWrite() {
        try {
            var result = writer.write(new KvRecord(key, value));
            if (result instanceof Write.Result writeResult) {
                return writeResult;
            }
            return result.success()
                    ? Write.Result.ok(result.message().orElse("success"), 1)
                    : Write.Result.error(result.message().orElse("write failed"));
        } catch (Exception e) {
            return Write.Result.error("Insert failed: " + e.getMessage());
        }
    }

    private Write.Result execMapDataWrite() {
        if (data == null || data.isEmpty()) {
            return Write.Result.ok(0);
        }

        List<Map<String, Object>> nonEmptyData = data.stream()
                .filter(Objects::nonNull)
                .filter(row -> !row.isEmpty())
                .toList();

        if (nonEmptyData.isEmpty()) {
            return Write.Result.ok(0);
        }

        try {
            List<KvRecord> records = RecordConverter.convertToKvRecords(table, nonEmptyData);
            if (records.isEmpty()) {
                return Write.Result.ok(0);
            }

            Write.Result batchResult = writer.batchWrite(records);
            if (batchResult.success()) {
                return batchResult;
            }

            long successCount = 0;
            for (KvRecord record : records) {
                try {
                    Write.Result singleResult = writer.write(record);
                    if (singleResult.success()) {
                        successCount++;
                    }
                } catch (Exception e) {
                    // 单条记录写入失败不影响整体操作成功状态
                }
            }

            return Write.Result.ok(successCount);
        } catch (Exception e) {
            return Write.Result.error("Batch write failed: " + e.getMessage());
        }
    }

    @Override
    public long writeSize() {
        if (isDirectKvOperation()) {
            return 1;
        }
        return data != null ? data.size() : 0;
    }

    @Override
    public OpType type() {
        return TYPE;
    }
}
