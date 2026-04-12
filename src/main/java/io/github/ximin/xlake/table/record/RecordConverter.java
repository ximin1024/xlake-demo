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
package io.github.ximin.xlake.table.record;

import io.github.ximin.xlake.storage.table.record.KvRecord;
import io.github.ximin.xlake.table.XlakeTable;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Slf4j
public final class RecordConverter {

    private RecordConverter() {}

    public static List<KvRecord> convertToKvRecords(XlakeTable table, List<Map<String, Object>> data) {
        if (isEmptyData(data)) {
            return List.of();
        }

        List<KvRecord> records = new ArrayList<>(data.size());
        for (Map<String, Object> rowData : data) {
            KvRecord record = convertSingleRecord(table, rowData);
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    public static KvRecord convertSingleRecord(XlakeTable table, Map<String, Object> rowData) {
        if (rowData == null || rowData.isEmpty()) {
            return null;
        }

        Object keyObj = rowData.get("key");
        Object valueObj = rowData.get("value");

        if (keyObj == null && !rowData.isEmpty()) {
            var entry = rowData.entrySet().iterator().next();
            keyObj = entry.getKey();
            valueObj = entry.getValue();
        }

        if (keyObj == null) {
            return null;
        }

        byte[] key = serializeToBytes(keyObj);
        byte[] value = serializeToBytes(valueObj);

        return new KvRecord(key, value);
    }

    public static byte[] serializeToBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }

        if (value instanceof byte[] bytes) {
            return bytes;
        }

        if (value instanceof String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }

        String strValue = value.toString();
        return strValue.getBytes(StandardCharsets.UTF_8);
    }

    public static boolean isEmptyData(List<Map<String, Object>> data) {
        return data == null || data.isEmpty();
    }

    public static List<KvRecord> convertFromCollection(Collection<?> inputData) {
        if (inputData == null || inputData.isEmpty()) {
            return List.of();
        }

        List<KvRecord> records = new ArrayList<>(inputData.size());
        int skipped = 0;

        for (Object item : inputData) {
            try {
                KvRecord record = convertFromObject(item);
                if (record != null) {
                    records.add(record);
                } else {
                    skipped++;
                    log.debug("Skipped unconvertible record: {}", item.getClass().getName());
                }
            } catch (Exception e) {
                skipped++;
                log.warn("Failed to convert record: {}, error: {}", item.getClass().getName(), e.getMessage());
            }
        }

        if (skipped > 0) {
            log.info("Converted {}/{} records, skipped {}", records.size(), inputData.size(), skipped);
        }

        return records;
    }

    @SuppressWarnings("unchecked")
    private static KvRecord convertFromObject(Object object) {
        if (object == null) {
            return null;
        }

        if (object instanceof KvRecord kvRecord) {
            return kvRecord;
        }

        if (object instanceof RecordView recordView) {
            byte[] key = recordView.key();
            byte[] value = recordView.value();
            if (key == null || value == null) {
                log.warn("RecordView has null key or value");
                return null;
            }
            return new KvRecord(key, value);
        }

        if (object instanceof Map.Entry<?, ?> entry) {
            try {
                byte[] key = serializeToBytes(entry.getKey());
                byte[] value = serializeToBytes(entry.getValue());
                return new KvRecord(key, value);
            } catch (Exception e) {
                log.error("Failed to serialize Map.Entry to bytes", e);
                return null;
            }
        }

        log.debug("Unsupported record type: {}", object.getClass().getName());
        return null;
    }
}
