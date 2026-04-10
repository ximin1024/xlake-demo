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
package io.github.ximin.xlake.storage.table.key;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DefaultKeyCodec implements KeyCodec {
    private static final byte TYPE_NULL = 0;
    private static final byte TYPE_BINARY = 1;
    private static final byte TYPE_STRING = 2;
    private static final byte TYPE_INT = 3;
    private static final byte TYPE_LONG = 4;
    private static final byte TYPE_FLOAT = 5;
    private static final byte TYPE_DOUBLE = 6;
    private static final byte TYPE_BOOLEAN = 7;
    private static final byte TYPE_DECIMAL = 8;
    private static final byte TYPE_DATE = 9;
    private static final byte TYPE_TIMESTAMP = 10;
    private static final byte TYPE_FALLBACK = 127;

    @Override
    public byte[] encodePrimaryKey(Map<String, Comparable> primaryKeyValues) throws IOException {
        Objects.requireNonNull(primaryKeyValues, "primaryKeyValues cannot be null");
        if (primaryKeyValues.isEmpty()) {
            throw new IllegalArgumentException("Primary key values cannot be empty");
        }
        List<Map.Entry<String, Comparable>> entries = primaryKeyValues.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             DataOutputStream data = new DataOutputStream(out)) {
            data.writeInt(entries.size());
            for (Map.Entry<String, Comparable> entry : entries) {
                data.writeUTF(entry.getKey());
                writeValue(data, entry.getValue());
            }
            data.flush();
            return out.toByteArray();
        }
    }

    private void writeValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(TYPE_NULL);
            out.writeInt(0);
            return;
        }
        if (value instanceof byte[] bytes) {
            out.writeByte(TYPE_BINARY);
            out.writeInt(bytes.length);
            out.write(bytes);
            return;
        }
        if (value instanceof String stringValue) {
            writeUtf8(out, TYPE_STRING, stringValue);
            return;
        }
        if (value instanceof Integer intValue) {
            out.writeByte(TYPE_INT);
            out.writeInt(Integer.BYTES);
            out.writeInt(intValue);
            return;
        }
        if (value instanceof Long longValue) {
            out.writeByte(TYPE_LONG);
            out.writeInt(Long.BYTES);
            out.writeLong(longValue);
            return;
        }
        if (value instanceof Float floatValue) {
            out.writeByte(TYPE_FLOAT);
            out.writeInt(Float.BYTES);
            out.writeFloat(floatValue);
            return;
        }
        if (value instanceof Double doubleValue) {
            out.writeByte(TYPE_DOUBLE);
            out.writeInt(Double.BYTES);
            out.writeDouble(doubleValue);
            return;
        }
        if (value instanceof Boolean booleanValue) {
            out.writeByte(TYPE_BOOLEAN);
            out.writeInt(1);
            out.writeBoolean(booleanValue);
            return;
        }
        if (value instanceof BigDecimal decimalValue) {
            writeUtf8(out, TYPE_DECIMAL, decimalValue.toPlainString());
            return;
        }
        if (value instanceof LocalDate localDate) {
            writeUtf8(out, TYPE_DATE, localDate.toString());
            return;
        }
        if (value instanceof Instant instant) {
            writeUtf8(out, TYPE_TIMESTAMP, instant.toString());
            return;
        }
        writeUtf8(out, TYPE_FALLBACK, value.toString());
    }

    private void writeUtf8(DataOutputStream out, byte type, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeByte(type);
        out.writeInt(bytes.length);
        out.write(bytes);
    }
}
