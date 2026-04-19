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

import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.writer.Writer;
import lombok.Builder;

import java.util.List;
import java.util.Map;

public class KvWriteBuilder implements WriteBuilder<KvWrite, KvWriteBuilder> {
    private XlakeTable table;
    private List<Map<String, Object>> data;
    private byte[] key;
    private byte[] value;
    private Integer partitionHint;
    private Writer writer;

    public static KvWriteBuilder builder() {
        return new KvWriteBuilder();
    }

    public KvWriteBuilder table(XlakeTable table) {
        this.table = table;
        return this;
    }

    @Override
    public KvWriteBuilder withTable(XlakeTable table) {
        this.table = table;
        return this;
    }

    public KvWriteBuilder withData(List<Map<String, Object>> data) {
        this.data = data;
        return this;
    }

    public KvWriteBuilder key(byte[] key) {
        this.key = key;
        return this;
    }

    public KvWriteBuilder value(byte[] value) {
        this.value = value;
        return this;
    }

    public KvWriteBuilder partitionHint(Integer partitionHint) {
        this.partitionHint = partitionHint;
        return this;
    }

    /**
     * Get the key for direct KV operation.
     * @return key byte array, or null if not set
     */
    public byte[] key() {
        return key;
    }

    /**
     * Get the value for direct KV operation.
     * @return value byte array, or null if not set
     */
    public byte[] value() {
        return value;
    }

    @Override
    public KvWriteBuilder withWriter(Writer writer) {
        this.writer = writer;
        return this;
    }

    @Override
    public KvWrite build() {
        if (key != null && value != null) {
            return new KvWrite(table, key, value, partitionHint, writer);
        }
        if (partitionHint != null) {
            return new KvWrite(table, data, partitionHint, writer);
        }
        return new KvWrite(table, data, writer);
    }

    @Override
    public XlakeTable table() {
        return table;
    }
}
