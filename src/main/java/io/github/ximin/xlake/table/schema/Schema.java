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
package io.github.ximin.xlake.table.schema;

import java.util.*;

public record Schema(StructType structType, Map<String, String> properties, long version) {
    public Schema {
        if (structType == null) throw new IllegalArgumentException("Struct type cannot be null");
        properties = properties != null ? Map.copyOf(properties) : Map.of();
    }

    public Schema(StructType structType) {
        this(structType, Map.of(), 1L);
    }

    public Schema(StructType structType, long version) {
        this(structType, Map.of(), version);
    }

    public List<StructField> fields() {
        return structType.fields();
    }

    public Optional<StructField> field(String name) {
        return structType.field(name);
    }

    public XlakeType fieldType(String name) {
        return structType.fieldType(name);
    }

    public Schema withProperty(String key, String value) {
        Map<String, String> newProperties = new HashMap<>(properties);
        newProperties.put(key, value);
        return new Schema(structType, newProperties, version);
    }

    public Schema withVersion(long newVersion) {
        return new Schema(structType, properties, newVersion);
    }

    // Schema构建器
    public static SchemaBuilder builder() {
        return new SchemaBuilder();
    }

    public static class SchemaBuilder {
        private final List<StructField> fields = new ArrayList<>();
        private final Map<String, String> properties = new HashMap<>();
        private long version = 1L;

        public SchemaBuilder field(String name, XlakeType dataType) {
            fields.add(new StructField(name, dataType));
            return this;
        }

        public SchemaBuilder field(String name, XlakeType dataType, String comment) {
            fields.add(new StructField(name, dataType, comment));
            return this;
        }

        public SchemaBuilder field(StructField field) {
            fields.add(field);
            return this;
        }

        public SchemaBuilder property(String key, String value) {
            properties.put(key, value);
            return this;
        }

        public SchemaBuilder version(long version) {
            this.version = version;
            return this;
        }

        public Schema build() {
            StructType structType = new StructType(fields);
            return new Schema(structType, properties, version);
        }
    }
}
