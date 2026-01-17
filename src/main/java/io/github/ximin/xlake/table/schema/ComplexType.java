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

sealed interface ComplexType extends XlakeType permits ArrayType, MapType, StructType {
    default XlakeType.TypeCategory category() {
        return XlakeType.TypeCategory.COMPLEX;
    }
}

record ArrayType(XlakeType elementType, boolean isNullable, String description) implements ComplexType {
    public ArrayType {
        if (elementType == null) throw new IllegalArgumentException("Element type cannot be null");
        description = description != null ? description : "Array of " + elementType.name();
    }

    public ArrayType(XlakeType elementType) {
        this(elementType, true, null);
    }

    public ArrayType(XlakeType elementType, boolean isNullable) {
        this(elementType, isNullable, null);
    }

    public String name() {
        return "array<" + elementType.name() + ">";
    }

    public int id() {
        return 100;
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new ArrayType(elementType, nullable, description);
    }
}

record MapType(XlakeType keyType, XlakeType valueType, boolean isNullable, String description) implements ComplexType {
    public MapType {
        if (keyType == null) throw new IllegalArgumentException("Key type cannot be null");
        if (valueType == null) throw new IllegalArgumentException("Value type cannot be null");
        description = description != null ? description : "Map from " + keyType.name() + " to " + valueType.name();
    }

    public MapType(XlakeType keyType, XlakeType valueType) {
        this(keyType, valueType, true, null);
    }

    public String name() {
        return "map<" + keyType.name() + "," + valueType.name() + ">";
    }

    public int id() {
        return 101;
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new MapType(keyType, valueType, nullable, description);
    }
}

// 结构体字段
record StructField(String name, XlakeType dataType, String comment, Map<String, String> metadata) {
    public StructField {
        if (name == null || name.trim().isEmpty()) throw new IllegalArgumentException("Field name cannot be empty");
        if (dataType == null) throw new IllegalArgumentException("Field data type cannot be null");
        comment = comment != null ? comment : "";
        metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
    }

    public StructField(String name, XlakeType dataType) {
        this(name, dataType, "", Map.of());
    }

    public StructField(String name, XlakeType dataType, String comment) {
        this(name, dataType, comment, Map.of());
    }

    public StructField withComment(String newComment) {
        return new StructField(name, dataType, newComment, metadata);
    }

    public StructField withMetadata(String key, String value) {
        Map<String, String> newMetadata = new HashMap<>(metadata);
        newMetadata.put(key, value);
        return new StructField(name, dataType, comment, newMetadata);
    }
}

// 结构体类型
record StructType(List<StructField> fields, boolean isNullable, String description) implements ComplexType {
    public StructType {
        if (fields == null || fields.isEmpty())
            throw new IllegalArgumentException("Struct must have at least one field");
        // 检查字段名重复
        Set<String> fieldNames = new HashSet<>();
        for (StructField field : fields) {
            if (!fieldNames.add(field.name())) {
                throw new IllegalArgumentException("Duplicate field name: " + field.name());
            }
        }
        fields = List.copyOf(fields);
        description = description != null ? description : "Struct with " + fields.size() + " fields";
    }

    public StructType(List<StructField> fields) {
        this(fields, true, null);
    }

    public String name() {
        return "struct<" + fields.stream()
                .map(f -> f.name() + ": " + f.dataType().name())
                .reduce((a, b) -> a + ", " + b)
                .orElse("") + ">";
    }

    public int id() {
        return 102;
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new StructType(fields, nullable, description);
    }

    public Optional<StructField> field(String name) {
        return fields.stream().filter(f -> f.name().equals(name)).findFirst();
    }

    public XlakeType fieldType(String name) {
        return field(name).map(StructField::dataType)
                .orElseThrow(() -> new IllegalArgumentException("Field not found: " + name));
    }
}
