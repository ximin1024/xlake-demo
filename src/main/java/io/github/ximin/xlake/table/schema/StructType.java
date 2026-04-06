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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public record StructType(List<StructField> fields, boolean isNullable, String description) implements ComplexType {
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
