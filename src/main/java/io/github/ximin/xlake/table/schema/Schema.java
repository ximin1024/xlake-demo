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

public record Schema(StructType structType, Map<String, String> properties,
                     long version, List<Constraint> constraints) {

    public Schema {
        if (structType == null)
            throw new IllegalArgumentException("Struct type cannot be null");
        properties = properties != null ? Map.copyOf(properties) : Map.of();
        constraints = constraints != null ? List.copyOf(constraints) : List.of();

        validateConstraints(structType, constraints);
        validateSinglePrimaryKey(constraints);
    }

    private static void validateConstraints(StructType structType, List<Constraint> constraints) {
        for (Constraint constraint : constraints) {
            if (constraint instanceof PrimaryKeyConstraint pkConstraint) {
                for (String fieldName : pkConstraint.fieldNames()) {
                    if (structType.field(fieldName).isEmpty()) {
                        throw new IllegalArgumentException(
                                "Primary key field '" + fieldName + "' does not exist in schema");
                    }
                }
            }
            // 其他约束类型的验证
        }
    }

    private static void validateSinglePrimaryKey(List<Constraint> constraints) {
        long primaryKeyCount = constraints.stream()
                .filter(c -> c instanceof PrimaryKeyConstraint)
                .count();
        if (primaryKeyCount > 1) {
            throw new IllegalArgumentException("Schema can only have one primary key constraint");
        }
    }

    public Schema(StructType structType) {
        this(structType, Map.of(), 1L, List.of());
    }

    public Schema(StructType structType, long version) {
        this(structType, Map.of(), version, List.of());
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

    // 获取主键约束（如果存在）
    public Optional<PrimaryKeyConstraint> primaryKeyConstraint() {
        return constraints.stream()
                .filter(PrimaryKeyConstraint.class::isInstance)
                .map(PrimaryKeyConstraint.class::cast)
                .findFirst();
    }

    // 获取主键字段列表（兼容旧API）
    public List<String> primaryKeys() {
        return primaryKeyConstraint()
                .map(PrimaryKeyConstraint::fieldNames)
                .orElse(List.of());
    }

    // 获取主键字段对象列表
    public List<StructField> primaryKeyFields() {
        return primaryKeys().stream()
                .map(pk -> structType.field(pk).orElseThrow())
                .toList();
    }

    // 检查是否是主键字段
    public boolean isPrimaryKeyField(String fieldName) {
        return primaryKeys().contains(fieldName);
    }

    // 检查是否包含主键
    public boolean hasPrimaryKey() {
        return primaryKeyConstraint().isPresent();
    }

    // 检查是否是联合主键
    public boolean isCompositePrimaryKey() {
        return primaryKeyConstraint()
                .map(PrimaryKeyConstraint::isComposite)
                .orElse(false);
    }

    // 获取主键数量
    public int primaryKeyCount() {
        return primaryKeys().size();
    }

    // 添加/更新主键约束
    public Schema withPrimaryKeyConstraint(PrimaryKeyConstraint newConstraint) {
        List<Constraint> newConstraints = new ArrayList<>(constraints);

        // 移除现有的主键约束
        newConstraints.removeIf(PrimaryKeyConstraint.class::isInstance);

        // 添加新的主键约束
        newConstraints.add(newConstraint);

        return new Schema(structType, properties, version, newConstraints);
    }

    // 便捷方法：添加主键
    public Schema withPrimaryKey(String fieldName) {
        PrimaryKeyConstraint existing = primaryKeyConstraint().orElse(null);
        List<String> newFieldNames;

        if (existing != null) {
            // 合并现有字段
            newFieldNames = new ArrayList<>(existing.fieldNames());
            if (!newFieldNames.contains(fieldName)) {
                newFieldNames.add(fieldName);
            }
        } else {
            // 新建主键
            newFieldNames = List.of(fieldName);
        }

        return withPrimaryKeyConstraint(new PrimaryKeyConstraint(newFieldNames));
    }

    // 便捷方法：设置主键列表
    public Schema withPrimaryKeys(List<String> fieldNames) {
        return withPrimaryKeyConstraint(new PrimaryKeyConstraint(fieldNames));
    }

    // 移除主键
    public Schema withoutPrimaryKey() {
        List<Constraint> newConstraints = constraints.stream()
                .filter(c -> !(c instanceof PrimaryKeyConstraint))
                .toList();
        return new Schema(structType, properties, version, newConstraints);
    }

    // 其他不变性方法
    public Schema withProperty(String key, String value) {
        Map<String, String> newProperties = new HashMap<>(properties);
        newProperties.put(key, value);
        return new Schema(structType, newProperties, version, constraints);
    }

    public Schema withVersion(long newVersion) {
        return new Schema(structType, properties, newVersion, constraints);
    }

    public Schema withConstraint(Constraint constraint) {
        List<Constraint> newConstraints = new ArrayList<>(constraints);
        newConstraints.add(constraint);
        return new Schema(structType, properties, version, newConstraints);
    }

    // Schema构建器
    public static SchemaBuilder builder() {
        return new SchemaBuilder();
    }

    public static class SchemaBuilder {
        private final List<StructField> fields = new ArrayList<>();
        private final Map<String, String> properties = new HashMap<>();
        private final List<Constraint> constraints = new ArrayList<>();
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

        public SchemaBuilder primaryKey(String fieldName) {
            Optional<PrimaryKeyConstraint> existing = constraints.stream()
                    .filter(PrimaryKeyConstraint.class::isInstance)
                    .map(PrimaryKeyConstraint.class::cast)
                    .findFirst();

            if (existing.isPresent()) {
                // 合并到现有主键
                List<String> newFieldNames = new ArrayList<>(existing.get().fieldNames());
                if (!newFieldNames.contains(fieldName)) {
                    newFieldNames.add(fieldName);
                }
                constraints.removeIf(PrimaryKeyConstraint.class::isInstance);
                constraints.add(new PrimaryKeyConstraint(newFieldNames));
            } else {
                // 新建主键
                constraints.add(new PrimaryKeyConstraint(List.of(fieldName)));
            }
            return this;
        }

        public SchemaBuilder primaryKeys(String... fieldNames) {
            constraints.removeIf(PrimaryKeyConstraint.class::isInstance);
            constraints.add(new PrimaryKeyConstraint(Arrays.asList(fieldNames)));
            return this;
        }

        public SchemaBuilder primaryKeyConstraint(PrimaryKeyConstraint constraint) {
            constraints.removeIf(PrimaryKeyConstraint.class::isInstance);
            constraints.add(constraint);
            return this;
        }

        public SchemaBuilder constraint(Constraint constraint) {
            constraints.add(constraint);
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
            return new Schema(structType, properties, version, constraints);
        }
    }
}
