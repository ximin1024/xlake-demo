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

import java.util.Map;

public class JavaTypeConverter implements TypeConverter<Class<?>> {
    private static final Map<Class<?>, XlakeType> JAVA_TO_DATA_TYPE = Map.ofEntries(
            Map.entry(Boolean.class, new BooleanType(true)),
            Map.entry(byte.class, new Int8Type(false)),
            Map.entry(Byte.class, new Int8Type(true)),
            Map.entry(int.class, new Int32Type(false)),
            Map.entry(Integer.class, new Int32Type(true)),
            Map.entry(long.class, new Int64Type(false)),
            Map.entry(Long.class, new Int64Type(true)),
            Map.entry(float.class, new Float32Type(false)),
            Map.entry(Float.class, new Float32Type(true)),
            Map.entry(double.class, new Float64Type(false)),
            Map.entry(Double.class, new Float64Type(true)),
            Map.entry(String.class, new StringType(true)),
            Map.entry(byte[].class, new BinaryType(true)),
            Map.entry(java.sql.Date.class, new DateType(true)),
            Map.entry(java.util.Date.class, new TimestampType(true)),
            Map.entry(java.time.LocalDate.class, new DateType(true)),
            Map.entry(java.time.LocalDateTime.class, new TimestampType(true))
    );

    @Override
    public XlakeType toXlakeType(Class<?> javaType) {
        XlakeType result = JAVA_TO_DATA_TYPE.get(javaType);
        if (result == null) {
            throw new IllegalArgumentException("Unsupported Java type: " + javaType.getName());
        }
        return result;
    }

    @Override
    public Class<?> fromXlakeType(XlakeType dataType) {
        return dataType.accept(new JavaTypeVisitor());
    }

    private static class JavaTypeVisitor implements TypeVisitor<Class<?>> {
        public Class<?> visit(BooleanType type) {
            return Boolean.class;
        }

        public Class<?> visit(Int8Type type) {
            return Byte.class;
        }

        public Class<?> visit(Int32Type type) {
            return Integer.class;
        }

        public Class<?> visit(Int64Type type) {
            return Long.class;
        }

        public Class<?> visit(Float32Type type) {
            return Float.class;
        }

        public Class<?> visit(Float64Type type) {
            return Double.class;
        }

        public Class<?> visit(DecimalType type) {
            return java.math.BigDecimal.class;
        }

        public Class<?> visit(StringType type) {
            return String.class;
        }

        public Class<?> visit(BinaryType type) {
            return byte[].class;
        }

        public Class<?> visit(DateType type) {
            return java.time.LocalDate.class;
        }

        public Class<?> visit(TimestampType type) {
            return java.time.LocalDateTime.class;
        }

        public Class<?> visit(ArrayType type) {
            Class<?> elementClass = type.elementType().accept(this);
            return java.util.List.class; // 简化处理
        }

        public Class<?> visit(MapType type) {
            return Map.class;
        }

        public Class<?> visit(StructType type) {
            return Map.class;
        }
    }
}
