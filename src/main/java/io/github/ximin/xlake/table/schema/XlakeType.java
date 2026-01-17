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

public sealed interface XlakeType permits ComplexType, PrimitiveType {

    String name();

    int id();

    boolean isNullable();

    String description();

    <T> T accept(TypeVisitor<T> visitor);

    XlakeType copyWithNullable(boolean nullable);

    enum TypeCategory {
        PRIMITIVE, COMPLEX, COLLECTION, STRUCT
    }

    default TypeCategory category() {
        return TypeCategory.PRIMITIVE;
    }
}

interface TypeVisitor<T> {
    T visit(BooleanType type);

    T visit(Int8Type type);

    T visit(Int32Type type);

    T visit(Int64Type type);

    T visit(Float32Type type);

    T visit(Float64Type type);

    T visit(DecimalType type);

    T visit(StringType type);

    T visit(BinaryType type);

    T visit(DateType type);

    T visit(TimestampType type);

    T visit(ArrayType type);

    T visit(MapType type);

    T visit(StructType type);
}


