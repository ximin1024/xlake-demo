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

public sealed interface ComplexType extends XlakeType permits ArrayType, MapType, StructType {
    default TypeCategory category() {
        return TypeCategory.COMPLEX;
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

