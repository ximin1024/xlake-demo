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

sealed interface PrimitiveType extends XlakeType
        permits
        BooleanType,
        Int8Type,
        Int32Type,
        Int64Type,
        Float32Type,
        Float64Type,
        DecimalType,
        StringType,
        BinaryType,
        DateType,
        TimestampType {

    int DEFAULT_TYPE_ID = 0;

    default int id() {
        return DEFAULT_TYPE_ID;
    }

    default TypeCategory category() {
        return TypeCategory.PRIMITIVE;
    }
}

record BooleanType(boolean isNullable, String description) implements PrimitiveType {
    public BooleanType {
        description = description != null ? description : "Xlake boolean type";
    }

    public BooleanType(boolean isNullable) {
        this(isNullable, "Xlake boolean type");
    }

    public BooleanType() {
        this(true, "Xlake boolean type");
    }

    public String name() {
        return "boolean";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new BooleanType(nullable, description);
    }
}

record Int8Type(boolean isNullable, String description) implements PrimitiveType {
    public Int8Type {
        description = description != null ? description : "Xlake 8-bit integer";
    }

    public Int8Type(boolean isNullable) {
        this(isNullable, "Xlake 8-bit integer");
    }

    public Int8Type() {
        this(true, "Xlake 8-bit integer");
    }

    public String name() {
        return "int8";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new Int8Type(nullable, description);
    }
}

record Int32Type(boolean isNullable, String description) implements PrimitiveType {
    public Int32Type {
        description = description != null ? description : "Xlake 32-bit integer";
    }

    public Int32Type(boolean isNullable) {
        this(isNullable, "Xlake 32-bit integer");
    }

    public Int32Type() {
        this(true, "Xlake 32-bit integer");
    }

    public String name() {
        return "int32";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new Int32Type(nullable, description);
    }
}

record Int64Type(boolean isNullable, String description) implements PrimitiveType {
    public Int64Type {
        description = description != null ? description : "Xlake 64-bit integer";
    }

    public Int64Type(boolean isNullable) {
        this(isNullable, "Xlake 64-bit integer");
    }

    public Int64Type() {
        this(true, "Xlake 64-bit integer");
    }

    public String name() {
        return "int64";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new Int64Type(nullable, description);
    }
}

record Float32Type(boolean isNullable, String description) implements PrimitiveType {
    public Float32Type {
        description = description != null ? description : "Xlake 32-bit float";
    }

    public Float32Type(boolean isNullable) {
        this(isNullable, "Xlake 32-bit float");
    }

    public Float32Type() {
        this(true, "Xlake 32-bit float");
    }

    public String name() {
        return "float32";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new Float32Type(nullable, description);
    }
}

record Float64Type(boolean isNullable, String description) implements PrimitiveType {
    public Float64Type {
        description = description != null ? description : "Xlake 64-bit float";
    }

    public Float64Type(boolean isNullable) {
        this(isNullable, "Xlake 64-bit float");
    }

    public Float64Type() {
        this(true, "Xlake 64-bit float");
    }

    public String name() {
        return "float64";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new Float64Type(nullable, description);
    }
}

record DecimalType(int precision, int scale, boolean isNullable, String description) implements PrimitiveType {
    public DecimalType {
        if (precision <= 0 || precision > 38) throw new IllegalArgumentException("Precision must be between 1 and 38");
        if (scale < 0 || scale > precision) throw new IllegalArgumentException("Scale must be between 0 and precision");
        description = description != null ? description : "Xlake decimal type with precision " + precision + ", scale " + scale;
    }

    public DecimalType(int precision, int scale) {
        this(precision, scale, true, null);
    }

    public String name() {
        return "decimal(" + precision + "," + scale + ")";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new DecimalType(precision, scale, nullable, description);
    }
}

record StringType(boolean isNullable, String description) implements PrimitiveType {
    public StringType {
        description = description != null ? description : "Xlake string type";
    }

    public StringType(boolean isNullable) {
        this(isNullable, "Xlake string type");
    }

    public StringType() {
        this(true, "Xlake string type");
    }

    public String name() {
        return "string";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new StringType(nullable, description);
    }
}

record BinaryType(boolean isNullable, String description) implements PrimitiveType {
    public BinaryType {
        description = description != null ? description : "Xlake binary data";
    }

    public BinaryType(boolean isNullable) {
        this(isNullable, "Xlake binary data");
    }

    public BinaryType() {
        this(true, "Xlake binary data");
    }

    public String name() {
        return "binary";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new BinaryType(nullable, description);
    }
}

record DateType(boolean isNullable, String description) implements PrimitiveType {
    public DateType {
        description = description != null ? description : "Xlake date type";
    }

    public DateType(boolean isNullable) {
        this(isNullable, "Xlake date type");
    }

    public DateType() {
        this(true, "Xlake date type");
    }

    public String name() {
        return "date";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new DateType(nullable, description);
    }
}

record TimestampType(boolean isNullable, String description) implements PrimitiveType {
    public TimestampType {
        description = description != null ? description : "Xlake timestamp type";
    }

    public TimestampType(boolean isNullable) {
        this(isNullable, "Xlake timestamp type");
    }

    public TimestampType() {
        this(true, "Xlake timestamp type");
    }

    public String name() {
        return "timestamp";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public XlakeType copyWithNullable(boolean nullable) {
        return new TimestampType(nullable, description);
    }
}
