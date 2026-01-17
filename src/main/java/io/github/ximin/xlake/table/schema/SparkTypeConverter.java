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

import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkTypeConverter implements TypeConverter<DataType> {
    @Override
    public XlakeType toXlakeType(DataType sparkType) {
        switch (sparkType) {
            case org.apache.spark.sql.types.BooleanType _ -> {
                return new BooleanType(true);
            }
            case ByteType _ -> {
                return new Int8Type(true, "Spark byte type");
            }
            case IntegerType _ -> {
                return new Int32Type(true, "Spark integer type");
            }
            case LongType _ -> {
                return new Int64Type(true, "Spark long type");
            }
            case FloatType _ -> {
                return new Float32Type(true, "Spark float type");
            }
            case DoubleType _ -> {
                return new Float64Type(true, "Spark double type");
            }
            case org.apache.spark.sql.types.DecimalType decimal -> {
                return new DecimalType(decimal.precision(), decimal.scale(), true, "Spark decimal type");
            }
            case org.apache.spark.sql.types.StringType _ -> {
                return new StringType(true, "Spark string type");
            }
            case org.apache.spark.sql.types.BinaryType _ -> {
                return new BinaryType(true, "Spark binary type");
            }
            case org.apache.spark.sql.types.DateType _ -> {
                return new DateType(true, "Spark date type");
            }
            case org.apache.spark.sql.types.TimestampType _ -> {
                return new TimestampType(true, "Spark timestamp type");
            }
            case org.apache.spark.sql.types.ArrayType array -> {
                XlakeType elementType = toXlakeType(array.elementType());
                return new ArrayType(elementType, true, "Spark array type");
            }
            case org.apache.spark.sql.types.MapType map -> {
                XlakeType keyType = toXlakeType(map.keyType());
                XlakeType valueType = toXlakeType(map.valueType());
                return new MapType(keyType, valueType, true, "Spark map type");
            }
            case org.apache.spark.sql.types.StructType struct -> {
                List<StructField> fields = Arrays.stream(struct.fields())
                        .map(this::convertSparkField)
                        .collect(Collectors.toList());
                return new StructType(fields, true, "Spark struct type");
            }
            case null, default -> throw new IllegalArgumentException("Unsupported Spark type: " + sparkType);
        }
    }

    @Override
    public DataType fromXlakeType(XlakeType dataType) {
        return dataType.accept(new XlakeTypeToSparkTypeVisitor());
    }

    private StructField convertSparkField(org.apache.spark.sql.types.StructField sparkField) {
        XlakeType dataType = toXlakeType(sparkField.dataType());
        String comment = sparkField.getComment().isDefined() ? sparkField.getComment().get() : "";

        // 提取元数据
        Map<String, String> metadata = new HashMap<>();
        metadata.put("nullable", String.valueOf(sparkField.nullable()));
        if (!comment.isEmpty()) {
            metadata.put("comment", comment);
        }

        return new StructField(
                sparkField.name(),
                dataType,
                comment,
                metadata
        );
    }

    private static class XlakeTypeToSparkTypeVisitor implements TypeVisitor<org.apache.spark.sql.types.DataType> {

        @Override
        public DataType visit(BooleanType type) {
            return DataTypes.BooleanType;
        }

        @Override
        public DataType visit(Int8Type type) {
            return DataTypes.ByteType;
        }

        @Override
        public DataType visit(Int32Type type) {
            return DataTypes.IntegerType;
        }

        @Override
        public DataType visit(Int64Type type) {
            return DataTypes.LongType;
        }

        @Override
        public DataType visit(Float32Type type) {
            return DataTypes.FloatType;
        }

        @Override
        public DataType visit(Float64Type type) {
            return DataTypes.DoubleType;
        }

        @Override
        public DataType visit(DecimalType type) {
            return DataTypes.createDecimalType(type.precision(), type.scale());
        }

        @Override
        public DataType visit(StringType type) {
            return DataTypes.StringType;
        }

        @Override
        public DataType visit(BinaryType type) {
            return DataTypes.BinaryType;
        }

        @Override
        public DataType visit(DateType type) {
            return DataTypes.DateType;
        }

        @Override
        public DataType visit(TimestampType type) {
            return DataTypes.TimestampType;
        }

        @Override
        public DataType visit(ArrayType type) {
            DataType elementType = type.elementType().accept(this);
            return DataTypes.createArrayType(elementType, type.isNullable());
        }

        @Override
        public DataType visit(MapType type) {
            DataType keyType = type.keyType().accept(this);
            DataType valueType = type.valueType().accept(this);
            return DataTypes.createMapType(keyType, valueType, type.isNullable());
        }

        @Override
        public DataType visit(StructType type) {
            org.apache.spark.sql.types.StructField[] sparkFields = type.fields().stream()
                    .map(this::convertXlakeField)
                    .toArray(org.apache.spark.sql.types.StructField[]::new);
            return DataTypes.createStructType(sparkFields);
        }

        private org.apache.spark.sql.types.StructField convertXlakeField(StructField field) {
            DataType sparkType = field.dataType().accept(this);
            MetadataBuilder metadataBuilder = new org.apache.spark.sql.types.MetadataBuilder();

            if (!field.comment().isEmpty()) {
                metadataBuilder.putString("comment", field.comment());
            }

            field.metadata().forEach((key, value) -> {
                if (!key.equals("comment")) {
                    metadataBuilder.putString(key, value);
                }
            });

            return new org.apache.spark.sql.types.StructField(
                    field.name(),
                    sparkType,
                    field.dataType().isNullable(),
                    metadataBuilder.build()
            );
        }
    }
}
