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

import java.util.List;
import java.util.Map;

public class SparkSchemaConverter {
    private static final SparkTypeConverter TYPE_CONVERTER = new SparkTypeConverter();

    public static Schema toXlakeSchema(org.apache.spark.sql.types.StructType sparkSchema) {
        StructType structType = (StructType) TYPE_CONVERTER.toXlakeType(sparkSchema);

        return new Schema(
                structType,
                extractSchemaProperties(sparkSchema),
                1L
        );
    }

    public static org.apache.spark.sql.types.StructType toSparkSchema(Schema dataLakeSchema) {
        return (org.apache.spark.sql.types.StructType) TYPE_CONVERTER.fromXlakeType(dataLakeSchema.structType());
    }

    private static Map<String, String> extractSchemaProperties(org.apache.spark.sql.types.StructType sparkSchema) {
        Map<String, String> properties = new java.util.HashMap<>();

        properties.put("field_count", String.valueOf(sparkSchema.fields().length));
        properties.put("source", "spark");

        return properties;
    }

    public static org.apache.spark.sql.types.StructField createSparkField(String name, XlakeType dataType,
                                                                          boolean nullable, String comment) {
        org.apache.spark.sql.types.DataType sparkType = TYPE_CONVERTER.fromXlakeType(dataType);

        org.apache.spark.sql.types.MetadataBuilder metadataBuilder = new org.apache.spark.sql.types.MetadataBuilder();
        if (comment != null && !comment.isEmpty()) {
            metadataBuilder.putString("comment", comment);
        }

        return new org.apache.spark.sql.types.StructField(
                name,
                sparkType,
                nullable,
                metadataBuilder.build()
        );
    }

    public static org.apache.spark.sql.types.StructField[] toSparkFields(List<StructField> dataLakeFields) {
        return dataLakeFields.stream()
                .map(field -> createSparkField(
                        field.name(),
                        field.dataType(),
                        field.dataType().isNullable(),
                        field.comment()
                ))
                .toArray(org.apache.spark.sql.types.StructField[]::new);
    }
}
