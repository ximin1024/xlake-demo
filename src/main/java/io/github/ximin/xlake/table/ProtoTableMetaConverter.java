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
package io.github.ximin.xlake.table;

import com.google.protobuf.Timestamp;
import io.github.ximin.xlake.meta.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public final class ProtoTableMetaConverter {

    private ProtoTableMetaConverter() {
    }

    public static PbTableMetadata toPb(TableMeta meta) {
        PbTableMetadata.Builder builder = PbTableMetadata.newBuilder()
                .setIdentifier(PbTableIdentifier.newBuilder()
                        .setCatalog(meta.catalogName())
                        .setDatabase(meta.databaseName())
                        .setTable(meta.tableName())
                        .build())
                .setTableId(meta.tableId())
                .setTableType(toPbType(meta.tableType()))
                .setSchema(toPbSchema(meta.schema()));

        if (meta.primaryKey() != null) {
            builder.setPrimaryKey(PbPrimaryKeyConstraint.newBuilder()
                    .addAllFieldNames(meta.primaryKey().fields()));
        }

        if (meta.partitionSpec() != null) {
            builder.setPartitionSpec(toPbPartitionSpec(meta.partitionSpec()));
        }

        if (meta.hiddenColumnConfig() != null) {
            builder.setHiddenColumns(toPbHiddenColumnConfig(meta.hiddenColumnConfig()));
        }

        builder.putAllProperties(meta.properties());

        if (meta.location() != null && !meta.location().isEmpty()) {
            builder.setLocation(meta.location());
        }

        if (meta.createdAt() != null) {
            builder.setCreatedAt(toTimestamp(meta.createdAt()));
        }

        if (meta.updatedAt() != null) {
            builder.setUpdatedAt(toTimestamp(meta.updatedAt()));
        }

        return builder.build();
    }

    public static TableMeta fromPb(PbTableMetadata pb) {
        TableMeta.Builder builder = TableMeta.builder()
                .withTableId(pb.getTableId())
                .withCatalogName(pb.getIdentifier().getCatalog())
                .withDatabaseName(pb.getIdentifier().getDatabase())
                .withTableName(pb.getIdentifier().getTable())
                .withTableType(fromPbType(pb.getTableType()))
                .withSchema(fromPbSchema(pb.getSchema()));

        if (pb.hasPrimaryKey()) {
            builder.withPrimaryKey(new PrimaryKey(new ArrayList<>(pb.getPrimaryKey().getFieldNamesList())));
        }

        if (pb.hasPartitionSpec()) {
            builder.withPartitionSpec(fromPbPartitionSpec(pb.getPartitionSpec()));
        }

        if (pb.hasHiddenColumns()) {
            builder.withHiddenColumnConfig(fromPbHiddenColumnConfig(pb.getHiddenColumns()));
        }

        if (!pb.getPropertiesMap().isEmpty()) {
            builder.withProperties(new HashMap<>(pb.getPropertiesMap()));
        }

        String location = pb.getLocation();
        if (location != null && !location.isEmpty()) {
            builder.withLocation(location);
        }

        if (pb.hasCreatedAt()) {
            builder.withCreatedAt(fromTimestamp(pb.getCreatedAt()));
        }

        if (pb.hasUpdatedAt()) {
            builder.withUpdatedAt(fromTimestamp(pb.getUpdatedAt()));
        }

        return builder.build();
    }

    private static PbSchema toPbSchema(io.github.ximin.xlake.table.schema.Schema schema) {
        PbSchema.Builder builder = PbSchema.newBuilder()
                .setStructType(toPbStructType(schema.structType()))
                .setVersion(schema.version());

        if (!schema.properties().isEmpty()) {
            builder.putAllProperties(schema.properties());
        }

        for (io.github.ximin.xlake.table.schema.Constraint constraint : schema.constraints()) {
            if (constraint instanceof io.github.ximin.xlake.table.schema.PrimaryKeyConstraint pk) {
                builder.addConstraints(PbPrimaryKeyConstraint.newBuilder()
                        .addAllFieldNames(pk.fieldNames()));
            }
        }

        return builder.build();
    }

    private static io.github.ximin.xlake.table.schema.Schema fromPbSchema(PbSchema pb) {
        List<io.github.ximin.xlake.table.schema.Constraint> constraints = new ArrayList<>();
        for (PbPrimaryKeyConstraint pkc : pb.getConstraintsList()) {
            constraints.add(new io.github.ximin.xlake.table.schema.PrimaryKeyConstraint(new ArrayList<>(pkc.getFieldNamesList())));
        }

        return new io.github.ximin.xlake.table.schema.Schema(
                fromPbStructType(pb.getStructType()),
                new HashMap<>(pb.getPropertiesMap()),
                pb.getVersion(),
                constraints
        );
    }

    private static PbStructType toPbStructType(io.github.ximin.xlake.table.schema.StructType structType) {
        PbStructType.Builder builder = PbStructType.newBuilder();
        for (io.github.ximin.xlake.table.schema.StructField field : structType.fields()) {
            builder.addFields(PbStructField.newBuilder()
                    .setFieldName(field.name())
                    .setDataType(toPbDataType(field.dataType()))
                    .setComment(field.comment() != null ? field.comment() : ""));
        }
        return builder.build();
    }

    private static io.github.ximin.xlake.table.schema.StructType fromPbStructType(PbStructType pb) {
        List<io.github.ximin.xlake.table.schema.StructField> fields = new ArrayList<>();
        for (PbStructField f : pb.getFieldsList()) {
            String comment = f.hasComment() ? f.getComment() : null;
            fields.add(new io.github.ximin.xlake.table.schema.StructField(f.getFieldName(), fromPbDataType(f.getDataType()), comment));
        }
        return new io.github.ximin.xlake.table.schema.StructType(fields);
    }

    private static PbDataType toPbDataType(io.github.ximin.xlake.table.schema.XlakeType type) {
        PbDataType.Builder builder = PbDataType.newBuilder()
                .setIsNullable(type.isNullable());

        String typeName = type.name();
        if (typeName.startsWith("decimal")) {
            builder.setPrimitiveType(PrimitiveType.DECIMAL);
        } else {
            builder.setPrimitiveType(toPbPrimitiveType(typeName));
        }

        return builder.build();
    }

    private static io.github.ximin.xlake.table.schema.XlakeType fromPbDataType(PbDataType pb) {
        boolean nullable = pb.getIsNullable();

        return switch (pb.getPrimitiveType()) {
            case BOOLEAN -> io.github.ximin.xlake.table.schema.Types.bool(nullable);
            case INT8 -> io.github.ximin.xlake.table.schema.Types.int8(nullable);
            case INT32 -> io.github.ximin.xlake.table.schema.Types.int32(nullable);
            case INT64 -> io.github.ximin.xlake.table.schema.Types.int64(nullable);
            case FLOAT32 -> io.github.ximin.xlake.table.schema.Types.float32(nullable);
            case FLOAT64 -> io.github.ximin.xlake.table.schema.Types.float64(nullable);
            case DECIMAL -> io.github.ximin.xlake.table.schema.Types.decimal(pb.getPrecision(), pb.getScale(), nullable);
            case STRING -> io.github.ximin.xlake.table.schema.Types.string(nullable);
            case BINARY -> io.github.ximin.xlake.table.schema.Types.binary(nullable);
            case DATE -> io.github.ximin.xlake.table.schema.Types.date(nullable);
            case TIMESTAMP -> io.github.ximin.xlake.table.schema.Types.timestamp(nullable);
            default -> throw new IllegalArgumentException("Unknown primitive type: " + pb.getPrimitiveType());
        };
    }

    private static PrimitiveType toPbPrimitiveType(String typeName) {
        return switch (typeName) {
            case "boolean" -> PrimitiveType.BOOLEAN;
            case "int8" -> PrimitiveType.INT8;
            case "int32" -> PrimitiveType.INT32;
            case "int64" -> PrimitiveType.INT64;
            case "float32" -> PrimitiveType.FLOAT32;
            case "float64" -> PrimitiveType.FLOAT64;
            case "string" -> PrimitiveType.STRING;
            case "binary" -> PrimitiveType.BINARY;
            case "date" -> PrimitiveType.DATE;
            case "timestamp" -> PrimitiveType.TIMESTAMP;
            default -> throw new IllegalArgumentException("Unknown type: " + typeName);
        };
    }

    private static TableType toPbType(TableMeta.TableType type) {
        return switch (type) {
            case PRIMARY_KEY -> TableType.PRIMARY_KEY;
            case APPEND_ONLY -> TableType.APPEND_ONLY;
        };
    }

    private static TableMeta.TableType fromPbType(TableType type) {
        return switch (type) {
            case PRIMARY_KEY -> TableMeta.TableType.PRIMARY_KEY;
            case APPEND_ONLY -> TableMeta.TableType.APPEND_ONLY;
            default -> throw new IllegalArgumentException("Unknown table type: " + type);
        };
    }

    private static PbPartitionSpec toPbPartitionSpec(PartitionSpec spec) {
        PbPartitionSpec.Builder builder = PbPartitionSpec.newBuilder()
                .setPartitionFieldsCount(spec.numLevels());
        for (PartitionSpec.BucketSpec bucketSpec : spec.bucketSpecs()) {
            builder.addBucketSpecs(PbBucketSpec.newBuilder()
                    .setSourceColumn(bucketSpec.sourceColumn() != null ? bucketSpec.sourceColumn() : "")
                    .setTransform(toPbTransform(bucketSpec.transform()))
                    .setNumBuckets(bucketSpec.numBuckets())
                    .setHidden(bucketSpec.hidden()));
        }
        return builder.build();
    }

    private static PartitionSpec fromPbPartitionSpec(PbPartitionSpec pb) {
        PartitionSpec.Builder builder = PartitionSpec.builder();
        for (PbBucketSpec bs : pb.getBucketSpecsList()) {
            builder.withBucketSpec(new PartitionSpec.BucketSpec(
                    bs.getSourceColumn().isEmpty() ? null : bs.getSourceColumn(),
                    fromPbTransform(bs.getTransform()),
                    bs.getNumBuckets(),
                    bs.getHidden()
            ));
        }
        return builder.build();
    }

    private static BucketTransform toPbTransform(PartitionSpec.Transform transform) {
        return switch (transform) {
            case IDENTITY -> BucketTransform.IDENTITY;
            case HASH -> BucketTransform.HASH;
            case RANGE -> BucketTransform.RANGE;
            case VALUE -> BucketTransform.VALUE;
        };
    }

    private static PartitionSpec.Transform fromPbTransform(BucketTransform transform) {
        return switch (transform) {
            case IDENTITY -> PartitionSpec.Transform.IDENTITY;
            case HASH -> PartitionSpec.Transform.HASH;
            case RANGE -> PartitionSpec.Transform.RANGE;
            case VALUE -> PartitionSpec.Transform.VALUE;
            default -> throw new IllegalArgumentException("Unknown transform: " + transform);
        };
    }

    private static PbHiddenColumnConfig toPbHiddenColumnConfig(HiddenColumnConfig config) {
        PbHiddenColumnConfig.Builder builder = PbHiddenColumnConfig.newBuilder();
        for (HiddenColumnConfig.HiddenColumnDef def : config.columns()) {
            builder.addColumns(PbHiddenColumnDef.newBuilder()
                    .setType(toPbColumnType(def.type()))
                    .setColumnName(def.columnName())
                    .setVisible(def.visible()));
        }
        return builder.build();
    }

    private static HiddenColumnConfig fromPbHiddenColumnConfig(PbHiddenColumnConfig pb) {
        HiddenColumnConfig.Builder builder = HiddenColumnConfig.builder();
        for (PbHiddenColumnDef def : pb.getColumnsList()) {
            builder.withColumn(new HiddenColumnConfig.HiddenColumnDef(
                    fromPbColumnType(def.getType()),
                    def.getColumnName(),
                    null,
                    def.getVisible()
            ));
        }
        return builder.build();
    }

    private static HiddenColumnType toPbColumnType(HiddenColumnConfig.ColumnType type) {
        return switch (type) {
            case ROW_NUMBER -> HiddenColumnType.ROW_NUMBER;
            case HIDDEN_PRIMARY_KEY -> HiddenColumnType.HIDDEN_PRIMARY_KEY;
            case COMMIT_TIMESTAMP -> HiddenColumnType.COMMIT_TIMESTAMP;
            case OPERATION_TYPE -> HiddenColumnType.OPERATION_TYPE;
        };
    }

    private static HiddenColumnConfig.ColumnType fromPbColumnType(HiddenColumnType type) {
        return switch (type) {
            case ROW_NUMBER -> HiddenColumnConfig.ColumnType.ROW_NUMBER;
            case HIDDEN_PRIMARY_KEY -> HiddenColumnConfig.ColumnType.HIDDEN_PRIMARY_KEY;
            case COMMIT_TIMESTAMP -> HiddenColumnConfig.ColumnType.COMMIT_TIMESTAMP;
            case OPERATION_TYPE -> HiddenColumnConfig.ColumnType.OPERATION_TYPE;
            default -> throw new IllegalArgumentException("Unknown hidden column type: " + type);
        };
    }

    public static PbSnapshot toPbSnapshot(Snapshot snapshot) {
        PbSnapshot.Builder builder = PbSnapshot.newBuilder()
                .setSnapshotId(snapshot.snapshotId())
                .setOperation(snapshot.operation())
                .setTimestamp(snapshot.timestampMillis());
        if (snapshot.manifestList() != null) {
            builder.setManifestList(snapshot.manifestList());
        }
        if (snapshot.summaryOptional().isPresent()) {
            builder.setSummary(snapshot.summaryOptional().get());
        }
        if (snapshot.schema() != null) {
            builder.setSchema(toPbSchema(snapshot.schema()));
        }
        return builder.build();
    }

    public static Snapshot fromPbSnapshot(PbSnapshot pb) {
        String summary = pb.getSummary().isEmpty() ? null : pb.getSummary();
        var schema = pb.hasSchema() ? fromPbSchema(pb.getSchema()) : null;
        return new Snapshot(
                pb.getSnapshotId(),
                pb.getManifestList().isEmpty() ? null : pb.getManifestList(),
                pb.getTimestamp(),
                summary,
                pb.getOperation(),
                schema
        );
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private static Instant fromTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
