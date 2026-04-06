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

import io.github.ximin.xlake.table.schema.Schema;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public class TableMeta implements Serializable {
    private final long tableId;
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final TableType tableType;
    private final Schema schema;
    private final PrimaryKey primaryKey;
    private final PartitionSpec partitionSpec;
    private final HiddenColumnConfig hiddenColumnConfig;
    private final Map<String, String> properties;
    private final String location;
    private final Instant createdAt;
    private final Instant updatedAt;

    public enum TableType {
        PRIMARY_KEY,
        APPEND_ONLY
    }

    private TableMeta(Builder builder) {
        this.tableId = builder.tableId;
        this.catalogName = builder.catalogName != null ? builder.catalogName : "default";
        this.databaseName = builder.databaseName;
        this.tableName = builder.tableName;
        this.tableType = builder.tableType;
        this.schema = builder.schema;
        this.primaryKey = builder.primaryKey;
        this.partitionSpec = builder.partitionSpec != null ? builder.partitionSpec : PartitionSpec.unpartitioned();
        this.hiddenColumnConfig = builder.hiddenColumnConfig != null ? builder.hiddenColumnConfig : HiddenColumnConfig.defaultConfig(builder.tableType);
        this.properties = builder.properties != null ? Map.copyOf(builder.properties) : Map.of();
        this.location = builder.location;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
    }

    public long tableId() {
        return tableId;
    }

    public String catalogName() {
        return catalogName;
    }

    public String databaseName() {
        return databaseName;
    }

    public String tableName() {
        return tableName;
    }

    public String fullName() {
        return catalogName + "." + databaseName + "." + tableName;
    }

    public TableType tableType() {
        return tableType;
    }

    public Schema schema() {
        return schema;
    }

    public PrimaryKey primaryKey() {
        return primaryKey;
    }

    public boolean hasPrimaryKey() {
        return primaryKey != null && !primaryKey.fields().isEmpty();
    }

    public PartitionSpec partitionSpec() {
        return partitionSpec;
    }

    public boolean isPartitioned() {
        return partitionSpec.isPartitioned();
    }

    public int numBuckets() {
        return partitionSpec.numBuckets();
    }

    public HiddenColumnConfig hiddenColumnConfig() {
        return hiddenColumnConfig;
    }

    public Map<String, String> properties() {
        return properties;
    }

    public String location() {
        return location;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    public boolean isAppendOnly() {
        return tableType == TableType.APPEND_ONLY;
    }

    public boolean isPrimaryKeyTable() {
        return tableType == TableType.PRIMARY_KEY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableMeta that)) return false;
        return tableId == that.tableId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(tableId);
    }

    @Override
    public String toString() {
        return "TableMeta{" +
                "tableId=" + tableId +
                ", name='" + fullName() + '\'' +
                ", type=" + tableType +
                ", location='" + location + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(TableMeta meta) {
        return new Builder(meta);
    }

    public static class Builder {
        private long tableId;
        private String catalogName = "default";
        private String databaseName;
        private String tableName;
        private TableType tableType;
        private Schema schema;
        private PrimaryKey primaryKey;
        private PartitionSpec partitionSpec;
        private HiddenColumnConfig hiddenColumnConfig;
        private Map<String, String> properties;
        private String location;
        private Instant createdAt;
        private Instant updatedAt;

        public Builder() {}

        public Builder(TableMeta meta) {
            this.tableId = meta.tableId;
            this.catalogName = meta.catalogName;
            this.databaseName = meta.databaseName;
            this.tableName = meta.tableName;
            this.tableType = meta.tableType;
            this.schema = meta.schema;
            this.primaryKey = meta.primaryKey;
            this.partitionSpec = meta.partitionSpec;
            this.hiddenColumnConfig = meta.hiddenColumnConfig;
            this.properties = meta.properties;
            this.location = meta.location;
            this.createdAt = meta.createdAt;
            this.updatedAt = meta.updatedAt;
        }

        public Builder withTableId(long tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder withCatalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withTableType(TableType tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder withSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder withPrimaryKey(PrimaryKey primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder withPartitionSpec(PartitionSpec partitionSpec) {
            this.partitionSpec = partitionSpec;
            return this;
        }

        public Builder withHiddenColumnConfig(HiddenColumnConfig hiddenColumnConfig) {
            this.hiddenColumnConfig = hiddenColumnConfig;
            return this;
        }

        public Builder withProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder withLocation(String location) {
            this.location = location;
            return this;
        }

        public Builder withCreatedAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder withUpdatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public TableMeta build() {
            Objects.requireNonNull(tableName, "tableName is required");
            Objects.requireNonNull(tableType, "tableType is required");
            Objects.requireNonNull(schema, "schema is required");
            if (updatedAt == null) {
                updatedAt = Instant.now();
            }
            if (createdAt == null) {
                createdAt = updatedAt;
            }
            return new TableMeta(this);
        }
    }
}
