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

import io.github.ximin.xlake.table.schema.Types;
import io.github.ximin.xlake.table.schema.XlakeType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class HiddenColumnConfig implements Serializable {

    public enum ColumnType {
        ROW_NUMBER("_row_number"),
        HIDDEN_PRIMARY_KEY("_hidden_pk"),
        COMMIT_TIMESTAMP("_commit_ts"),
        OPERATION_TYPE("_op_type");

        private final String defaultName;

        ColumnType(String defaultName) {
            this.defaultName = defaultName;
        }

        public String defaultName() {
            return defaultName;
        }
    }

    public record HiddenColumnDef(
            ColumnType type,
            String columnName,
            XlakeType dataType,
            boolean visible
    ) implements Serializable {

        public static HiddenColumnDef rowNumber(XlakeType dataType) {
            return new HiddenColumnDef(ColumnType.ROW_NUMBER, ColumnType.ROW_NUMBER.defaultName(), dataType, false);
        }

        public static HiddenColumnDef hiddenPk(XlakeType dataType) {
            return new HiddenColumnDef(ColumnType.HIDDEN_PRIMARY_KEY, ColumnType.HIDDEN_PRIMARY_KEY.defaultName(), dataType, false);
        }

        public static HiddenColumnDef commitTimestamp(XlakeType dataType) {
            return new HiddenColumnDef(ColumnType.COMMIT_TIMESTAMP, ColumnType.COMMIT_TIMESTAMP.defaultName(), dataType, false);
        }

        public static HiddenColumnDef operationType(XlakeType dataType) {
            return new HiddenColumnDef(ColumnType.OPERATION_TYPE, ColumnType.OPERATION_TYPE.defaultName(), dataType, false);
        }
    }

    private final List<HiddenColumnDef> columns;

    private HiddenColumnConfig(Builder builder) {
        this.columns = Collections.unmodifiableList(new ArrayList<>(builder.columns));
    }

    public List<HiddenColumnDef> columns() {
        return columns;
    }

    public boolean hasRowNumber() {
        return columns.stream().anyMatch(c -> c.type() == ColumnType.ROW_NUMBER);
    }

    public boolean hasHiddenPk() {
        return columns.stream().anyMatch(c -> c.type() == ColumnType.HIDDEN_PRIMARY_KEY);
    }

    public Optional<HiddenColumnDef> findColumn(ColumnType type) {
        return columns.stream().filter(c -> c.type() == type).findFirst();
    }

    public List<String> visibleHiddenColumnNames() {
        return columns.stream()
                .filter(HiddenColumnDef::visible)
                .map(HiddenColumnDef::columnName)
                .toList();
    }

    public List<String> allHiddenColumnNames() {
        return columns.stream()
                .map(HiddenColumnDef::columnName)
                .toList();
    }

    public static HiddenColumnConfig defaultConfig(TableMeta.TableType tableType) {
        return builder()
                .withRowNumber()
                .withCommitTimestamp()
                .withOperationType()
                .withHiddenPk(tableType == TableMeta.TableType.APPEND_ONLY)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(HiddenColumnConfig config) {
        return new Builder(config);
    }

    @Override
    public String toString() {
        return "HiddenColumnConfig{columns=" + columns + "}";
    }

    public static class Builder {
        private final List<HiddenColumnDef> columns = new ArrayList<>();

        public Builder() {}

        public Builder(HiddenColumnConfig config) {
            this.columns.addAll(config.columns);
        }

        public Builder withColumn(HiddenColumnDef def) {
            columns.add(def);
            return this;
        }

        public Builder withRowNumber() {
            return withColumn(HiddenColumnDef.rowNumber(Types.int64()));
        }

        public Builder withHiddenPk(boolean include) {
            if (include) {
                return withColumn(HiddenColumnDef.hiddenPk(Types.int64()));
            }
            return this;
        }

        public Builder withCommitTimestamp() {
            return withColumn(HiddenColumnDef.commitTimestamp(Types.timestamp()));
        }

        public Builder withOperationType() {
            return withColumn(HiddenColumnDef.operationType(Types.string()));
        }

        public HiddenColumnConfig build() {
            return new HiddenColumnConfig(this);
        }
    }
}
