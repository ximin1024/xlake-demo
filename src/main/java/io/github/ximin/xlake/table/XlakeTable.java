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

import io.github.ximin.xlake.table.op.*;
import io.github.ximin.xlake.table.schema.Schema;

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface XlakeTable extends Closeable {
    TableMeta meta();

    default String name() {
        return meta().fullName();
    }

    default String getTableIdentifier() {
        return name();
    }

    default String uniqId() {
        return meta().catalogName() + "." + meta().databaseName() + "." + meta().tableName() + "." + meta().tableId();
    }

    default Schema schema() {
        return meta().schema();
    }

    default long tableId() {
        return meta().tableId();
    }

    default TableMeta.TableType tableType() {
        return meta().tableType();
    }

    default boolean isKVTable() {
        return meta().isPrimaryKeyTable();
    }

    default boolean isAppendOnlyTable() {
        return meta().isAppendOnly();
    }

    default PrimaryKey primaryKey() {
        return meta().primaryKey();
    }

    default PartitionSpec partitionSpec() {
        return meta().partitionSpec();
    }

    default HiddenColumnConfig hiddenColumnConfig() {
        return meta().hiddenColumnConfig();
    }

    DynamicTableInfo dynamicInfo();

    Snapshot currentSnapshot();

    default Optional<Snapshot> currentSnapshotOptional() {
        return Optional.ofNullable(currentSnapshot());
    }

    Snapshot snapshot(long snapshotId);

    List<Snapshot> snapshots();

    default <R extends OpResult> R op(Op<R> op) {
        return op.exec();
    }

    default <R extends OpResult, O extends Op<R>> R op(TableOpBuilder<R, O> builder) {
        return builder.build().exec();
    }

    default Read.Result read(Read read) {
        return read.exec();
    }

    default <R extends Read> Read.Result read(ReadBuilder<R, ?> builder) {
        return read(builder.build());
    }

    default Write.Result write(Write write) {
        return write.exec();
    }

    default <W extends Write> Write.Result write(WriteBuilder<W, ?> builder) {
        return write(builder.build());
    }

    void refresh();

    default OpResult refreshOp() {
        return null;
    }

    boolean isClosed();
}

