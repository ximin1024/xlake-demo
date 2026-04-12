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
package io.github.ximin.xlake.table.op;

import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.table.read.TableReader;
import io.github.ximin.xlake.table.XlakeTable;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

@Slf4j
public abstract class BaseRead implements Read {

    protected final XlakeTable table;

    protected final List<DataBlock> dataBlocks;

    protected final Expression pushedPredicate;

    protected final boolean primaryKeyLookup;

    protected final TableReader reader;

    protected BaseRead(XlakeTable table,
                       List<DataBlock> dataBlocks,
                       Expression pushedPredicate,
                       boolean primaryKeyLookup,
                       TableReader reader) {
        this.table = Objects.requireNonNull(table, "table cannot be null");
        this.dataBlocks = dataBlocks;
        this.pushedPredicate = pushedPredicate;
        this.primaryKeyLookup = primaryKeyLookup;
        this.reader = Objects.requireNonNull(reader, "reader cannot be null - Phase 1 requires Reader delegation");
    }

    public XlakeTable getTable() {
        return table;
    }

    public TableReader getReader() {
        return reader;
    }

    @Override
    public List<DataBlock> getDataBlocks() {
        return dataBlocks != null ? dataBlocks : List.of();
    }

    @Override
    public long estimatedSize() {
        return dataBlocks != null ?
                dataBlocks.stream().mapToLong(DataBlock::getSize).sum() : 0L;
    }

    @Override
    public Expression getPushedPredicate() {
        return pushedPredicate;
    }

    @Override
    public boolean isPrimaryKeyLookup() {
        return primaryKeyLookup;
    }

    protected void ensureOpen() {
        if (table.isClosed()) {
            throw new IllegalStateException(
                    "Table is closed: " + table.getTableIdentifier() +
                            " (operation: " + this.getClass().getSimpleName() + ")");
        }
        if (log.isDebugEnabled()) {
            log.debug("Table {} is open for operation: {}",
                    table.getTableIdentifier(), this.getClass().getSimpleName());
        }
    }
}