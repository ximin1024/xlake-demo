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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public final class FindImpl extends BaseRead implements Find {
    private static final OpType TYPE = OpType.FIND;


    private final Map<String, Comparable> primaryKeyValues;


    private final List<String> projection;


    private final long snapshotId;


    private final boolean caseSensitive;


    private FindImpl(XlakeTable table,
                     List<DataBlock> dataBlocks,
                     Expression pushedPredicate,
                     Map<String, Comparable> primaryKeyValues,
                     List<String> projection,
                     long snapshotId,
                     boolean caseSensitive,
                     TableReader reader) {
        super(table, dataBlocks, pushedPredicate, true, reader);  // true = 主键查找
        this.primaryKeyValues = primaryKeyValues != null
                ? Map.copyOf(primaryKeyValues) : Map.of();
        this.projection = projection != null ? List.copyOf(projection) : List.of();
        this.snapshotId = snapshotId;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public Expression filter() {
        return pushedPredicate;
    }

    @Override
    public List<String> projection() {
        return projection;
    }

    @Override
    public long snapshotId() {
        return snapshotId;
    }

    @Override
    public boolean caseSensitive() {
        return caseSensitive;
    }

    @Override
    public long limit() {
        return 1L;  // Find操作最多返回1条记录
    }

    public Map<String, Comparable> primaryKeyValues() {
        return primaryKeyValues;
    }


    @Override
    public Find.Result exec() {
        // 1. 前置状态检查
        ensureOpen();

        try {
            // 2. ✅ Phase 1核心修复：委托给Reader执行点查
            log.debug("Executing Find operation for key: {} on table: {}",
                    primaryKeyValues, table.getTableIdentifier());

            Find.Result result = reader.find(this);

            // 3. 记录结果日志
            if (result.success()) {
                if (result.foundRecord().isPresent()) {
                    log.info("Find succeeded: record found for key: {} on table: {}",
                            primaryKeyValues, table.getTableIdentifier());
                } else {
                    log.debug("Find completed: record not found for key: {} on table: {}",
                            primaryKeyValues, table.getTableIdentifier());
                }
            } else {
                log.error("Find failed for key: {} on table: {}, error: {}",
                        primaryKeyValues, table.getTableIdentifier(),
                        result.message().orElse("Unknown error"));
            }

            return result;

        } catch (IOException e) {
            log.error("IO error during Find for key: {} on table: {}",
                    primaryKeyValues, table.getTableIdentifier(), e);
            return Find.Result.error("IO error during find: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error during Find for key: {} on table: {}",
                    primaryKeyValues, table.getTableIdentifier(), e);
            return Find.Result.error("Unexpected find error: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder implements ReadBuilder<FindImpl, Builder> {
        private XlakeTable table;
        private List<DataBlock> dataBlocks;
        private Expression pushedPredicate;
        private Map<String, Comparable> primaryKeyValues;
        private List<String> projection;
        private Long snapshotId;
        private boolean caseSensitive = true;
        private TableReader reader;  // Phase 1: 新增Reader注入

        @Override
        public Builder withTable(XlakeTable table) {
            this.table = table;
            return this;
        }

        @Override
        public Builder withDataBlocks(List<DataBlock> dataBlocks) {
            this.dataBlocks = dataBlocks;
            return this;
        }

        @Override
        public Builder withPredicate(Expression predicate) {
            this.pushedPredicate = predicate;
            return this;
        }

        @Override
        public XlakeTable table() {
            return table;
        }

        public Builder withPrimaryKey(Map<String, Comparable> pkValues) {
            this.primaryKeyValues = pkValues;
            return this;
        }

        public Builder withProjection(List<String> projection) {
            this.projection = projection;
            return this;
        }

        public Builder useSnapshot(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder caseSensitive(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
            return this;
        }


        public Builder withReader(TableReader reader) {
            this.reader = Objects.requireNonNull(reader, "reader cannot be null");
            return this;
        }

        public FindImpl build() {
            if (primaryKeyValues == null || primaryKeyValues.isEmpty()) {
                throw new IllegalArgumentException("Find requires primary key values");
            }
            Objects.requireNonNull(table, "table is required");
            Objects.requireNonNull(reader, "reader is required - use withReader() to inject TableReader");

            log.debug("Building FindImpl: table={}, key={}, hasReader={}",
                    table.getTableIdentifier(), primaryKeyValues, true);

            return new FindImpl(
                    table, dataBlocks, pushedPredicate,
                    primaryKeyValues, projection,
                    snapshotId != null ? snapshotId : -1L,
                    caseSensitive, reader
            );
        }
    }

    @Override
    public List<DataBlock> getDataBlocks() {
        return dataBlocks;
    }

    @Override
    public long estimatedSize() {
        return dataBlocks.stream().mapToLong(DataBlock::getSize).sum();
    }

    @Override
    public Expression getPushedPredicate() {
        return pushedPredicate;
    }

    @Override
    public boolean isPrimaryKeyLookup() {
        return true;
    }
}
