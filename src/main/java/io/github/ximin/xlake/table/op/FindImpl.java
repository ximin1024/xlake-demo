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
import io.github.ximin.xlake.table.XlakeTable;

import java.util.List;
import java.util.Map;

public final class FindImpl implements Find {
    private static final OpType TYPE = OpType.FIND;

    private final XlakeTable table;
    private final List<DataBlock> dataBlocks;
    private final Expression pushedPredicate;
    private final Map<String, Comparable> primaryKeyValues;
    private final List<String> projection;
    private final long snapshotId;
    private final boolean caseSensitive;

    private FindImpl(Builder builder) {
        this.table = builder.table;
        this.dataBlocks = builder.dataBlocks != null ? List.copyOf(builder.dataBlocks) : List.of();
        this.pushedPredicate = builder.pushedPredicate;
        this.primaryKeyValues = builder.primaryKeyValues != null
                ? Map.copyOf(builder.primaryKeyValues) : Map.of();
        this.projection = builder.projection != null ? List.copyOf(builder.projection) : List.of();
        this.snapshotId = builder.snapshotId != null ? builder.snapshotId : -1L;
        this.caseSensitive = builder.caseSensitive;
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
        return 1L;
    }

    public Map<String, Comparable> primaryKeyValues() {
        return primaryKeyValues;
    }

    @Override
    public Find.Result exec() {
        return Find.Result.error("Find operation not implemented yet", null);
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    public static Builder builder() {
        return new Builder();
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

    public static class Builder implements ReadBuilder<FindImpl, Builder> {
        private XlakeTable table;
        private List<DataBlock> dataBlocks;
        private Expression pushedPredicate;
        private Map<String, Comparable> primaryKeyValues;
        private List<String> projection;
        private Long snapshotId;
        private boolean caseSensitive = true;

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

        public FindImpl build() {
            if (primaryKeyValues == null || primaryKeyValues.isEmpty()) {
                throw new IllegalArgumentException("Find requires primary key values");
            }
            return new FindImpl(this);
        }
    }
}
