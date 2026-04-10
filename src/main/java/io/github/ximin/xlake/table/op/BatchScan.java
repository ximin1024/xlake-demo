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

public class BatchScan implements Scan {
    private final XlakeTable table;
    private final List<DataBlock> dataBlocks;
    private final Expression pushedPredicate;
    private final List<String> projections;

    public BatchScan(XlakeTable table, List<DataBlock> dataBlocks, Expression pushedPredicate, List<String> projections) {
        this.table = table;
        this.dataBlocks = dataBlocks;
        this.pushedPredicate = pushedPredicate;
        this.projections = projections;
    }

    @Override
    public List<DataBlock> plan() {
        return dataBlocks != null ? dataBlocks : List.of();
    }

    @Override
    public Expression getPushedPredicate() {
        return pushedPredicate;
    }

    @Override
    public long estimatedSize() {
        return dataBlocks != null ? dataBlocks.stream().mapToLong(DataBlock::getSize).sum() : 0;
    }

    @Override
    public Scan.Result exec() {
        return Scan.Result.error("BatchScan exec not implemented - use Reader to execute", null);
    }

    @Override
    public OpType type() {
        return OpType.BATCH_SCAN;
    }

    @Override
    public List<String> projections() {
        return projections != null ? projections : List.of();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements ReadBuilder<BatchScan, Builder> {
        private XlakeTable table;
        private List<DataBlock> dataBlocks;
        private Expression pushedPredicate;
        private List<String> projections;

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

        public Builder withProjections(List<String> projections) {
            this.projections = projections;
            return this;
        }

        public BatchScan build() {
            return new BatchScan(table, dataBlocks, pushedPredicate, projections);
        }
    }
}
