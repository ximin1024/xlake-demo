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
import io.github.ximin.xlake.storage.DataBlock;

import java.util.List;

public class KvScan implements Scan {

    private final List<DataBlock> dataBlocks;
    private final Expression pushedPredicate;
    private final long snapshotId;

    public KvScan(List<DataBlock> dataBlocks, Expression pushedPredicate, long snapshotId) {
        this.dataBlocks = dataBlocks;
        this.pushedPredicate = pushedPredicate;
        this.snapshotId = snapshotId;
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
    public OpResult exec() {
        return OpResult.failure("KvScan exec not implemented - use Reader to execute");
    }

    @Override
    public String type() {
        return "KV_SCAN";
    }

    public long snapshotId() {
        return snapshotId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<DataBlock> dataBlocks;
        private Expression pushedPredicate;
        private long snapshotId = -1;

        public Builder withDataBlocks(List<DataBlock> dataBlocks) {
            this.dataBlocks = dataBlocks;
            return this;
        }

        public Builder withPredicate(Expression predicate) {
            this.pushedPredicate = predicate;
            return this;
        }

        public Builder withSnapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public KvScan build() {
            return new KvScan(dataBlocks, pushedPredicate, snapshotId);
        }
    }
}
