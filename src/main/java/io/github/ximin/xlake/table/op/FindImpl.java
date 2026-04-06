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
import java.util.Map;

public final class FindImpl implements Find {

    private final Map<String, Comparable> primaryKeyValues;
    private final List<String> projection;
    private final long snapshotId;
    private final boolean caseSensitive;

    private FindImpl(Builder builder) {
        this.primaryKeyValues = builder.primaryKeyValues != null
                ? Map.copyOf(builder.primaryKeyValues) : Map.of();
        this.projection = builder.projection != null ? List.copyOf(builder.projection) : List.of();
        this.snapshotId = builder.snapshotId != null ? builder.snapshotId : -1L;
        this.caseSensitive = builder.caseSensitive;
    }

    @Override
    public Expression filter() {
        return null;
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
    public OpResult exec() {
        return OpResult.failure("Find operation not implemented yet");
    }

    @Override
    public String type() {
        return "";
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public List<DataBlock> getDataBlocks() {
        return List.of();
    }

    @Override
    public long estimatedSize() {
        return 0;
    }

    @Override
    public Expression getPushedPredicate() {
        return null;
    }

    @Override
    public boolean isPrimaryKeyLookup() {
        return false;
    }

    public static class Builder {
        private Map<String, Comparable> primaryKeyValues;
        private List<String> projection;
        private Long snapshotId;
        private boolean caseSensitive = true;

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
