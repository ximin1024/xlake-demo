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

import java.util.List;

public abstract class BaseRead implements Read {

    protected final List<DataBlock> dataBlocks;
    protected final Expression pushedPredicate;
    protected final boolean primaryKeyLookup;

    protected BaseRead(List<DataBlock> dataBlocks, Expression pushedPredicate, boolean primaryKeyLookup) {
        this.dataBlocks = dataBlocks;
        this.pushedPredicate = pushedPredicate;
        this.primaryKeyLookup = primaryKeyLookup;
    }

    @Override
    public List<DataBlock> getDataBlocks() {
        return dataBlocks;
    }

    @Override
    public long estimatedSize() {
        return dataBlocks.stream()
                .mapToLong(DataBlock::getSize)
                .sum();
    }

    @Override
    public Expression getPushedPredicate() {
        return pushedPredicate;
    }

    @Override
    public boolean isPrimaryKeyLookup() {
        return primaryKeyLookup;
    }
}