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
package io.github.ximin.xlake.storage.table;


import io.github.ximin.xlake.common.collection.cache.SortedCache;
import io.github.ximin.xlake.storage.block.DataBlock;

import java.util.ArrayList;
import java.util.List;

public class StoreState {

    private final SortedCache<Long, List<DataBlock>> state = new SortedCache<>(2000);

    public List<DataBlock> commitState(long commitId) {
        List<DataBlock> value = state.get(commitId);
        if (value == null) {
            return new ArrayList<>();
        }
        return List.copyOf(value);
    }
}
