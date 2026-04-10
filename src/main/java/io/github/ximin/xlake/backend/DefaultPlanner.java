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
package io.github.ximin.xlake.backend;

import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.table.Snapshot;
import io.github.ximin.xlake.table.op.Read;

import java.util.List;

public class DefaultPlanner implements ReadPlanner {
    private Snapshot snapshot;
    private Expression predicate;

    @Override
    public List<Read> plan(Context context) {
        if (context != null) {
            this.snapshot = context.snapshot();
            this.predicate = context.predicate();
        }
        return List.of();
    }
}
