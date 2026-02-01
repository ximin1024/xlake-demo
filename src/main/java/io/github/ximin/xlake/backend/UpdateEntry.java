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
import io.github.ximin.xlake.backend.query.ExpressionUtils;
import lombok.Getter;

import java.util.*;

public class UpdateEntry {
    @Getter
    private final Expression condition;
    private final Map<String, Comparable> updates;
    @Getter
    private final long timestamp;
    @Getter
    private final String entryId;

    public UpdateEntry(Expression condition, Map<String, Comparable> updates) {
        this.condition = condition;
        this.updates = new HashMap<>(updates);
        this.timestamp = System.currentTimeMillis();
        this.entryId = UUID.randomUUID().toString();
    }

    public boolean isPrimaryKeyUpdate(Set<String> primaryKeys) {
        return ExpressionUtils.isPrimaryKeyEquals(condition, primaryKeys);
    }

    public Map<String, Comparable> getPrimaryKeyValues(Set<String> primaryKeys) {
        return ExpressionUtils.extractPrimaryKeyValues(condition, primaryKeys);
    }

    public Map<String, Comparable> getUpdates() {
        return Collections.unmodifiableMap(updates);
    }

    @Override
    public String toString() {
        return String.format("UPDATE SET %s WHERE %s", updates, condition);
    }
}
