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
package io.github.ximin.xlake.backend.query;

import lombok.Getter;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Getter
public class ColumnRef implements Expression {

    private final String columnName;

    public ColumnRef(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        return row.containsKey(columnName);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.COLUMN_REF;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return Collections.singleton(columnName);
    }

    @Override
    public Expression simplify() {
        return this;
    }

    @Override
    public Expression copy() {
        return new ColumnRef(columnName);
    }

    @Override
    public String toString() {
        return columnName;
    }
}
