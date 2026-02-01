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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class In implements Expression {

    private final Expression column;
    private final Set<Comparable> values;

    public In(Expression column, Set<Comparable> values) {
        this.column = column;
        this.values = new HashSet<>(values);
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        if (!(column instanceof ColumnRef)) {
            return false;
        }

        String columnName = ((ColumnRef) column).getColumnName();
        if (!row.containsKey(columnName)) {
            return false;
        }

        Comparable rowValue = row.get(columnName);
        return rowValue != null && values.contains(rowValue);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.IN;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return column.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        if (values.isEmpty()) {
            return new Literal(false);
        }

        if (values.size() == 1) {
            return new Eq(column, new Literal(values.iterator().next()));
        }

        return this;
    }

    public Set<Comparable> getValues() {
        return Collections.unmodifiableSet(values);
    }

    @Override
    public Expression copy() {
        return new In(column.copy(), new HashSet<>(values));
    }

    @Override
    public String toString() {
        return column.toString() + " IN (" +
                values.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(", ")) +
                ")";
    }
}
