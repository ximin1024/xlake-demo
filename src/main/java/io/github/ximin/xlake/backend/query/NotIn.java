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

import java.util.*;
import java.util.stream.Collectors;

public class NotIn implements Expression {

    private final Expression column;
    private final Set<Comparable> values;

    public NotIn(Expression column, Set<Comparable> values) {
        this.column = column;
        this.values = new HashSet<>(values);
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        Comparable columnValue = evaluateExpression(column, row);

        if (columnValue == null) {
            // NULL 不在任何集合中，返回 true
            return true;
        }

        return !values.contains(columnValue);
    }

    private Comparable evaluateExpression(Expression expr, Map<String, Comparable> row) {
        if (expr instanceof ColumnRef) {
            String columnName = ((ColumnRef) expr).getColumnName();
            return row.get(columnName);
        } else if (expr instanceof Literal) {
            return ((Literal) expr).getValue();
        }
        return null;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.NOT_IN;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return column.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        Expression simplifiedColumn = column.simplify();

        if (values.isEmpty()) {
            // NOT IN 空集合总是为真
            return new Literal(true);
        }

        if (values.size() == 1) {
            // NOT IN 单个值转换为 !=
            Comparable singleValue = values.iterator().next();
            return new NotEq(simplifiedColumn, new Literal(singleValue)
            );
        }

        return new NotIn(simplifiedColumn, values);
    }

    @Override
    public boolean alwaysTrue() {
        // 如果集合为空，总是为真
        return values.isEmpty();
    }

    @Override
    public boolean alwaysFalse() {
        // NOT IN 永远不会总是为假，除非列总是为 NULL（需要更多上下文信息）
        return false;
    }

    public Expression getColumn() {
        return column;
    }

    public Set<Comparable> getValues() {
        return Collections.unmodifiableSet(values);
    }

    @Override
    public Expression copy() {
        return new NotIn(column.copy(), new HashSet<>(values));
    }

    @Override
    public String toString() {
        String valuesStr = values.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        return column.toString() + " NOT IN (" + valuesStr + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NotIn that = (NotIn) obj;
        return Objects.equals(column, that.column) &&
                Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, values, getType());
    }
}
