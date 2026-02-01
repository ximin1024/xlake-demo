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

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class IsNotNull implements Expression {

    private final Expression column;

    public IsNotNull(Expression column) {
        this.column = column;
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        Comparable value = evaluateExpression(column, row);
        return value != null;
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
        return ExpressionType.IS_NOT_NULL;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return column.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        Expression simplifiedColumn = column.simplify();

        // 如果列是字面量，可以直接计算
        if (simplifiedColumn instanceof Literal) {
            Comparable value = ((Literal) simplifiedColumn).getValue();
            return new Literal(value != null);
        }

        return new Not(new IsNull(simplifiedColumn));
    }

    @Override
    public boolean alwaysTrue() {
        return false;
    }

    @Override
    public boolean alwaysFalse() {
        return false;
    }

    public Expression getColumn() {
        return column;
    }

    @Override
    public Expression copy() {
        return new IsNotNull(column.copy());
    }

    @Override
    public String toString() {
        return column.toString() + " IS NOT NULL";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IsNotNull that = (IsNotNull) obj;
        return Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, getType());
    }
}
