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

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Between implements Expression {

    private final Expression column;
    private final Expression lowerBound;
    private final Expression upperBound;
    private final boolean inclusive;

    public Between(Expression column, Expression lowerBound,
                   Expression upperBound, boolean inclusive) {
        this.column = column;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.inclusive = inclusive;
    }

    public Between(Expression column, Expression lowerBound, Expression upperBound) {
        this(column, lowerBound, upperBound, true);
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        Comparable columnValue = evaluateExpression(column, row);
        Comparable lowerValue = evaluateExpression(lowerBound, row);
        Comparable upperValue = evaluateExpression(upperBound, row);

        // 如果任何值为 null，返回 false
        if (columnValue == null || lowerValue == null || upperValue == null) {
            return false;
        }

        int lowerCompare = columnValue.compareTo(lowerValue);
        int upperCompare = columnValue.compareTo(upperValue);

        if (inclusive) {
            return lowerCompare >= 0 && upperCompare <= 0;
        } else {
            return lowerCompare > 0 && upperCompare < 0;
        }
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
        return ExpressionType.BETWEEN;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        columns.addAll(column.getReferencedColumns());
        columns.addAll(lowerBound.getReferencedColumns());
        columns.addAll(upperBound.getReferencedColumns());
        return columns;
    }

    @Override
    public Expression simplify() {
        Expression simplifiedColumn = column.simplify();
        Expression simplifiedLower = lowerBound.simplify();
        Expression simplifiedUpper = upperBound.simplify();

        // 如果上下界都是字面量，可以优化
        if (simplifiedLower instanceof Literal && simplifiedUpper instanceof Literal) {

            Comparable lower = ((Literal) simplifiedLower).getValue();
            Comparable upper = ((Literal) simplifiedUpper).getValue();

            // 如果下界大于上界，永远为假
            if (lower != null && upper != null && lower.compareTo(upper) > 0) {
                return new Literal(false);
            }
        }

        return new Between(simplifiedColumn, simplifiedLower, simplifiedUpper, inclusive);
    }

    @Override
    public boolean alwaysTrue() {
        return false;
    }

    @Override
    public boolean alwaysFalse() {
        // 检查下界是否大于上界
        if (lowerBound instanceof Literal && upperBound instanceof Literal) {

            Comparable lower = ((Literal) lowerBound).getValue();
            Comparable upper = ((Literal) upperBound).getValue();

            return lower != null && upper != null && lower.compareTo(upper) > 0;
        }
        return false;
    }

    public Expression getColumn() {
        return column;
    }

    public Expression getLowerBound() {
        return lowerBound;
    }

    public Expression getUpperBound() {
        return upperBound;
    }

    public boolean isInclusive() {
        return inclusive;
    }

    @Override
    public Expression copy() {
        return new Between(
                column.copy(),
                lowerBound.copy(),
                upperBound.copy(),
                inclusive
        );
    }

    @Override
    public String toString() {
        String operator = inclusive ? "BETWEEN" : "BETWEEN EXCLUSIVE";
        return column.toString() + " " + operator + " " +
                lowerBound.toString() + " AND " + upperBound.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Between that = (Between) obj;
        return inclusive == that.inclusive &&
                Objects.equals(column, that.column) &&
                Objects.equals(lowerBound, that.lowerBound) &&
                Objects.equals(upperBound, that.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, lowerBound, upperBound, inclusive, getType());
    }
}
