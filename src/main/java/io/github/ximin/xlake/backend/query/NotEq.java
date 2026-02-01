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

public class NotEq extends BinaryComparison {

    protected NotEq(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        // 处理 null 值
        if (left instanceof ColumnRef && right instanceof Literal) {
            String column = ((ColumnRef) left).getColumnName();
            Comparable value = ((Literal) right).getValue();

            if (!row.containsKey(column)) {
                return false;
            }

            Comparable rowValue = row.get(column);

            // 如果查询值为 null
            if (value == null) {
                return rowValue != null;
            }

            // 行值为 null，查询值不为 null，返回 true
            if (rowValue == null) {
                return true;
            }

            return !rowValue.equals(value);
        }

        Comparable leftVal = evaluateSide(left, row);
        Comparable rightVal = evaluateSide(right, row);

        if (leftVal == null && rightVal == null) {
            return false;
        }

        if (leftVal == null || rightVal == null) {
            return true;
        }

        return !leftVal.equals(rightVal);
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        if (left == null && right == null) {
            return false;
        }

        if (left == null || right == null) {
            return true;
        }

        return !left.equals(right);
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new NotEq(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.NOT_EQUAL;
    }

    @Override
    public String toString() {
        return left.toString() + " != " + right.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        NotEq that = (NotEq) obj;
        return Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, getType());
    }

    @Override
    public Expression copy() {
        return new NotEq(left.copy(), right.copy());
    }
}
