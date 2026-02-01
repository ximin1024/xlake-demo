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

public class LtEq extends BinaryComparison{

    protected LtEq(Expression left, Expression right) {
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

            // 如果行值或查询值为 null，返回 false
            if (rowValue == null || value == null) {
                return false;
            }

            return rowValue.compareTo(value) <= 0;
        }

        Comparable leftVal = evaluateSide(left, row);
        Comparable rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        return leftVal.compareTo(rightVal) <= 0;
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        if (left == null || right == null) {
            return false;
        }

        return left.compareTo(right) <= 0;
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new LtEq(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LESS_THAN_OR_EQUAL;
    }

    @Override
    public String toString() {
        return left.toString() + " <= " + right.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        LtEq that = (LtEq) obj;
        return Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, getType());
    }

    @Override
    public Expression copy() {
        return new LtEq(left.copy(), right.copy());
    }
}
