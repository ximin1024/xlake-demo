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

public class GtEq extends BinaryComparison {

    public GtEq(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        // 如果任一值为 null，返回 false
        if (left == null || right == null) {
            return false;
        }

        return left.compareTo(right) >= 0;
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new GtEq(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.GREATER_THAN_OR_EQUAL;
    }

    @Override
    public String toString() {
        return left.toString() + " >= " + right.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GtEq that = (GtEq) obj;
        return Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, getType());
    }

    @Override
    public Expression copy() {
        return new GtEq(left.copy(), right.copy());
    }
}
