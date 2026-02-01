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

public class Gt extends BinaryComparison {

    public Gt(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        Comparable leftVal = evaluateSide(left, row);
        Comparable rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        return leftVal.compareTo(rightVal) > 0;
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        return left != null && right != null && left.compareTo(right) > 0;
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new Gt(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.GREATER_THAN;
    }

    @Override
    public String toString() {
        return left.toString() + " > " + right.toString();
    }
}
