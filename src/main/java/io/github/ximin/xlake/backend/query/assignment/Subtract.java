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
package io.github.ximin.xlake.backend.query.assignment;

import io.github.ximin.xlake.backend.query.Expression;

// stock = stock - quantity
public class Subtract extends ArithmeticBase {

    public Subtract(String targetColumn,
                    Expression leftOperand,
                    Expression rightOperand) {
        super(targetColumn, leftOperand, rightOperand, Operator.SUBTRACT);
    }

    @Override
    protected Expression createArithmeticExpression(Expression left, Expression right, Operator op) {
        return new Subtract(targetColumn, left, right);
    }

    @Override
    protected ArithmeticBase createSimplified(Expression left, Expression right) {
        return new Subtract(targetColumn, left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.ARITHMETIC_SUBTRACT;
    }
}
