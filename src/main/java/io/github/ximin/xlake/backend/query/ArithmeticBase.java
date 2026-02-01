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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class ArithmeticBase implements Assignment {
    protected final String targetColumn;
    protected final Expression leftOperand;
    protected final Expression rightOperand;
    protected final Operator operator;

    protected ArithmeticBase(String targetColumn,
                             Expression leftOperand,
                             Expression rightOperand,
                             Operator operator) {
        this.targetColumn = targetColumn;
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.operator = operator;
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        // 结构表示，不实现求值
        throw new UnsupportedOperationException("Arithmetic is for structure representation only");
    }

    @Override
    public String getTargetColumn() {
        return targetColumn;
    }

    @Override
    public Expression getValueExpression() {
        return createArithmeticExpression(leftOperand, rightOperand, operator);
    }

    protected abstract Expression createArithmeticExpression(Expression left, Expression right, Operator op);


    private Comparable performOperation(Comparable left, Comparable right, Operator op) {
        // todo 简化的算术运算实现,实际应用中需要更完善的类型检查和转换

        try {
            if (left instanceof Number && right instanceof Number) {
                double leftNum = ((Number) left).doubleValue();
                double rightNum = ((Number) right).doubleValue();

                switch (op) {
                    case ADD:
                        return leftNum + rightNum;
                    case SUBTRACT:
                        return leftNum - rightNum;
                    case MULTIPLY:
                        return leftNum * rightNum;
                    case DIVIDE:
                        if (rightNum == 0) return null;
                        return leftNum / rightNum;
                    case MODULO:
                        if (rightNum == 0) return null;
                        return leftNum % rightNum;
                }
            }
        } catch (Exception e) {
            // 处理运算异常
        }

        return null;
    }


    @Override
    public boolean selfAssignment() {
        if (leftOperand instanceof ColumnRef) {
            String leftColumn = ((ColumnRef) leftOperand).getColumnName();
            return targetColumn.equals(leftColumn);
        }
        return false;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.ARITHMETIC;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        columns.add(targetColumn);
        columns.addAll(leftOperand.getReferencedColumns());
        columns.addAll(rightOperand.getReferencedColumns());
        return columns;
    }

    @Override
    public Expression simplify() {
        Expression simplifiedLeft = leftOperand.simplify();
        Expression simplifiedRight = rightOperand.simplify();

        // 如果都是字面量，可以预计算
        if (simplifiedLeft instanceof Literal &&
                simplifiedRight instanceof Literal) {

            Comparable leftVal = ((Literal) simplifiedLeft).getValue();
            Comparable rightVal = ((Literal) simplifiedRight).getValue();

            if (leftVal != null && rightVal != null) {
                Comparable result = performOperation(leftVal, rightVal, operator);
                if (result != null) {
                    return new Direct(targetColumn, new Literal(result)
                    );
                }
            }
        }

        return createSimplified(simplifiedLeft, simplifiedRight);
    }

    protected abstract ArithmeticBase createSimplified(
            Expression left, Expression right);

    @Override
    public Expression copy() {
        try {
            return getClass()
                    .getDeclaredConstructor(String.class, Expression.class,
                            Expression.class, Operator.class)
                    .newInstance(targetColumn, leftOperand.copy(),
                            rightOperand.copy(), operator);
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy expression", e);
        }
    }

    @Override
    public String toString() {
        return targetColumn + " = " + leftOperand + " " + operator.getSymbol() + " " + rightOperand;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ArithmeticBase that = (ArithmeticBase) obj;
        return Objects.equals(targetColumn, that.targetColumn) &&
                Objects.equals(leftOperand, that.leftOperand) &&
                Objects.equals(rightOperand, that.rightOperand) &&
                operator == that.operator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetColumn, leftOperand, rightOperand, operator);
    }

    @Getter
    public enum Operator implements Serializable {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULO("%"),
        POWER("^");

        private final String symbol;

        Operator(String symbol) {
            this.symbol = symbol;
        }
    }
}
