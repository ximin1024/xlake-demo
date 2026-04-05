package io.github.ximin.xlake.backend.query.assignment;

import io.github.ximin.xlake.backend.query.Expression;

//示例：age = age + 1
public class Add extends ArithmeticBase {

    public Add(String targetColumn,
               Expression leftOperand,
               Expression rightOperand) {
        super(targetColumn, leftOperand, rightOperand, Operator.ADD);
    }

    @Override
    protected Expression createArithmeticExpression(Expression left, Expression right, Operator op) {
        return new Add(targetColumn, left, right);
    }

    @Override
    protected ArithmeticBase createSimplified(Expression left, Expression right) {
        return new Add(targetColumn, left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.ARITHMETIC_ADD;
    }
}
