package io.github.ximin.xlake.backend.query;

import java.util.Map;

public class Contains extends BinaryComparison {

    public Contains(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        Comparable leftVal = evaluateSide(left, row);
        Comparable rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        return leftVal.toString().contains(rightVal.toString());
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        return left.toString().contains(right.toString());
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new Contains(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LIKE;
    }

    @Override
    public String toString() {
        return left.toString() + " CONTAINS " + right.toString();
    }
}
