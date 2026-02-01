package io.github.ximin.xlake.backend.query;

import java.util.Map;

public class EndWith extends BinaryComparison {
    public EndWith(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        Comparable leftVal = evaluateSide(left, row);
        Comparable rightVal = evaluateSide(right, row);

        if (leftVal == null || rightVal == null) {
            return false;
        }

        return leftVal.toString().endsWith(rightVal.toString());
    }

    @Override
    protected boolean compare(Comparable left, Comparable right) {
        return left.toString().endsWith(right.toString());
    }

    @Override
    protected Expression createSimplified(Expression left, Expression right) {
        return new EndWith(left, right);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LIKE;
    }

    @Override
    public String toString() {
        return left.toString() + " ENDS WITH " + right.toString();
    }
}
