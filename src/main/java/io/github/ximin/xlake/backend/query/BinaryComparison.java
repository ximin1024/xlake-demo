package io.github.ximin.xlake.backend.query;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class BinaryComparison implements BinaryExpression {
    protected final Expression left;
    protected final Expression right;

    protected BinaryComparison(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        columns.addAll(left.getReferencedColumns());
        columns.addAll(right.getReferencedColumns());
        return columns;
    }

    @Override
    public Expression simplify() {
        Expression simplifiedLeft = left.simplify();
        Expression simplifiedRight = right.simplify();

        // 如果两边都是字面量，直接计算
        if (simplifiedLeft instanceof Literal && simplifiedRight instanceof Literal) {
            Comparable leftVal = ((Literal) simplifiedLeft).getValue();
            Comparable rightVal = ((Literal) simplifiedRight).getValue();

            if (leftVal == null || rightVal == null) {
                return new Literal(false);
            }

            return new Literal(compare(leftVal, rightVal));
        }

        return createSimplified(simplifiedLeft, simplifiedRight);
    }

    protected abstract Expression createSimplified(Expression left, Expression right);

    protected abstract boolean compare(Comparable left, Comparable right);

    @Override
    public Expression getLeft() {
        return left;
    }

    @Override
    public Expression getRight() {
        return right;
    }

    protected Comparable evaluateSide(Expression expr, Map<String, Comparable> row) {
        if (expr instanceof ColumnRef) {
            String columnName = ((ColumnRef) expr).getColumnName();
            return row.get(columnName);
        } else if (expr instanceof Literal) {
            return ((Literal) expr).getValue();
        } else {
            // 对于复杂表达式，尝试求值，这里简化处理，实际实现需要更完整的求值逻辑
            throw new UnsupportedOperationException(
                    "Complex expressions not supported in this context");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BinaryComparison that = (BinaryComparison) obj;
        return Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, getClass());
    }

    @Override
    public Expression copy() {
        try {
            return getClass()
                    .getDeclaredConstructor(Expression.class, Expression.class)
                    .newInstance(left.copy(), right.copy());
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy expression", e);
        }
    }
}
