package io.github.ximin.xlake.backend.query;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class IsNull implements Expression {

    private final Expression column;

    public IsNull(Expression column) {
        this.column = column;
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        Comparable value = evaluateExpression(column, row);
        return value == null;
    }

    private Comparable evaluateExpression(Expression expr, Map<String, Comparable> row) {
        if (expr instanceof ColumnRef) {
            String columnName = ((ColumnRef) expr).getColumnName();
            return row.get(columnName);
        } else if (expr instanceof Literal) {
            return ((Literal) expr).getValue();
        }
        return null;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.IS_NULL;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return column.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        Expression simplifiedColumn = column.simplify();

        // 如果列是字面量，可以直接计算
        if (simplifiedColumn instanceof Literal) {
            Comparable value = ((Literal) simplifiedColumn).getValue();
            return new Literal(value == null);
        }

        return new IsNull(simplifiedColumn);
    }

    @Override
    public boolean alwaysTrue() {
        // todo 检查列是否总是为 null，可结合column统计信息和schema判断
        return false;
    }

    @Override
    public boolean alwaysFalse() {
        // todo 检查列是否从不为 null，可结合column统计信息和schma判断
        return false;
    }

    public Expression getColumn() {
        return column;
    }

    @Override
    public Expression copy() {
        return new IsNull(column.copy());
    }

    @Override
    public String toString() {
        return column.toString() + " IS NULL";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IsNull that = (IsNull) obj;
        return Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, getType());
    }
}
