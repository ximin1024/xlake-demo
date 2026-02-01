package io.github.ximin.xlake.backend.query;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Direct implements Assignment {

    private final String targetColumn;
    private final Expression valueExpression;

    public Direct(String targetColumn, Expression valueExpression) {
        this.targetColumn = targetColumn;
        this.valueExpression = valueExpression;
    }

    public Direct(String targetColumn, Comparable value) {
        this(targetColumn, new Literal(value));
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        // 赋值表达式本身不用于谓词求值
        return true;
    }

    @Override
    public String getTargetColumn() {
        return targetColumn;
    }

    @Override
    public Expression getValueExpression() {
        return valueExpression;
    }

    @Override
    public boolean selfAssignment() {
        if (valueExpression instanceof ColumnRef) {
            String sourceColumn = ((ColumnRef) valueExpression).getColumnName();
            return targetColumn.equals(sourceColumn);
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
        columns.addAll(valueExpression.getReferencedColumns());
        return columns;
    }

    @Override
    public Expression simplify() {
        Expression simplifiedValue = valueExpression.simplify();
        if (simplifiedValue.equals(valueExpression)) {
            return this;
        }
        return new Direct(targetColumn, simplifiedValue);
    }

    @Override
    public Expression copy() {
        return new Direct(targetColumn, valueExpression.copy());
    }

    @Override
    public String toString() {
        return targetColumn + " = " + valueExpression;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Direct that = (Direct) obj;
        return Objects.equals(targetColumn, that.targetColumn) && Objects.equals(valueExpression, that.valueExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetColumn, valueExpression);
    }
}
