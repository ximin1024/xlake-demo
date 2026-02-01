package io.github.ximin.xlake.backend.query;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class Literal implements Expression {

    private final Comparable value;
    private final Class<?> valueType;

    public Literal(Comparable value) {
        this.value = value;
        this.valueType = value != null ? value.getClass() : null;
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        // 字面量总是为真
        return true;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LITERAL;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return Collections.emptySet();
    }

    @Override
    public Expression simplify() {
        return this;
    }

    public Comparable getValue() {
        return value;
    }

    public Class<?> getValueType() {
        return valueType;
    }

    @Override
    public Expression copy() {
        return new Literal(value);
    }

    @Override
    public String toString() {
        return value != null ? value.toString() : "NULL";
    }
}
