package io.github.ximin.xlake.backend.query;

import java.util.List;

public interface LogicalExpression extends Expression {
    List<Expression> children();
}
