package io.github.ximin.xlake.backend.query.serializer;

import io.github.ximin.xlake.backend.query.Expression;

public interface ExpressionSerializer {
    byte[] serialize(Expression expression);
    Expression deserialize(byte[] bytes);
}
