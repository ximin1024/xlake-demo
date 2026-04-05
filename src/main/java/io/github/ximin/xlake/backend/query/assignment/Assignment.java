package io.github.ximin.xlake.backend.query.assignment;

import io.github.ximin.xlake.backend.query.Expression;

import java.util.Set;

public interface Assignment extends Expression {

    String targetColumn();


    Expression valueExpression();


    Set<String> getReferencedColumns();

    // 检查是否赋值给自身（例如：age = age + 1）
    boolean selfAssignment();
}