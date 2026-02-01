package io.github.ximin.xlake.backend.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ExpressionUtils {

    // 判断表达式是否是主键等值条件
    public static boolean isPrimaryKeyEquals(Expression expression, Set<String> primaryKeys) {
        if (expression instanceof Eq eq) {
            if (eq.getLeft() instanceof ColumnRef && eq.getRight() instanceof Literal) {
                String column = ((ColumnRef) eq.getLeft()).getColumnName();
                return primaryKeys.contains(column);
            }
        }

        if (expression instanceof And and) {
            for (Expression child : and.children()) {
                if (!isPrimaryKeyEquals(child, primaryKeys)) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    // 判断表达式是否只涉及主键
    public static boolean involvesOnlyPrimaryKeys(Expression expression, Set<String> primaryKeys) {
        Set<String> referencedColumns = expression.getReferencedColumns();
        return primaryKeys.containsAll(referencedColumns);
    }

    // 表达式与计算
    public static Expression merge(Expression expr1, Expression expr2) {
        if (expr1 == null) {
            return expr2;
        }
        if (expr2 == null) {
            return expr1;
        }

        return new And(expr1, expr2);
    }

    // 表达式
    public static boolean contains(Expression expr1, Expression expr2) {
        // todo 基于列引用和值范围的判断，实际实现需要更复杂的逻辑分析，如果是相同的列比较，可以直接判断
        if (expr1 instanceof Eq eq1 && expr2 instanceof Eq eq2) {
            if (eq1.getLeft().equals(eq2.getLeft())) {
                return eq1.getRight().equals(eq2.getRight());
            }
        }

        return false;
    }

    public static Map<String, Comparable> extractPrimaryKeyValues(Expression expression, Set<String> primaryKeys) {
        Map<String, Comparable> result = new HashMap<>();

        if (expression instanceof Eq) {
            extractPrimaryKeyValue((Eq) expression, primaryKeys, result);
        } else if (expression instanceof And and) {
            for (Expression child : and.children()) {
                if (child instanceof Eq) {
                    extractPrimaryKeyValue((Eq) child, primaryKeys, result);
                }
            }
        }

        return result;
    }

    private static void extractPrimaryKeyValue(Eq eq, Set<String> primaryKeys, Map<String, Comparable> result) {
        if (eq.getLeft() instanceof ColumnRef &&
                eq.getRight() instanceof Literal) {
            String column = ((ColumnRef) eq.getLeft()).getColumnName();
            Comparable value = ((Literal) eq.getRight()).getValue();

            if (primaryKeys.contains(column)) {
                result.put(column, value);
            }
        }
    }
}
