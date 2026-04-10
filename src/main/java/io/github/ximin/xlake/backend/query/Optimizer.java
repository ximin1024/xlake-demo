/*-
 * #%L
 * xlake-demo
 * %%
 * Copyright (C) 2026 ximin1024
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.github.ximin.xlake.backend.query;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Optimizer {
    public static Expression optimize(Expression expression) {
        if (expression == null) {
            return null;
        }
        return applyOptimizationRules(expression);
    }

    private static Expression applyOptimizationRules(Expression expr) {
        // 1. 常量折叠
        expr = constantFolding(expr);

        // 2. 简化表达式
        expr = expr.simplify();

        // 3. 去除冗余条件
        expr = removeRedundantConditions(expr);

        return expr;
    }

    private static Expression constantFolding(Expression expr) {
        if (expr instanceof BinaryComparison) {
            BinaryComparison binExpr = (BinaryComparison) expr;
            Expression left = binExpr.getLeft();
            Expression right = binExpr.getRight();

            // 如果两边都是字面量，直接计算
            if (left instanceof Literal && right instanceof Literal) {
                Comparable leftVal = ((Literal) left).getValue();
                Comparable rightVal = ((Literal) right).getValue();

                // 根据表达式类型计算结果
                boolean result;
                if (expr instanceof Eq) {
                    result = leftVal != null && leftVal.equals(rightVal);
                } else if (expr instanceof NotEq) {
                    result = leftVal != null && !leftVal.equals(rightVal);
                } else if (expr instanceof Gt) {
                    result = leftVal != null && rightVal != null && leftVal.compareTo(rightVal) > 0;
                } else if (expr instanceof GtEq) {
                    result = leftVal != null && rightVal != null && leftVal.compareTo(rightVal) >= 0;
                } else if (expr instanceof Lt) {
                    result = leftVal != null && rightVal != null && leftVal.compareTo(rightVal) < 0;
                } else if (expr instanceof LtEq) {
                    result = leftVal != null && rightVal != null && leftVal.compareTo(rightVal) <= 0;
                } else {
                    return expr;
                }

                return new Literal(result);
            }
        }

        // 递归处理子表达式
        if (expr instanceof And andExpr) {
            List<Expression> optimizedChildren = new ArrayList<>();
            for (Expression child : andExpr.children()) {
                Expression optimizedChild = constantFolding(child);
                optimizedChildren.add(optimizedChild);
            }
            return new And(optimizedChildren);
        }

        if (expr instanceof Or orExpr) {
            List<Expression> optimizedChildren = new ArrayList<>();
            for (Expression child : orExpr.children()) {
                Expression optimizedChild = constantFolding(child);
                optimizedChildren.add(optimizedChild);
            }
            return new Or(optimizedChildren);
        }

        if (expr instanceof Not notExpr) {
            Expression optimizedChild = constantFolding(notExpr.children().get(0));
            return new Not(optimizedChild);
        }

        return expr;
    }

    private static Expression removeRedundantConditions(Expression expr) {
        if (expr instanceof And andExpr) {
            List<Expression> children = andExpr.children();

            // 收集非重复条件
            Set<Expression> uniqueChildren = new LinkedHashSet<>();
            for (Expression child : children) {
                Expression optimizedChild = removeRedundantConditions(child);

                // 跳过总是为真的条件
                if (!optimizedChild.alwaysTrue()) {
                    uniqueChildren.add(optimizedChild);
                }
            }

            // 构建新的 AND 表达式
            if (uniqueChildren.isEmpty()) {
                return new Literal(true);
            } else if (uniqueChildren.size() == 1) {
                return uniqueChildren.iterator().next();
            } else {
                return new And(new ArrayList<>(uniqueChildren));
            }
        }

        if (expr instanceof Or orExpr) {
            List<Expression> children = orExpr.children();

            // 收集非重复条件
            Set<Expression> uniqueChildren = new LinkedHashSet<>();
            for (Expression child : children) {
                Expression optimizedChild = removeRedundantConditions(child);

                // 跳过总是为假的条件
                if (!optimizedChild.alwaysFalse()) {
                    uniqueChildren.add(optimizedChild);
                }
            }

            // 构建新的 OR 表达式
            if (uniqueChildren.isEmpty()) {
                return new Literal(false);
            } else if (uniqueChildren.size() == 1) {
                return uniqueChildren.iterator().next();
            } else {
                return new Or(new ArrayList<>(uniqueChildren));
            }
        }

        return expr;
    }
}
