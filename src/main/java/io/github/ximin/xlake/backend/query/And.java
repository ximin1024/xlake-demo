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

import java.util.*;
import java.util.stream.Collectors;

public class And implements LogicalExpression {

    private final List<Expression> children;

    public And(Expression... children) {
        this.children = Arrays.asList(children);
    }

    public And(List<Expression> children) {
        this.children = new ArrayList<>(children);
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        for (Expression child : children) {
            if (!child.evaluate(row)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.AND;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        for (Expression child : children) {
            columns.addAll(child.getReferencedColumns());
        }
        return columns;
    }

    @Override
    public Expression simplify() {
        List<Expression> simplified = new ArrayList<>();

        for (Expression child : children) {
            Expression simplifiedChild = child.simplify();

            if (simplifiedChild.alwaysFalse()) {
                return new Literal(false);
            }

            if (!simplifiedChild.alwaysTrue()) {
                simplified.add(simplifiedChild);
            }
        }

        if (simplified.isEmpty()) {
            return new Literal(true);
        } else if (simplified.size() == 1) {
            return simplified.get(0);
        } else {
            return new And(simplified);
        }
    }

    @Override
    public boolean alwaysTrue() {
        return children.stream().allMatch(Expression::alwaysTrue);
    }

    @Override
    public boolean alwaysFalse() {
        return children.stream().anyMatch(Expression::alwaysFalse);
    }

    @Override
    public Expression copy() {
        List<Expression> copiedChildren = children.stream()
                .map(Expression::copy)
                .collect(Collectors.toList());
        return new And(copiedChildren);
    }

    @Override
    public String toString() {
        return children.stream()
                .map(Expression::toString)
                .collect(Collectors.joining(" AND ", "(", ")"));
    }

    @Override
    public List<Expression> children() {
        return Collections.unmodifiableList(children);
    }
}
