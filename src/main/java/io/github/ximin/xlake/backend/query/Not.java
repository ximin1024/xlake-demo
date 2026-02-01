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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Not implements LogicalExpression {

    private final Expression child;

    public Not(Expression child) {
        this.child = child;
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        return !child.evaluate(row);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.NOT;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return child.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        Expression simplifiedChild = child.simplify();
        if (simplifiedChild instanceof Not) {
            return ((Not) simplifiedChild).child.simplify();
        } else if (simplifiedChild.alwaysTrue()) {
            return new Literal(false);
        } else if (simplifiedChild.alwaysFalse()) {
            return new Literal(true);
        } else {
            return new Not(simplifiedChild);
        }
    }

    @Override
    public Expression copy() {
        return new Not(child.copy());
    }

    @Override
    public String toString() {
        return "NOT " + child.toString();
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }
}
