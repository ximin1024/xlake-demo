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
package io.github.ximin.xlake.backend.query.assignment;

import io.github.ximin.xlake.backend.query.ColumnRef;
import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.backend.query.Literal;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public record Direct(String targetColumn, Expression valueExpression) implements Assignment {

    public Direct(String targetColumn, Comparable value) {
        this(targetColumn, new Literal(value));
    }

    @Override
    public boolean evaluate(Map<String, Comparable> row) {
        // 赋值表达式本身不用于谓词求值
        return true;
    }

    @Override
    public boolean selfAssignment() {
        if (valueExpression instanceof ColumnRef) {
            String sourceColumn = ((ColumnRef) valueExpression).columnName();
            return targetColumn.equals(sourceColumn);
        }
        return false;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.DIRECT;
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

}
