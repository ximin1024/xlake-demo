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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface Expression extends Serializable {

    // todo use map temporarily，a custom row or record object needed
    boolean evaluate(Map<String, Comparable> row);

    ExpressionType getType();

    Set<String> getReferencedColumns();

    Expression simplify();

    default boolean alwaysTrue() {
        return false;
    }

    default boolean alwaysFalse() {
        return false;
    }

    Expression copy();

    enum ExpressionType implements Serializable {
        // basic
        LITERAL,
        COLUMN_REF,

        // comparison
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,

        // set
        IN,
        NOT_IN,

        // logical
        AND,
        OR,
        NOT,

        // range
        BETWEEN,

        // string
        LIKE,
        STARTS_WITH,
        ENDS_WITH,
        CONTAINS,

        // null
        IS_NULL,
        IS_NOT_NULL,

        // assignment
        // column = value
        DIRECT,
        // column = column + value
        ARITHMETIC,
        // CASE WHEN 赋值
        CASE,
        // column = function(...)
        FUNCTION,
        // if
        CONDITIONAL
    }
}
