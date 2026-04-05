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
        // 基础类型
        LITERAL,
        COLUMN_REF,

        // 比较操作符
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,

        // 集合操作
        IN,
        NOT_IN,

        // 逻辑操作
        AND,
        OR,
        NOT,

        // 范围操作
        BETWEEN,

        // 字符串操作
        LIKE,
        STARTS_WITH,
        ENDS_WITH,
        CONTAINS,

        // 空值操作
        IS_NULL,
        IS_NOT_NULL,

        // 赋值
        // 直接赋值：column = value
        DIRECT,
        // 算术赋值：column = column + value
        ARITHMETIC_ADD,
        ARITHMETIC_SUBTRACT,
        ARITHMETIC_MULTIPLY,
        ARITHMETIC_DIVIDE,
        // CASE WHEN 赋值
        /*
        SELECT
        CASE
            WHEN column2 > 100 THEN 20
            ELSE column1
        END AS column1,
        column2
        FROM table
        WHERE column2 > 50
         */
        CASE,
        // 函数赋值：column = function(...)
        FUNCTION,
        // 条件赋值
        CONDITIONAL
    }
}
