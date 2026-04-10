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
package io.github.ximin.xlake.table.record;

import io.github.ximin.xlake.table.schema.XlakeType;
import io.github.ximin.xlake.table.schema.Schema;

/**
 * 数据库式记录抽象。
 *
 * <p>与传统执行引擎里的 tuple/row 不同，这里将记录分为：
 * header（身份、版本、可见性）
 * 和 body（字段值载荷）两部分，以便后续支持主键表、版本可见性、删除标记和 schema evolution。</p>
 */
public interface Record {

    /**
     * 记录主键。
     */
    Key key();

    /**
     * 记录头信息。
     */
    Header header();

    /**
     * 记录体。
     */
    Body body();

    /**
     * 记录主键抽象。
     */
    interface Key {

        /**
         * 主键的稳定二进制表达。
         */
        byte[] encoded();
    }

    /**
     * 记录头部，描述一条记录的身份与版本属性。
     */
    record Header(
            Key key,
            long schemaVersion,
            long sequenceNumber,
            long commitTimestamp,
            Kind kind,
            boolean visible
    ) {
    }

    /**
     * 记录体，描述字段值载荷。
     */
    interface Body {

        /**
         * 记录体对应的 Schema。
         */
        Schema schema();

        /**
         * 按字段 ID 读取值。
         */
        Value value(FieldId fieldId);

        /**
         * 按字段名读取值。
         */
        Value value(String fieldName);

        /**
         * 是否存在指定字段。
         */
        boolean has(FieldId fieldId);

        /**
         * 是否存在指定字段名。
         */
        boolean has(String fieldName);
    }

    /**
     * 字段稳定标识。
     *
     * <p>字段名可变、ordinal 可漂移，FieldId 用于作为长期稳定的逻辑字段身份。</p>
     */
    record FieldId(int id, String name) {
    }

    /**
     * 单元值抽象。
     */
    interface Value {

        /**
         * 值对应的数据类型。
         */
        XlakeType type();

        /**
         * 值是否为 null。
         */
        boolean isNull();

        /**
         * 原始值对象。
         */
        Object raw();
    }

    /**
     * 记录变更类型。
     */
    enum Kind {
        INSERT,
        UPDATE,
        DELETE
    }
}
