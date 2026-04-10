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
package io.github.ximin.xlake.backend;

import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.backend.query.serializer.ExpressionSerializer;
import io.github.ximin.xlake.backend.query.serializer.ExpressionSerializerFactory;
import lombok.Getter;

import java.io.Serial;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class UpdateValue implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @Getter
    private final Object value;
    @Getter
    private final ValueType type;
    private final byte[] serializedValue;
    private static final ExpressionSerializer serializer;

    public enum ValueType {
        NULL, STRING, INTEGER, LONG, DOUBLE, BOOLEAN, BINARY, EXPRESSION
    }

    static {
        serializer = ExpressionSerializerFactory.createDefaultSerializer();
    }

    private UpdateValue(Object value, ValueType type, byte[] serializedValue) {
        this.value = value;
        this.type = type;
        this.serializedValue = serializedValue;
    }

    public static UpdateValue fromObject(Object obj) {
        switch (obj) {
            case null -> {
                return new UpdateValue(null, ValueType.NULL, new byte[0]);
            }
            case String s -> {
                return new UpdateValue(obj, ValueType.STRING, s.getBytes());
            }
            case Integer i -> {
                return new UpdateValue(obj, ValueType.INTEGER,
                        ByteBuffer.allocate(4).putInt(i).array());
            }
            case Long l -> {
                return new UpdateValue(obj, ValueType.LONG,
                        ByteBuffer.allocate(8).putLong(l).array());
            }
            case Double v -> {
                return new UpdateValue(obj, ValueType.DOUBLE,
                        ByteBuffer.allocate(8).putDouble(v).array());
            }
            case Boolean b -> {
                return new UpdateValue(obj, ValueType.BOOLEAN,
                        new byte[]{(byte) (b ? 1 : 0)});
            }
            case byte[] bytes -> {
                return new UpdateValue(obj, ValueType.BINARY, bytes);
            }
            case Expression expr -> {
                // 表达式需要特殊处理
                byte[] serialized = serializer.serialize(expr);
                return new UpdateValue(obj, ValueType.EXPRESSION, serialized);
            }
            default -> {
                // 默认转为字符串
                String str = obj.toString();
                return new UpdateValue(str, ValueType.STRING, str.getBytes());
            }
        }

    }

    public byte[] getSerializedValue() {
        return serializedValue != null ? serializedValue : new byte[0];
    }

    public boolean isNull() {
        return type == ValueType.NULL;
    }

    public boolean isExpression() {
        return type == ValueType.EXPRESSION;
    }

    // 类型安全获取方法
    public String getAsString() {
        if (type == ValueType.STRING) {
            return (String) value;
        }
        throw new ClassCastException("Value is not a string");
    }

    public Integer getAsInteger() {
        if (type == ValueType.INTEGER) {
            return (Integer) value;
        }
        throw new ClassCastException("Value is not an integer");
    }
}
