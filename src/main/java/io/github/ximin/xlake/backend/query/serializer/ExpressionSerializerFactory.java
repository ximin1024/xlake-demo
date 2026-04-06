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
package io.github.ximin.xlake.backend.query.serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ExpressionSerializerFactory {

    public enum SerializerType {
        JSON,
        PROTOBUF
    }

    private static final Map<SerializerType, Supplier<ExpressionSerializer>> SERIALIZER_CREATORS = new HashMap<>();

    static {
        SERIALIZER_CREATORS.put(SerializerType.JSON, JsonExpressionSerializer::new);
        //SERIALIZER_CREATORS.put(SerializerType.PROTOBUF, ProtobufExpressionSerializer::new);
    }

    public static ExpressionSerializer createSerializer(SerializerType type) {
        Supplier<ExpressionSerializer> creator = SERIALIZER_CREATORS.get(type);
        if (creator == null) {
            throw new IllegalArgumentException("Unsupported serializer type: " + type);
        }
        return creator.get();
    }

    public static ExpressionSerializer createDefaultSerializer() {
        return createSerializer(SerializerType.JSON);
    }

    public static ExpressionSerializer createSerializerFromConfig(String config) {
        // 可以从配置文件中读取配置
        // 这里简化处理，根据字符串匹配
        try {
            SerializerType type = SerializerType.valueOf(config.toUpperCase());
            return createSerializer(type);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown serializer type in config: " + config, e);
        }
    }

    public static void registerSerializer(SerializerType type, Supplier<ExpressionSerializer> creator) {
        SERIALIZER_CREATORS.put(type, creator);
    }

    public static SerializerType[] getSupportedTypes() {
        return SERIALIZER_CREATORS.keySet().toArray(new SerializerType[0]);
    }
}
