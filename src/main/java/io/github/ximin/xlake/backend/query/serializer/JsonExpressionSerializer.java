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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.github.ximin.xlake.backend.query.Expression;
import io.jsonwebtoken.io.SerializationException;

import java.io.IOException;

public class JsonExpressionSerializer implements ExpressionSerializer {

    private final ObjectMapper objectMapper;

    public JsonExpressionSerializer() {
        this.objectMapper = createObjectMapper();
    }

    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // 配置多态序列化
        mapper.registerModule(createExpressionModule());

        // 配置序列化选项
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        return mapper;
    }

    private SimpleModule createExpressionModule() {
        SimpleModule module = new SimpleModule("ExpressionModule");

        // 注册序列化器和反序列化器（如果需要自定义序列化逻辑）
        // 这里使用默认的序列化，因为Expression及其子类已经是可序列化的POJO

        return module;
    }

    @Override
    public byte[] serialize(Expression expression) {
        try {
            return objectMapper.writeValueAsBytes(expression);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize expression to JSON", e);
        }
    }

    @Override
    public Expression deserialize(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Expression.class);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize expression from JSON", e);
        }
    }
}
