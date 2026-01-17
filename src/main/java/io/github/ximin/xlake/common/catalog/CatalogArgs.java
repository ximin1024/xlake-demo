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
package io.github.ximin.xlake.common.catalog;

import com.beust.jcommander.*;
import lombok.Getter;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

public class CatalogArgs {

    public static CatalogArgs fromMap(Map<String, String> configMap) {
        CatalogArgs args = new CatalogArgs();
        JCommander jc = JCommander.newBuilder()
                .addObject(args)
                .addConverterInstanceFactory(new MapBasedConverterFactory(configMap))
                .build();

        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            for (Field field : args.getClass().getDeclaredFields()) {
                Parameter param = field.getAnnotation(Parameter.class);
                if (param != null && Arrays.asList(param.names()).contains(key)) {
                    try {
                        field.setAccessible(true);
                        field.set(args, value);
                    } catch (IllegalAccessException e) {
                        throw new ParameterException("Failed to set field: " + field.getName(), e);
                    }
                }
            }
        }
        return args;
    }

    static class MapBasedConverterFactory implements IStringConverterInstanceFactory {
        private final Map<String, String> configMap;

        MapBasedConverterFactory(Map<String, String> configMap) {
            this.configMap = configMap;
        }

        @Override
        public IStringConverter<?> getConverterInstance(Parameter parameter, Class<?> forType, String optionName) {
            // 注意：optionName是当前正在解析的命令行选项（例如"--xlake.catalog.type"）
            // 我们可以根据optionName从Map中获取值，并返回一个直接返回该值的转换器。
            return new IStringConverter<Object>() {
                @Override
                public Object convert(String value) {
                    // 但是，这里我们忽略传入的value，因为我们要使用Map中的值
                    // 注意：这种方式需要谨慎，因为它改变了转换器的常规行为。
                    // 实际上，我们可能不需要这个转换器来做实际的转换，因为我们已经通过反射设置了值。
                    // 这里为了演示，我们返回Map中对应optionName的值。
                    // 但是，请注意，JCommander可能会为同一个字段调用多次转换器（对于每个names）
                    // 因此，我们可以这样实现：
                    return configMap.get(optionName);
                }
            };
        }
    }

    @Parameter(
            names = {"xlake.catalog.type"},
            description = "The type of xlake catalog",
            help = true

    )
    @Getter
    private String catalogType = "metastore";
}
