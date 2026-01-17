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
package io.github.ximin.xlake.table.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class TypeConverterRegistry {
    private final Map<Class<?>, TypeConverter<?>> converters = new HashMap<>();

    public TypeConverterRegistry() {
        registerBuiltinConverters();
        loadExtensionConverters();
    }

    private void registerBuiltinConverters() {
        registerConverter(Class.class, new JavaTypeConverter());
        try {
            Class.forName("org.apache.spark.sql.types.DataType");
            registerConverter(org.apache.spark.sql.types.DataType.class, new SparkTypeConverter());
            System.out.println("Spark type converter registered successfully");
        } catch (ClassNotFoundException e) {
            System.out.println("Spark not found in classpath, skipping Spark type converter");
        }
    }

    private void loadExtensionConverters() {
        ServiceLoader<TypeConverter> loader = ServiceLoader.load(TypeConverter.class);
        for (TypeConverter converter : loader) {
            Class<?> targetType = extractTargetType(converter);
            if (targetType != null) {
                registerConverter(targetType, converter);
                System.out.println("Registered extension converter for: " + targetType.getName());
            }
        }
    }

    private Class<?> extractTargetType(TypeConverter converter) {
        try {
            // 通过反射获取泛型参数类型
            java.lang.reflect.Type[] interfaces = converter.getClass().getGenericInterfaces();
            for (java.lang.reflect.Type iface : interfaces) {
                if (iface instanceof java.lang.reflect.ParameterizedType) {
                    java.lang.reflect.ParameterizedType pType = (java.lang.reflect.ParameterizedType) iface;
                    if (pType.getRawType().equals(TypeConverter.class)) {
                        java.lang.reflect.Type[] typeArgs = pType.getActualTypeArguments();
                        if (typeArgs.length > 0 && typeArgs[0] instanceof Class) {
                            return (Class<?>) typeArgs[0];
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to extract target type from converter: " + converter.getClass().getName());
        }
        return null;
    }

    public <T> void registerConverter(Class<?> targetType, TypeConverter<T> converter) {
        converters.put(targetType, converter);
    }

    @SuppressWarnings("unchecked")
    public <T> TypeConverter<T> getConverter(Class<T> targetType) {
        TypeConverter<?> converter = converters.get(targetType);
        if (converter == null) {
            throw new IllegalArgumentException("No converter registered for: " + targetType.getName());
        }
        return (TypeConverter<T>) converter;
    }

    public <T> XlakeType toXlakeType(T externalType, Class<T> targetType) {
        return getConverter(targetType).toXlakeType(externalType);
    }

    public <T> T fromXlakeType(XlakeType dataType, Class<T> targetType) {
        return getConverter(targetType).fromXlakeType(dataType);
    }
}
