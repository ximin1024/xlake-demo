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
package io.github.ximin.xlake.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unchecked")
public class SingletonContainer {
    private static final Map<String, Object> INSTANCES = new ConcurrentHashMap<>();

    private SingletonContainer() {
    }

    private static String generateKey(Class<?> clazz, Object... params) {
        if (params == null || params.length == 0) {
            return clazz.getName();
        }
        int paramsHash = Arrays.deepHashCode(params);
        return STR."\{clazz.getName()}@\{Integer.toHexString(paramsHash)}";
    }

    public static <T> T getInstance(Class<T> clazz, Object... constructorArgs) {
        String key = generateKey(clazz, constructorArgs);
        return (T) INSTANCES.computeIfAbsent(key, _ -> {
            try {
                if (constructorArgs == null || constructorArgs.length == 0) {
                    Constructor<T> constructor = clazz.getDeclaredConstructor();
                    constructor.setAccessible(true);
                    return constructor.newInstance();
                } else {
                    Class<?>[] paramTypes = Arrays.stream(constructorArgs)
                            .map(Object::getClass)
                            .toArray(Class<?>[]::new);

                    Constructor<T> constructor = clazz.getDeclaredConstructor(paramTypes);
                    constructor.setAccessible(true);
                    return constructor.newInstance(constructorArgs);
                }
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(
                        STR."class \{clazz.getName()} do not have matching constructor，args: \{Arrays.toString(constructorArgs)}", e);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(STR."create \{clazz.getName()} singleton fail.", e);
            }
        });
    }

    public static <T> T destroyInstance(Class<T> clazz, Object... constructorArgs) {
        String key = generateKey(clazz, constructorArgs);
        Object instance = INSTANCES.remove(key);

        if (instance instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                System.err.println(STR."close singleton fail: \{e.getMessage()}");
            }
        }

        return (T) instance;
    }
}
