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
package io.github.ximin.xlake.common.config;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class XlakeConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Design note for Spark integration:
     *
     * When using XlakeConfig with Spark broadcast:
     * 1. Driver creates XlakeConfig and validates all values via with()
     * 2. Config is serialized and broadcast to executors
     * 3. On executors, ConfigOption validators are NOT available (transient)
     * 4. This is intentional: validation already happened on driver
     *
     * The serialized form only contains the values map - the ConfigOption
     * definitions (including validators) stay on the driver. When get()
     * is called on executor, no re-validation occurs since values are trusted.
     */
    private final Map<String, Object> values;

    private XlakeConfig(Map<String, Object> values) {
        this.values = new ConcurrentHashMap<>(values);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(ConfigOption<T> option) {
        Object value = values.getOrDefault(option.key(), option.defaultValue());
        // Type safety check - ensure the retrieved value matches expected type
        if (value != null && !option.type().isInstance(value)) {
            throw new IllegalStateException("Type mismatch for option: " + option.key());
        }
        return (T) value;
    }

    public <T> XlakeConfig with(ConfigOption<T> option, T value) {
        Objects.requireNonNull(value, "value cannot be null for option: " + option.key());
        option.validate(value);
        Map<String, Object> newValues = new ConcurrentHashMap<>(this.values);
        newValues.put(option.key(), value);
        return new XlakeConfig(newValues);
    }

    public <T> XlakeConfig withFallback(ConfigOption<T> option, T value) {
        if (values.containsKey(option.key())) {
            return this;
        }
        return with(option, value);
    }

    public static XlakeConfig empty() {
        return new XlakeConfig(Map.of());
    }

    public static XlakeConfig fromMap(Map<String, Object> map) {
        return new XlakeConfig(Map.copyOf(map));
    }
}
