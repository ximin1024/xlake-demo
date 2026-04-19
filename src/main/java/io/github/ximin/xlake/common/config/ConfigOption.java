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
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class ConfigOption<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String key;
    private final T defaultValue;
    private final Class<T> type;
    // Validators are not serialized - they are re-created on the executor side
    private transient List<Predicate<T>> validators;

    private ConfigOption(String key, T defaultValue, Class<T> type, List<Predicate<T>> validators) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.type = type;
        this.validators = validators;
    }

    public String key() {
        return key;
    }

    public T defaultValue() {
        return defaultValue;
    }

    public Class<T> type() {
        return type;
    }

    public void validate(T value) {
        if (validators == null) {
            return; // Validators not available after deserialization
        }
        for (Predicate<T> validator : validators) {
            if (!validator.test(value)) {
                throw new IllegalArgumentException("Validation failed for option '" + key + "' with value: " + value);
            }
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Validators are transient and not serialized, set to empty list after deserialization
        // Validation has already been performed on the driver side
        this.validators = List.of();
    }

    public static <T> ConfigOptionBuilder<T> builder(String key, T defaultValue, Class<T> type) {
        return new ConfigOptionBuilder<>(key, defaultValue, type);
    }

    public static ConfigOption<String> string(String key, String defaultValue) {
        return builder(key, defaultValue, String.class).build();
    }

    public static ConfigOption<Integer> int_(String key, int defaultValue) {
        return builder(key, defaultValue, Integer.class).build();
    }

    public static ConfigOption<Long> long_(String key, long defaultValue) {
        return builder(key, defaultValue, Long.class).build();
    }

    public static ConfigOption<Boolean> bool(String key, boolean defaultValue) {
        return builder(key, defaultValue, Boolean.class).build();
    }

    public static ConfigOption<Double> double_(String key, double defaultValue) {
        return builder(key, defaultValue, Double.class).build();
    }

    public static ConfigOption<Duration> duration(String key, Duration defaultValue) {
        return builder(key, defaultValue, Duration.class).build();
    }

    public static class ConfigOptionBuilder<T> {
        private final String key;
        private final T defaultValue;
        private final Class<T> type;
        private final java.util.ArrayList<Predicate<T>> validators = new java.util.ArrayList<>();

        private ConfigOptionBuilder(String key, T defaultValue, Class<T> type) {
            this.key = key;
            this.defaultValue = defaultValue;
            this.type = type;
        }

        public ConfigOptionBuilder<T> validate(Predicate<T> validator) {
            validators.add(validator);
            return this;
        }

        public ConfigOptionBuilder<T> validateNonNull() {
            validators.add(Objects::nonNull);
            return this;
        }

        public ConfigOptionBuilder<T> validatePositive() {
            // Strictly positive: value must be > 0, not >= 0
            // Note: type safety check is performed at runtime
            validators.add(v -> v instanceof Number && ((Number) v).doubleValue() > 0);
            return this;
        }

        public ConfigOptionBuilder<T> validateRange(T min, T max, Comparator<T> comparator) {
            validators.add(v -> comparator.compare(min, v) <= 0 && comparator.compare(v, max) <= 0);
            return this;
        }

        /**
         * Type-safe range validation for Integer values.
         * Returns false if the value is not an Integer or is outside the range.
         */
        public ConfigOptionBuilder<T> validateRange(int min, int max) {
            validators.add(v -> {
                if (!(v instanceof Integer)) {
                    return false;
                }
                int val = (Integer) v;
                return val >= min && val <= max;
            });
            return this;
        }

        public ConfigOption<T> build() {
            return new ConfigOption<>(key, defaultValue, type, List.copyOf(validators));
        }

        private interface Comparator<T> {
            int compare(T a, T b);
        }
    }
}
