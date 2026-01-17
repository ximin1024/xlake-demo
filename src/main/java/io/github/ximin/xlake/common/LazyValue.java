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

import java.util.function.Supplier;

public sealed interface LazyValue<T> permits LazyValue.Initial, LazyValue.Uninitialized, LazyValue.Value {

    T get();

    boolean isInitialized();

    record Uninitialized<T>(Supplier<T> supplier) implements LazyValue<T> {
        @Override
        public T get() {
            throw new IllegalStateException("LazyValue has not been initialized");
        }

        @Override
        public boolean isInitialized() {
            return false;
        }
    }

    record Initial<T>(Supplier<T> supplier) implements LazyValue<T> {
        @Override
        public T get() {
            throw new IllegalStateException("LazyValue is initializing");
        }

        @Override
        public boolean isInitialized() {
            return false;
        }
    }

    record Value<T>(T value) implements LazyValue<T> {
        @Override
        public T get() {
            return value;
        }

        @Override
        public boolean isInitialized() {
            return true;
        }
    }
}
