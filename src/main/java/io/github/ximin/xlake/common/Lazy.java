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

public class Lazy<T> {
    private volatile LazyValue<T> state;

    public Lazy(Supplier<T> supplier) {
        this.state = new LazyValue.Uninitialized<>(supplier);
    }

    public T get() {
        if (!state.isInitialized()) {
            synchronized (this) {
                if (state instanceof LazyValue.Uninitialized<T>(Supplier<T> supplier)) {
                    state = new LazyValue.Initial<>(supplier);
                    try {
                        T value = supplier.get();
                        state = new LazyValue.Value<>(value);
                    } catch (Exception e) {
                        state = new LazyValue.Uninitialized<>(supplier);
                        throw new RuntimeException("Lazy initial fail", e);
                    }
                }
            }
        }

        return state.get();
    }

    public boolean isInitialized() {
        return state.isInitialized();
    }

    public void reset() {
        if (state instanceof LazyValue.Uninitialized<T>(Supplier<T> supplier)) {
            state = new LazyValue.Uninitialized<>(supplier);
        } else if (state instanceof LazyValue.Value<T>(T value)) {
            state = new LazyValue.Uninitialized<>(() -> value);
        }
    }
}
