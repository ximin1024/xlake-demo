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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Supplier;

public class FastLazy<T> implements Supplier<T> {
    private final Supplier<T> supplier;
    private volatile T value;

    private static final VarHandle VALUE_HANDLE;

    static {
        try {
            var lookup = MethodHandles.lookup();
            VALUE_HANDLE = lookup.findVarHandle(FastLazy.class, "value", Object.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public FastLazy(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public T get() {
        T result = value;
        if (result == null) {
            result = supplier.get();
            if (VALUE_HANDLE.compareAndSet(this, null, result)) {
                return result;
            } else {
                // 其他线程已经设置
                return value;
            }
        }
        return result;
    }

    public boolean isInitialized() {
        return value != null;
    }
}
