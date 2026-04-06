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
package io.github.ximin.xlake.table.op;

import java.util.Collection;

public class Insert implements Write {

    private final WriteMode mode;
    private final Collection<?> data;

    public enum WriteMode {
        APPEND,
        OVERWRITE,
        PARTIAL_OVERWRITE
    }

    public Insert(Builder builder) {
        this.mode = builder.mode;
        this.data = builder.data;
    }

    public WriteMode mode() {
        return mode;
    }

    public Collection<?> data() {
        return data;
    }

    @Override
    public OpResult exec() {
        return Write.Result.ok(data != null ? data.size() : 0);
    }

    @Override
    public String type() {
        return "INSERT";
    }

    @Override
    public long writeSize() {
        return data != null ? data.size() : 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private WriteMode mode = WriteMode.APPEND;
        private Collection<?> data;

        public Builder withMode(WriteMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder withData(Collection<?> data) {
            this.data = data;
            return this;
        }

        public Insert build() {
            return new Insert(this);
        }
    }
}
