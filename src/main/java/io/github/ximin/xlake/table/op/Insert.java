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
import java.util.Properties;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.writer.Writer;

public class Insert implements Write {
    private final static OpType TYPE = OpType.INSERT;

    private final XlakeTable table;
    private final WriteMode mode;
    private final Collection<?> data;
    private final Writer writer;
    private final Properties config;

    public enum WriteMode {
        APPEND,
        OVERWRITE,
        PARTIAL_OVERWRITE
    }

    public Insert(Builder builder) {
        this.table = builder.table;
        this.mode = builder.mode;
        this.data = builder.data;
        this.writer = builder.writer;
        this.config = builder.config;
    }

    public WriteMode mode() {
        return mode;
    }

    public Collection<?> data() {
        return data;
    }

    @Override
    public Write.Result exec() {
        return Write.Result.ok(data != null ? data.size() : 0);
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    @Override
    public long writeSize() {
        return data != null ? data.size() : 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements WriteBuilder<Insert, Builder> {
        private XlakeTable table;
        private WriteMode mode = WriteMode.APPEND;
        private Collection<?> data;
        private Writer writer;
        private Properties config = new Properties();

        @Override
        public Builder withTable(XlakeTable table) {
            this.table = table;
            return this;
        }

        public Builder withMode(WriteMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder withData(Collection<?> data) {
            this.data = data;
            return this;
        }

        @Override
        public Builder withWriter(Writer writer) {
            this.writer = writer;
            return this;
        }

        @Override
        public Builder withConfig(Properties config) {
            this.config = config;
            return this;
        }

        public Builder withConfig(String key, String value) {
            this.config.setProperty(key, value);
            return this;
        }

        @Override
        public XlakeTable table() {
            return table;
        }

        @Override
        public Insert build() {
            return new Insert(this);
        }
    }
}
