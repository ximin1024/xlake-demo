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

import io.github.ximin.xlake.backend.query.Expression;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.writer.Writer;

import java.util.Map;
import java.util.Properties;

public class Update implements Write {
    private final static OpType TYPE = OpType.UPDATE;

    private final XlakeTable table;
    private final Expression condition;
    private final Map<String, Object> updates;
    private final Writer writer;
    private final Properties config;

    public Update(XlakeTable table, Expression condition, Map<String, Object> updates, Writer writer, Properties config) {
        this.table = table;
        this.condition = condition;
        this.updates = updates;
        this.writer = writer;
        this.config = config;
    }

    @Override
    public Write.Result exec() {
        try {
            writer.init(config);

            long updatedCount = doUpdate();

            writer.flush();

            return Write.Result.ok(updatedCount);
        } catch (Exception e) {
            return Write.Result.error("Update failed: " + e.getMessage(), e);
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }
    }

    private long doUpdate() throws Exception {
        return 0L;
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    @Override
    public long writeSize() {
        return updates != null ? updates.size() : 0;
    }

    public Expression condition() {
        return condition;
    }

    public Map<String, Object> updates() {
        return updates;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements WriteBuilder<Update, Builder> {
        private XlakeTable table;
        private Expression condition;
        private Map<String, Object> updates;
        private Writer writer;
        private Properties config = new Properties();

        @Override
        public Builder withTable(XlakeTable table) {
            this.table = table;
            return this;
        }

        public Builder withCondition(Expression condition) {
            this.condition = condition;
            return this;
        }

        public Builder withUpdates(Map<String, Object> updates) {
            this.updates = updates;
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
        public Update build() {
            if (condition == null) {
                throw new IllegalStateException("condition must be set");
            }
            if (updates == null || updates.isEmpty()) {
                throw new IllegalStateException("updates must be set");
            }
            if (writer == null) {
                throw new IllegalStateException("writer must be set");
            }
            return new Update(table, condition, updates, writer, config);
        }
    }
}
