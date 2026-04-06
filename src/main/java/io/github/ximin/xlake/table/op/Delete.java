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
import io.github.ximin.xlake.writer.Writer;

import java.util.Properties;

public class Delete implements Write {
    private final static String TYPE = "DELETE";

    private final Expression condition;
    private final Writer writer;
    private final Properties config;

    public Delete(Expression condition, Writer writer, Properties config) {
        this.condition = condition;
        this.writer = writer;
        this.config = config;
    }

    @Override
    public OpResult exec() {
        try {
            writer.init(config);

            long deletedCount = doDelete();

            writer.flush();

            return new WriteResult(true, "Deleted " + deletedCount + " records", null, System.currentTimeMillis(), deletedCount);
        } catch (Exception e) {
            return new WriteResult(false, "Delete failed: " + e.getMessage(), e, System.currentTimeMillis(), 0);
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }
    }

    private long doDelete() throws Exception {
        throw new UnsupportedOperationException("Delete operation not yet implemented");
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public long writeSize() {
        return 0;
    }

    public Expression condition() {
        return condition;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Expression condition;
        private Writer writer;
        private Properties config = new Properties();

        public Builder withCondition(Expression condition) {
            this.condition = condition;
            return this;
        }

        public Builder withWriter(Writer writer) {
            this.writer = writer;
            return this;
        }

        public Builder withConfig(Properties config) {
            this.config = config;
            return this;
        }

        public Builder withConfig(String key, String value) {
            this.config.setProperty(key, value);
            return this;
        }

        public Delete build() {
            if (condition == null) {
                throw new IllegalStateException("condition must be set");
            }
            if (writer == null) {
                throw new IllegalStateException("writer must be set");
            }
            return new Delete(condition, writer, config);
        }
    }
}
