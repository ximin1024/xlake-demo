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

import java.util.Collection;
import java.util.Properties;

public class Overwrite implements Write {
    private final static OpType TYPE = OpType.OVERWRITE;

    private final XlakeTable table;
    private final Collection<?> data;
    private final Expression partitionFilter;
    private final Writer writer;
    private final Properties config;

    public Overwrite(XlakeTable table, Collection<?> data, Expression partitionFilter, Writer writer, Properties config) {
        this.table = table;
        this.data = data;
        this.partitionFilter = partitionFilter;
        this.writer = writer;
        this.config = config;
    }

    @Override
    public Write.Result exec() {
        try {
            writer.init(config);

            long overwrittenCount = doOverwrite();

            writer.flush();

            return Result.ok("Overwritten " + overwrittenCount + " records", overwrittenCount);
        } catch (Exception e) {
            return Result.error("Overwrite failed: " + e.getMessage(), e);
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }
    }

    private long doOverwrite() throws Exception {
        throw new UnsupportedOperationException("Overwrite operation not yet implemented");
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    @Override
    public long writeSize() {
        return data != null ? data.size() : 0;
    }

    public Collection<?> data() {
        return data;
    }

    public Expression partitionFilter() {
        return partitionFilter;
    }

    public boolean isFullOverwrite() {
        return partitionFilter == null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements WriteBuilder<Overwrite, Builder> {
        private XlakeTable table;
        private Collection<?> data;
        private Expression partitionFilter;
        private Writer writer;
        private Properties config = new Properties();

        @Override
        public Builder withTable(XlakeTable table) {
            this.table = table;
            return this;
        }

        public Builder withData(Collection<?> data) {
            this.data = data;
            return this;
        }

        public Builder withPartitionFilter(Expression partitionFilter) {
            this.partitionFilter = partitionFilter;
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
        public Overwrite build() {
            if (data == null || data.isEmpty()) {
                throw new IllegalStateException("data must be set");
            }
            if (writer == null) {
                throw new IllegalStateException("writer must be set");
            }
            return new Overwrite(table, data, partitionFilter, writer, config);
        }
    }
}
