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
package io.github.ximin.xlake.backend.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

public class DefaultParquetWriterBuilder<T> extends ParquetWriter.Builder<T, DefaultParquetWriterBuilder<T>> {
    private WriteSupport<T> writeSupport;
    public DefaultParquetWriterBuilder(Path path, WriteSupport<T> writeSupport) {
        super(path);
        this.writeSupport = writeSupport;
    }

    public DefaultParquetWriterBuilder(Path path) {
        super(path);
    }

    protected DefaultParquetWriterBuilder(OutputFile path) {
        super(path);
    }

    @Override
    protected DefaultParquetWriterBuilder<T> self() {
        return this;
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration conf) {
        return writeSupport;
    }
}
