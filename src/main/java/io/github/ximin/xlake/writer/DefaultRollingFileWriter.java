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
package io.github.ximin.xlake.writer;

import io.github.ximin.xlake.table.op.Write;
import org.apache.parquet.hadoop.ParquetWriter;

import java.util.Collection;
import java.util.Properties;

public class DefaultRollingFileWriter<T> implements RollingFileWriter<T> {
    private ParquetWriter<T> writer;
    private long totalFileSize = 0;
    private long recordsWritten;
    private long recordsFailed;
    private boolean closed;
    private String filePath = "";

    public DefaultRollingFileWriter(long totalFileSize) {
        this.totalFileSize = totalFileSize;
    }

    @Override
    public boolean shouldRoll() {
        return length() <= totalFileSize;
    }

    @Override
    public void init(Properties properties) {

    }

    @Override
    public Write.Result write(T obj) {
        if (closed) {
            recordsFailed++;
            return Write.Result.error("Writer is closed");
        }
        recordsWritten++;
        return Write.Result.ok(1);
    }

    @Override
    public Write.Result batchWrite(Collection<T> objs) {
        if (objs == null || objs.isEmpty()) {
            return Write.Result.ok(0);
        }
        long written = 0;
        for (T obj : objs) {
            Write.Result result = write(obj);
            if (result.success()) {
                written++;
            }
        }
        return Write.Result.ok(written);
    }

    @Override
    public void flush() {
    }

    @Override
    public long recordsWritten() {
        return recordsWritten;
    }

    @Override
    public long recordsFailed() {
        return recordsFailed;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public String filePath() {
        return filePath;
    }

    @Override
    public long length() {
        return 0;
    }
}
