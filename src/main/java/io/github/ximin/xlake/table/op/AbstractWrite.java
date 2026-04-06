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

import io.github.ximin.xlake.writer.Writer;

import java.util.Properties;

public abstract class AbstractWrite implements Write {
    protected final Properties config;
    protected final Writer writer;

    public AbstractWrite(Writer writer, Properties config) {
        this.writer = writer;
        this.config = config;
    }

    @Override
    public OpResult exec() {
        try {
            writer.init(config);

            doWrite();

            writer.flush();

            return Result.ok(writer.recordsWritten());

        } catch (Exception e) {
            return Result.error("Write failed: " + e.getMessage());
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
                // 记录日志即可，不影响主流程
            }
        }
    }

    protected abstract void doWrite() throws Exception;

    @Override
    public long writeSize() {
        return writer.recordsWritten();
    }
}



