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

import java.util.Collection;
import java.util.Properties;

public abstract class BatchWrite extends AbstractWrite {
    protected final Collection data;

    public BatchWrite(Writer writer, Properties config, Collection data) {
        super(writer, config);
        this.data = data;
    }

    @Override
    protected void doWrite() throws Exception {
        Write.Result result = writer.batchWrite(data);

        // 如果 Writer 不支持批量接口，降级为逐条写入
        if (result == null) {
            for (Object item : data) {
                Write.Result singleResult = writer.write(item);
                if (!singleResult.success()) {
                    throw new RuntimeException("Record write failed: " + singleResult.message());
                }
            }
        }
    }
}
