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

import io.github.ximin.xlake.table.KvRecord;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.writer.Writer;
import lombok.Getter;

public class KvWrite implements Write {
    private final static String TYPE = "KV_WRITE";
    private final XlakeTable table;
    private final byte[] key;
    private final byte[] value;
    @Getter
    private final Integer partitionHint;
    private final Writer writer;

    public KvWrite(XlakeTable table, byte[] key, byte[] value, Integer partitionHint, Writer writer) {
        this.table = table;
        this.key = key;
        this.value = value;
        this.partitionHint = partitionHint;
        this.writer = writer;
    }

    @Override
    public long writeSize() {
        return 0;
    }

    @Override
    public OpResult exec() {
        return writer.write(new KvRecord(key, value));
    }

    @Override
    public String type() {
        return TYPE;
    }
}
