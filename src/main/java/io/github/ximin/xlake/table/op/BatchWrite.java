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

import io.github.ximin.xlake.storage.table.record.KvRecord;
import io.github.ximin.xlake.table.record.RecordConverter;
import io.github.ximin.xlake.writer.Writer;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class BatchWrite extends AbstractWrite {
    private final static OpType TYPE = OpType.BATCH_WRITE;
    protected final Collection<?> data;

    public BatchWrite(Writer writer, Properties config, Collection<?> data) {
        super(writer, config);
        this.data = data;
    }

    @Override
    protected void doWrite() throws Exception {
        List<KvRecord> records = RecordConverter.convertFromCollection(data);

        if (records.isEmpty()) {
            return;
        }

        Write.Result result = writer.batchWrite(records);

        if (result == null) {
            for (KvRecord record : records) {
                Write.Result singleResult = writer.write(record);
                if (!singleResult.success()) {
                    throw new RuntimeException("Record write failed: " + singleResult.message());
                }
            }
        }
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    public Insert.Result execAsInsert() {
        Write.Result result = super.exec();
        return new Insert.Result(result.success(), result.count(), result.message().orElse(null));
    }
}
