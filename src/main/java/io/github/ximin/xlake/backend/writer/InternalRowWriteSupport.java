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

import io.github.ximin.xlake.table.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.HashMap;

public class InternalRowWriteSupport extends WriteSupport<InternalRow> {
    private RecordConsumer recordConsumer;
    private final Schema schema;
    private String minKey;
    private String maxKey;

    public static final String MIN_KEY = "min_key";
    public static final String MAX_KEY = "max_key";

    public InternalRowWriteSupport(Schema schema) {
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return null;
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(InternalRow record) {
        int i = 0;
        while (i < schema.fields().size()) {
            if (!record.isNullAt(i)) {
                //recordConsumer.startField(schema.getColumnInfos().get(i).getName(), i);
                writeValue(record, i);
                //recordConsumer.endField(schema.getColumnInfos().get(i).getName(), i);
            }
            i += 1;
        }
    }

    @Override
    public WriteSupport.FinalizedWriteContext finalizeWrite() {
        HashMap<String, String> extraFooter = new HashMap<>();
        if (minKey != null && maxKey != null) {
            extraFooter.put(MIN_KEY, minKey);
            extraFooter.put(MAX_KEY, maxKey);
        }
        return new WriteSupport.FinalizedWriteContext(extraFooter);
    }

    public void add(String recordKey) {
        if (minKey != null) {
            minKey = minKey.compareTo(recordKey) <= 0 ? minKey : recordKey;
        } else {
            minKey = recordKey;
        }

        if (maxKey != null) {
            maxKey = maxKey.compareTo(recordKey) >= 0 ? maxKey : recordKey;
        } else {
            maxKey = recordKey;
        }
    }

    private void writeValue(InternalRow record, int ordinal) {
//        switch (schema.getColumnInfos().get(ordinal).getType()) {
//            case INT:
//                recordConsumer.addInteger(record.getInt(ordinal));
//                break;
//            case LONG:
//                recordConsumer.addLong(record.getLong(ordinal));
//                break;
//            case FLOAT:
//                recordConsumer.addFloat(record.getFloat(ordinal));
//                break;
//            case DOUBLE:
//                recordConsumer.addDouble(record.getDouble(ordinal));
//                break;
//            case BOOL:
//                recordConsumer.addBoolean(record.getBoolean(ordinal));
//                break;
//            case STRING:
//                recordConsumer.addBinary(Binary.fromReusedByteArray(record.getUTF8String(ordinal).getBytes()));
//                break;
//            case BINARY:
//                recordConsumer.addBinary(Binary.fromReusedByteArray(record.getBinary(ordinal)));
//                break;
//            // todo struct, map, array, user defined
//            default:
//                throw new RuntimeException(String.format("Not support %s type in lake db.", schema.getColumnInfos().get(ordinal).getType()));
//        }
    }
}
