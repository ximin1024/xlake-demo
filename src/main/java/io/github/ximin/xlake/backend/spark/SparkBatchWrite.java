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
package io.github.ximin.xlake.backend.spark;

import io.github.ximin.xlake.common.Config;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.table.schema.Schema;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Arrays;
import java.util.List;

public record SparkBatchWrite(
        XlakeTable table,
        Config.WriteConf writeConf,
        Schema requiredSchema, long commitId
) implements BatchWrite {

    @Override
    public boolean useCommitCoordinator() {
        return BatchWrite.super.useCommitCoordinator();
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
        BatchWrite.super.onDataWriterCommit(message);
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new SparkDataWriterFactory(writeConf, requiredSchema, commitId, table.uniqId());
    }

    @Override
    public void commit(WriterCommitMessage[] writerCommitMessages) {
        List<SparkCommitMessage> commitMessageList = Arrays.stream(writerCommitMessages)
                .map(message -> (SparkCommitMessage) message)
                .toList();
    }

    @Override
    public void abort(WriterCommitMessage[] writerCommitMessages) {

    }
}
