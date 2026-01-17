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

import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class SparkDeltaBatchWrite implements DeltaBatchWrite {

    @Override
    public boolean useCommitCoordinator() {
        return DeltaBatchWrite.super.useCommitCoordinator();
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
        DeltaBatchWrite.super.onDataWriterCommit(message);
    }

    @Override
    public void commit(WriterCommitMessage[] writerCommitMessages) {
        //
    }

    @Override
    public void abort(WriterCommitMessage[] writerCommitMessages) {

    }

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return null;
    }
}
