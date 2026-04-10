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
package io.github.ximin.xlake.backend;

import io.github.ximin.xlake.storage.block.DataBlock;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

public interface RecordReader {

    void init() throws IOException;

    boolean hasNext() throws IOException;

    // todo 先用internal row替代一下
    InternalRow next() throws IOException;

    float getProgress();

    DataBlock getDataBlock();

    long getRecordsRead();
}
