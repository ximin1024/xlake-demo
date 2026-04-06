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

import io.github.ximin.xlake.table.KeyGenerator;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.writer.Writer;

import java.util.Collection;
import java.util.Properties;

public class AppendOnlyWrite extends BatchWrite {
    public final static String TYPE = "APPEND_ONLY_WRITE";

    private XlakeTable table;
    private Integer partitionId;
    private KeyGenerator keyGenerator;

    public AppendOnlyWrite(Writer writer, Properties config, Collection data) {
        super(writer, config, data);
    }

    @Override
    public String type() {
        return TYPE;
    }
}
