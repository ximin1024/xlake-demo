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

import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.writer.Writer;

import java.util.Properties;

public interface WriteBuilder<W extends Write, B extends WriteBuilder<W, B>> extends TableOpBuilder<Write.Result, W> {
    B withTable(XlakeTable table);

    @Override
    W build();

    B withWriter(Writer writer);

    default B withConfig(Properties config) {
        throw new UnsupportedOperationException("Config is not supported by this builder");
    }

    @Override
    XlakeTable table();
}
