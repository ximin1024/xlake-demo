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
package io.github.ximin.xlake.storage.block.read;

import io.github.ximin.xlake.storage.block.ColdDataBlock;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.table.op.Find;
import io.github.ximin.xlake.table.op.Scan;
import java.io.IOException;
import java.util.List;

public class OrcBlockReader extends AbstractColdFormatBlockReader {
    public OrcBlockReader() {
        super(DataBlock.Format.ORC);
    }

    @Override
    protected Find.Result findVisible(Find find, List<ColdDataBlock> blocks) throws IOException {
        return Find.Result.error("ORC reader is not implemented yet", null);
    }

    @Override
    protected Scan.Result scanVisible(Scan scan, List<ColdDataBlock> blocks) throws IOException {
        return Scan.Result.error("ORC reader is not implemented yet", null);
    }
}
