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
package io.github.ximin.xlake.storage.table.write;

import io.github.ximin.xlake.storage.lmdb.LmdbInstance;
import io.github.ximin.xlake.table.op.Write;
import io.github.ximin.xlake.table.op.WriteBuilder;
import io.github.ximin.xlake.writer.LmdbWriter;
import java.io.IOException;
import java.util.Objects;

public class LmdbTableWriter implements TableWriter {

    @Override
    public Write.Result write(Write write) throws IOException {
        Objects.requireNonNull(write, "write cannot be null");
        var result = write.exec();
        if (result instanceof Write.Result writeResult) {
            return writeResult;
        }
        return Write.Result.error("Unexpected write result type: " + result.getClass().getName());
    }

    @Override
    public Write.Result write(WriteBuilder builder, WriteContext context) throws IOException {
        Objects.requireNonNull(builder, "builder cannot be null");
        Objects.requireNonNull(context, "context cannot be null");
        if (!(context instanceof LmdbWriteContext lmdbWriteContext)) {
            throw new IOException("Unsupported write context: " + context.getClass().getName());
        }
        builder.withWriter(new LmdbWriter(lmdbWriteContext.instance()));
        return write(builder.build());
    }

    public record LmdbWriteContext(LmdbInstance instance) implements WriteContext {
        public LmdbWriteContext {
            Objects.requireNonNull(instance, "instance cannot be null");
        }
    }
}
