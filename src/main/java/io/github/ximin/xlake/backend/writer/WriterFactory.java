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

import io.github.ximin.xlake.common.Config;
import io.github.ximin.xlake.table.schema.Schema;
import lombok.Builder;
import org.apache.spark.sql.connector.write.DataWriter;

public abstract class WriterFactory<T> {

    protected final Schema schema;
    protected final Config.WriteConf writeConf;
    protected final long commitId;
    protected final String tableIdentifier;

    public WriterFactory(Schema schema, Config.WriteConf writeConf, long commitId, String tableIdentifier) {
        this.schema = schema;
        this.writeConf = writeConf;
        this.commitId = commitId;
        this.tableIdentifier = tableIdentifier;
    }

    public abstract DataWriter<T> createWriter();
}
