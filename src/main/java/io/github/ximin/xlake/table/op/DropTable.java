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

import io.github.ximin.xlake.table.GrpcTableMetaClient;
import io.github.ximin.xlake.table.TableMetaClient;
import io.github.ximin.xlake.table.XlakeTable;

import java.util.function.Supplier;

public class DropTable extends RpcOp {
    private final static String TYPE = "DROP_TABLE";

    private DropTable(XlakeTable table, Supplier<TableMetaClient> clientSupplier) {
        super(table, clientSupplier);
    }

    @Override
    public OpResult exec() {
        try {
            getClient().dropTable(table.name());
            return RpcOp.Result.ok("Table dropped: " + table.name());
        } catch (Exception e) {
            return RpcOp.Result.error("Drop table failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    public static DropTableBuilder builder() {
        return new DropTableBuilder();
    }

    public static class DropTableBuilder {
        private XlakeTable nebulakeTable;
        private Supplier<TableMetaClient> clientSupplier;

        public DropTableBuilder tableName(XlakeTable nebulakeTable) {
            this.nebulakeTable = nebulakeTable;
            return this;
        }

        public DropTableBuilder clientSupplier(Supplier<TableMetaClient> clientSupplier) {
            this.clientSupplier = clientSupplier;
            return this;
        }

        public DropTable build() {
            if (nebulakeTable == null) {
                throw new IllegalStateException("tableName must be set");
            }
            if (clientSupplier == null) {
                clientSupplier = GrpcTableMetaClient::new;
            }
            return new DropTable(nebulakeTable, clientSupplier);
        }
    }
}
