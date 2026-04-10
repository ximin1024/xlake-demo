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

import io.github.ximin.xlake.metastore.client.GrpcTableMetaClient;
import io.github.ximin.xlake.metastore.client.TableMetaClient;
import io.github.ximin.xlake.table.XlakeTable;

import java.util.function.Supplier;

public class DropTable extends RpcOp {
    private final static OpType TYPE = OpType.DROP_TABLE;

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
    public OpType type() {
        return TYPE;
    }

    public static DropTableBuilder builder() {
        return new DropTableBuilder();
    }

    public static class DropTableBuilder implements TableOpBuilder<OpResult, DropTable> {
        private XlakeTable xlakeTable;
        private Supplier<TableMetaClient> clientSupplier;

        public DropTableBuilder withTable(XlakeTable xlakeTable) {
            this.xlakeTable = xlakeTable;
            return this;
        }

        public DropTableBuilder tableName(XlakeTable xlakeTable) {
            return withTable(xlakeTable);
        }

        public DropTableBuilder clientSupplier(Supplier<TableMetaClient> clientSupplier) {
            this.clientSupplier = clientSupplier;
            return this;
        }

        @Override
        public XlakeTable table() {
            return xlakeTable;
        }

        @Override
        public DropTable build() {
            if (xlakeTable == null) {
                throw new IllegalStateException("tableName must be set");
            }
            if (clientSupplier == null) {
                clientSupplier = GrpcTableMetaClient::new;
            }
            return new DropTable(xlakeTable, clientSupplier);
        }
    }
}
