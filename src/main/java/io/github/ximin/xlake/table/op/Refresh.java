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

public class Refresh extends RpcOp {
    private final static OpType TYPE = OpType.REFRESH;

    private Refresh(XlakeTable table, Supplier<TableMetaClient> clientSupplier) {
        super(table, clientSupplier);
    }

    @Override
    public OpResult exec() {
        try {
            long snapshotIdBefore = table.dynamicInfo().currentSnapshotId();

            getClient().refreshTable(table.name());

            table.dynamicInfo().refresh();

            long snapshotIdAfter = table.dynamicInfo().currentSnapshotId();

            return DdlResult.RefreshResult.ok(snapshotIdBefore, snapshotIdAfter);
        } catch (Exception e) {
            return RpcOp.Result.error("Refresh failed: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    public static RefreshBuilder builder() {
        return new RefreshBuilder();
    }

    public static class RefreshBuilder implements TableOpBuilder<OpResult, Refresh> {
        private XlakeTable table;
        private Supplier<TableMetaClient> clientSupplier;

        public RefreshBuilder withTable(XlakeTable table) {
            this.table = table;
            return this;
        }

        public RefreshBuilder tableName(XlakeTable table) {
            return withTable(table);
        }

        public RefreshBuilder clientSupplier(Supplier<TableMetaClient> clientSupplier) {
            this.clientSupplier = clientSupplier;
            return this;
        }

        @Override
        public XlakeTable table() {
            return table;
        }

        @Override
        public Refresh build() {
            if (table == null) {
                throw new IllegalStateException("tableName must be set");
            }
            if (clientSupplier == null) {
                clientSupplier = GrpcTableMetaClient::new;
            }
            return new Refresh(table, clientSupplier);
        }
    }
}
