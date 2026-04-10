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

import io.github.ximin.xlake.meta.*;
import io.github.ximin.xlake.metastore.client.GrpcTableMetaClient;
import io.github.ximin.xlake.metastore.client.TableMetaClient;
import io.github.ximin.xlake.table.XlakeTable;

import java.util.function.Supplier;

public class AlterSchema extends RpcOp {
    private final static OpType TYPE = OpType.ALTER_SCHEMA;
    private final PbSchema newSchema;

    private AlterSchema(XlakeTable table, Supplier<TableMetaClient> clientSupplier, PbSchema newSchema) {
        super(table, clientSupplier);
        this.newSchema = newSchema;
    }

    @Override
    public OpResult exec() {
        try {
            var tableOp = PbTableOperation.newBuilder()
                    .setOperationType(type().wireName())
                    .setTableName(table.name())
                    .setSchema(newSchema)
                    .build();

            var result = getClient().alterTable(tableOp);
            return GrpcOpResult.of(result);
        } catch (Exception e) {
            return RpcOp.Result.error("Alter schema failed: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    public static AlterSchemaBuilder builder() {
        return new AlterSchemaBuilder();
    }

    public static class AlterSchemaBuilder implements TableOpBuilder<OpResult, AlterSchema> {
        private XlakeTable table;
        private Supplier<TableMetaClient> clientSupplier;
        private PbSchema newSchema;

        public AlterSchemaBuilder withTable(XlakeTable table) {
            this.table = table;
            return this;
        }

        public AlterSchemaBuilder tableName(XlakeTable table) {
            return withTable(table);
        }

        public AlterSchemaBuilder clientSupplier(Supplier<TableMetaClient> clientSupplier) {
            this.clientSupplier = clientSupplier;
            return this;
        }

        public AlterSchemaBuilder newSchema(PbSchema newSchema) {
            this.newSchema = newSchema;
            return this;
        }

        @Override
        public XlakeTable table() {
            return table;
        }

        @Override
        public AlterSchema build() {
            if (table == null) {
                throw new IllegalStateException("tableName must be set");
            }
            if (newSchema == null) {
                throw new IllegalStateException("newSchema must be set");
            }
            if (clientSupplier == null) {
                clientSupplier = GrpcTableMetaClient::new;
            }
            return new AlterSchema(table, clientSupplier, newSchema);
        }
    }
}
