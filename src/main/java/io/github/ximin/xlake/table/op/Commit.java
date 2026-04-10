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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class Commit extends RpcOp {

    private final static OpType TYPE = OpType.COMMIT;
    private final List<Op<?>> operations;
    private final long commitId;

    private Commit(XlakeTable table,
                   long commitId,
                   Supplier<TableMetaClient> clientSupplier,
                   List<Op<?>> operations) {
        super(table, clientSupplier);
        this.operations = operations;
        this.commitId = commitId;
    }

    @Override
    public OpResult exec() {
        if (operations.isEmpty()) {
            return RpcOp.Result.error("No operations to commit");
        }

        try {
            List<PbTableOperation> grpcOps = new ArrayList<>();
            for (Op<?> op : operations) {
                grpcOps.add(convertToGrpcOperation(op));
            }

            PbOperationResult result = getClient().commitOperations(table.name(), commitId, grpcOps);
            return GrpcOpResult.of(result);
        } catch (Exception e) {
            return RpcOp.Result.error("Commit failed: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    private PbTableOperation convertToGrpcOperation(Op<?> op) {
        return PbTableOperation.newBuilder()
                .setOperationType(op.type().wireName())
                .setTableName(table.name())
                .build();
    }

    public static CommitBuilder builder() {
        return new CommitBuilder();
    }

    public static class CommitBuilder implements TableOpBuilder<OpResult, Commit> {
        private XlakeTable table;
        private Supplier<TableMetaClient> clientSupplier;
        private List<Op<?>> operations = new ArrayList<>();
        private long commitId;

        public CommitBuilder withTable(XlakeTable table) {
            this.table = table;
            return this;
        }

        public CommitBuilder tableName(XlakeTable table) {
            return withTable(table);
        }

        public CommitBuilder clientSupplier(Supplier<TableMetaClient> clientSupplier) {
            this.clientSupplier = clientSupplier;
            return this;
        }

        public CommitBuilder addOperation(Op<?> operation) {
            this.operations.add(operation);
            return this;
        }

        public CommitBuilder operations(List<Op<?>> operations) {
            this.operations = operations;
            return this;
        }

        public CommitBuilder commitId(long commitId) {
            this.commitId = commitId;
            return this;
        }

        @Override
        public XlakeTable table() {
            return table;
        }

        @Override
        public Commit build() {
            if (table == null) {
                throw new IllegalStateException("tableName must be set");
            }
            if (clientSupplier == null) {
                clientSupplier = GrpcTableMetaClient::new;
            }
            return new Commit(table, commitId, clientSupplier, operations);
        }
    }
}
