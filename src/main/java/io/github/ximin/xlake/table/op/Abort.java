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

public class Abort extends RpcOp {
    private final static String TYPE = "ABORT";

    private final long transactionId;
    private final String reason;

    private Abort(XlakeTable table, long transactionId, String reason,
                  Supplier<TableMetaClient> clientSupplier) {
        super(table, clientSupplier);
        this.transactionId = transactionId;
        this.reason = reason;
    }

    @Override
    public OpResult exec() {
        try {
            getClient().abortTransaction(table.name(), transactionId);

            return TxnResult.AbortResult.ok(transactionId, reason);
        } catch (Exception e) {
            return RpcOp.Result.error("Abort failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    public long transactionId() {
        return transactionId;
    }

    public String reason() {
        return reason;
    }

    public static AbortBuilder builder() {
        return new AbortBuilder();
    }

    public static class AbortBuilder {
        private XlakeTable table;
        private long transactionId;
        private String reason;
        private Supplier<TableMetaClient> clientSupplier;

        public AbortBuilder tableName(XlakeTable table) {
            this.table = table;
            return this;
        }

        public AbortBuilder transactionId(long transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public AbortBuilder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public AbortBuilder clientSupplier(Supplier<TableMetaClient> clientSupplier) {
            this.clientSupplier = clientSupplier;
            return this;
        }

        public Abort build() {
            if (table == null) {
                throw new IllegalStateException("tableName must be set");
            }
            if (transactionId <= 0) {
                throw new IllegalStateException("transactionId must be positive");
            }
            if (clientSupplier == null) {
                clientSupplier = GrpcTableMetaClient::new;
            }
            return new Abort(table, transactionId, reason, clientSupplier);
        }
    }
}
