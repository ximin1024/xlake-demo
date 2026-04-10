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

import io.github.ximin.xlake.metastore.client.TableMetaClient;
import io.github.ximin.xlake.table.XlakeTable;

import java.util.Optional;
import java.util.function.Supplier;

public abstract class RpcOp extends AbstractOp<OpResult> {
    protected final Supplier<TableMetaClient> clientSupplier;

    protected RpcOp(XlakeTable table, Supplier<TableMetaClient> clientSupplier) {
        super(table);
        this.clientSupplier = clientSupplier;
    }

    protected TableMetaClient getClient() {
        return clientSupplier.get();
    }

    public record Result(boolean success, Optional<String> message, Optional<Throwable> error) implements OpResult {

        @Override
        public boolean success() {
            return success;
        }

        @Override
        public long timestamp() {
            return 0;
        }

        public static Result ok() {
            return new Result(true, Optional.of("success"), null);
        }

        public static Result ok(String message) {
            return new Result(true, Optional.of(message), null);
        }

        public static Result error(String msg) {
            return new Result(false, Optional.of(msg), null);
        }

        public static Result error(String msg, Throwable throwable) {
            return new Result(false, Optional.of(msg), Optional.of(throwable));
        }
    }
}
