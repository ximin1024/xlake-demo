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
package io.github.ximin.xlake.metastore;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class MetastoreStateMachine extends BaseStateMachine {
    private final ShardedRocksStore store;

    public MetastoreStateMachine(ShardedRocksStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        try {
            var req = MetastoreProto.GetRequest.parseFrom(request.getContent().toByteArray());
            byte[] value = store.get(req.getKey().toByteArray());
            if (value == null) {
                return CompletableFuture.completedFuture(Message.EMPTY);
            }
            return CompletableFuture.completedFuture(Message.valueOf(
                    String.valueOf(MetastoreProto.GetResponse.newBuilder()
                            .setValue(com.google.protobuf.ByteString.copyFrom(value))
                            .build().toByteString())
            ));
        } catch (Exception e) {
            LOG.error("Query failed", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        try {
            var logData = trx.getLogEntry().getStateMachineLogEntry().getLogData();
            var req = MetastoreProto.PutRequest.parseFrom(logData.toByteArray());

            switch (req.getOp()) {
                case PUT:
                    store.put(req.getKey().toByteArray(), req.getValue().toByteArray());
                    break;
                case DELETE:
                    store.delete(req.getKey().toByteArray());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown op: " + req.getOp());
            }
            return CompletableFuture.completedFuture(Message.EMPTY);
        } catch (Exception e) {
            LOG.error("Apply transaction failed", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void pause() {
    }

    @Override
    public void reinitialize() {
    }
}
