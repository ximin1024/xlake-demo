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
package io.github.ximin.xlake.backend.spark.routing;

import io.github.ximin.xlake.backend.routing.RecoveryCompleteMessage;
import io.github.ximin.xlake.backend.routing.RecoveryFailureType;
import io.github.ximin.xlake.backend.routing.RecoveryTaskMessage;
import io.github.ximin.xlake.backend.routing.ShardRecoveryRecord;
import io.github.ximin.xlake.storage.DynamicMmapStore;
import io.github.ximin.xlake.storage.block.DataBlock;
import io.github.ximin.xlake.storage.table.TableStore;
import io.github.ximin.xlake.table.DynamicTableInfo;
import io.github.ximin.xlake.table.XlakeTable;
import io.github.ximin.xlake.table.Snapshot;
import io.github.ximin.xlake.table.TableMeta;
import io.github.ximin.xlake.table.op.KvWriteBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.*;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class ExecutorShardEndpoint implements RpcEndpoint {
    public static final String ENDPOINT_PREFIX = "xlake-executor-shard-endpoint-";
    private static final int MAX_BATCH_IDS_PER_SHARD = 1024;

    private final RpcEnv rpcEnv;
    private final Map<Integer, EpochDeduper> shardDedupers = new ConcurrentHashMap<>();

    public ExecutorShardEndpoint(RpcEnv rpcEnv) {
        this.rpcEnv = Objects.requireNonNull(rpcEnv, "rpcEnv must not be null");
    }

    @Override
    public RpcEnv rpcEnv() {
        return rpcEnv;
    }

    @Override
    public void onStart() {
    }

    @Override
    public void onStop() {
        shardDedupers.clear();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        return new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Object message) {
                return false;
            }

            @Override
            public BoxedUnit apply(Object message) {
                return BoxedUnit.UNIT;
            }
        };
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveAndReply(RpcCallContext context) {
        return new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Object message) {

                return message instanceof RoutingMessages.ShardWriteForward
                        || message instanceof RecoveryTaskMessage;
            }

            @Override
            public BoxedUnit apply(Object message) {
                if (message instanceof RoutingMessages.ShardWriteForward forward) {
                    try {
                        context.reply(applyWrite(forward));
                    } catch (Exception e) {
                        context.reply(RoutingMessages.WriteAck.error("apply_failed"));
                    }
                }
                if (message instanceof RecoveryTaskMessage task) {
                    context.reply(handleRecoveryTask(task));
                }
                return BoxedUnit.UNIT;
            }
        };
    }

    private RoutingMessages.WriteAck applyWrite(RoutingMessages.ShardWriteForward forward) {
        if (forward.keys() == null || forward.values() == null || forward.keys().length != forward.values().length) {
            return RoutingMessages.WriteAck.error("invalid_kv_batch");
        }

        EpochDeduper deduper = shardDedupers.computeIfAbsent(forward.shardId(), ignored -> new EpochDeduper());
        // Epoch fencing:
        // - checkAndBegin/commit/abort are synchronized and only touch metadata (epoch/committed/inflight)
        // - store.write() runs outside the deduper lock to avoid holding the lock during IO
        RoutingMessages.WriteAck precheck = deduper.checkAndBegin(forward.epoch(), forward.batchId());
        if (precheck != null) {
            return precheck;
        }

        try {
            DynamicMmapStore store = DynamicMmapStore.getInstance(forward.basePath(), forward.storeId());
            XlakeTable tableRef = new EndpointXlakeTableRef(forward.tableIdentifier());
            DynamicMmapStore.RoutingOwnerContext ownerContext =
                    new DynamicMmapStore.RoutingOwnerContext(SparkEnv.get().executorId(), forward.epoch());
            for (int i = 0; i < forward.keys().length; i++) {
                KvWriteBuilder builder = KvWriteBuilder.builder()
                        .table(tableRef)
                        .key(forward.keys()[i])
                        .value(forward.values()[i])
                        .partitionHint(forward.shardId());
                store.write(builder, ownerContext);
            }
            deduper.commit(forward.epoch(), forward.batchId());
            return RoutingMessages.WriteAck.ok();
        } catch (DynamicMmapStore.RecoverableWriteException e) {
            deduper.abort(forward.epoch(), forward.batchId());
            return RoutingMessages.WriteAck.retry("retryable_store_failure");
        } catch (Exception e) {
            // Do NOT record batchId before the actual store.write succeeds, otherwise retry could be deduped away.
            deduper.abort(forward.epoch(), forward.batchId());
            return RoutingMessages.WriteAck.error("apply_failed");
        }
    }

    private boolean handleRecoveryTask(RecoveryTaskMessage task) {
        boolean allSuccess = true;
        for (ShardRecoveryRecord record : task.shards()) {
            try {
                DynamicMmapStore store = DynamicMmapStore.getInstance(task.basePath(), task.storeId());
                store.recoverShard(record);
                sendRecoveryComplete(RecoveryCompleteMessage.success(record.shardId(), record.epoch()));
            } catch (Exception e) {
                allSuccess = false;
                log.error("Recovery failed for shard {} from executor {}",
                        record.shardId(), record.previousExecutorId(), e);
                sendRecoveryComplete(RecoveryCompleteMessage.failure(
                        record.shardId(), record.epoch(),
                        e.getMessage() != null ? e.getMessage() : "Unknown error",
                        RecoveryFailureType.WAL_REPLAY_FAILURE
                ));
            }
        }
        return allSuccess;
    }

    private void sendRecoveryComplete(RecoveryCompleteMessage message) {
        try {
            RpcEnv env = SparkEnv.get().rpcEnv();
            RpcAddress driverAddress = SparkEnv.get().conf().get("spark.driver.host") != null
                    ? RpcAddress.apply(
                    SparkEnv.get().conf().get("spark.driver.host"),
                    Integer.parseInt(SparkEnv.get().conf().get("spark.driver.port", "0"))
            )
                    : null;
            if (driverAddress != null) {
                RpcEndpointRef driverRef = env.setupEndpointRef(driverAddress,
                        DriverRoutingEndpoint.ENDPOINT_NAME);
                driverRef.send(message);
            }
        } catch (Exception e) {
            log.error("Failed to send RecoveryCompleteMessage for shard {}", message.shardId(), e);
        }
    }

    static final class EpochDeduper {
        private long epoch = -1L;
        private final ArrayDeque<String> committedOrder = new ArrayDeque<>();
        private final HashSet<String> committed = new HashSet<>();
        private final HashSet<String> inflight = new HashSet<>();


        synchronized RoutingMessages.WriteAck checkAndBegin(long incomingEpoch, String batchId) {
            if (incomingEpoch < epoch) {
                return RoutingMessages.WriteAck.staleEpoch("stale_epoch");
            }
            if (incomingEpoch > epoch) {
                // Epoch switching must fence older epoch writers. If there is any in-flight batch,
                // do NOT advance epoch (which would clear inflight) — ask caller to retry later.
                //
                // This avoids the race: old epoch passes checkAndBegin -> new epoch arrives and
                // clears inflight -> old epoch still writes but becomes "stale" on commit/abort.
                if (!inflight.isEmpty()) {
                    return RoutingMessages.WriteAck.retry("epoch_switch_inflight");
                }
                epoch = incomingEpoch;
            }
            if (batchId == null || batchId.isBlank()) {
                return RoutingMessages.WriteAck.error("invalid_batch_id");
            }
            if (committed.contains(batchId)) {
                return RoutingMessages.WriteAck.ok();
            }
            if (inflight.contains(batchId)) {
                // Another attempt is still running; ask the caller to retry later.
                return RoutingMessages.WriteAck.retry("inflight_batch");
            }
            inflight.add(batchId);
            return null;
        }

        synchronized void commit(long incomingEpoch, String batchId) {
            if (batchId == null || batchId.isBlank()) {
                return;
            }
            if (incomingEpoch < epoch) {
                inflight.remove(batchId);
                return;
            }
            if (incomingEpoch > epoch) {
                epoch = incomingEpoch;
                inflight.clear();
            }

            inflight.remove(batchId);
            if (committed.add(batchId)) {
                committedOrder.addLast(batchId);
            }
            while (committedOrder.size() > MAX_BATCH_IDS_PER_SHARD) {
                String removed = committedOrder.removeFirst();
                committed.remove(removed);
            }
        }

        synchronized void abort(long incomingEpoch, String batchId) {
            if (batchId == null || batchId.isBlank()) {
                return;
            }
            if (incomingEpoch < epoch) {
                return;
            }
            if (incomingEpoch > epoch) {
                epoch = incomingEpoch;
                inflight.clear();
            }
            inflight.remove(batchId);
        }
    }

    private record EndpointXlakeTableRef(String uniqId) implements XlakeTable {
        @Override
        public void close() {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void refresh() {
        }

        @Override
        public TableMeta meta() {
            return null;
        }

        @Override
        public DynamicTableInfo dynamicInfo() {
            return null;
        }

        @Override
        public Snapshot currentSnapshot() {
            return null;
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            return null;
        }

        @Override
        public List<Snapshot> snapshots() {
            return List.of();
        }
    }
}
