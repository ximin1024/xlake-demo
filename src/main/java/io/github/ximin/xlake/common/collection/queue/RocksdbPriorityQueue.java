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
package io.github.ximin.xlake.common.collection.queue;

import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.ximin.xlake.common.utls.Strs.leftPad;


public class RocksdbPriorityQueue {
    private final static String PRIORITY_QUEUE_CF = "priority_queue";

    private final RocksDB db;
    private final AtomicLong counter = new AtomicLong();
    private final ColumnFamilyHandle priorityQueueCFHandle;

    public RocksdbPriorityQueue(RocksDB db, Map<String, ColumnFamilyHandle> cfHandleMap, String queueName) throws RocksDBException {
        this.db = db;
        String queueCFName = queueName(queueName);
        ColumnFamilyHandle queueCFHandle = cfHandleMap.get(queueCFName);
        if (queueCFHandle == null) {
            this.priorityQueueCFHandle = db.createColumnFamily(new ColumnFamilyDescriptor(queueCFName.getBytes(StandardCharsets.UTF_8)));
            cfHandleMap.put(queueCFName, this.priorityQueueCFHandle);
        } else {
            this.priorityQueueCFHandle = queueCFHandle;
        }
    }

    public void enqueue(byte[] data, int priority) throws RocksDBException {
        long adjustedPriority = -priority;
        long uniqueId = counter.incrementAndGet();
        db.put(priorityQueueCFHandle, key(adjustedPriority, uniqueId).getBytes(), data);
    }

    public byte[] dequeue() throws RocksDBException {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            if (iterator.isValid()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                db.delete(priorityQueueCFHandle, key);
                return value;
            }
        }
        // empty queue
        return null;
    }

    private String key(long adjustedPriority, long uniqueId) {
        return leftPad(Long.toString(adjustedPriority), 20) + "_" + leftPad(Long.toString(uniqueId), 20);
    }

    public void close() {
        db.close();
    }

    private String queueName(String queueName) {
        return PRIORITY_QUEUE_CF + "_" + queueName;
    }
}
