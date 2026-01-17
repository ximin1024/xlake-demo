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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.IntStream;

public class RocksdbQueue {
    private final static String QUEUE_CF = "queue";
    private final static String HEAD_KEY = "head";
    private final static String TAIL_KEY = "tail";
    private final RocksDB db;
    private final ColumnFamilyHandle queueCFHandle;

    public RocksdbQueue(RocksDB db, Map<String, ColumnFamilyHandle> cfHandleMap, String queueName) throws RocksDBException {
        this.db = db;
        String queueCFName = queueName(queueName);
        ColumnFamilyHandle queueCFHandle = cfHandleMap.get(queueCFName);
        if (queueCFHandle == null) {
            this.queueCFHandle = db.createColumnFamily(new ColumnFamilyDescriptor(queueCFName.getBytes(StandardCharsets.UTF_8)));
            cfHandleMap.put(queueCFName, this.queueCFHandle);
        } else {
            this.queueCFHandle = queueCFHandle;
        }
        db.put(this.queueCFHandle, HEAD_KEY.getBytes(StandardCharsets.UTF_8), "0".getBytes());
        db.put(this.queueCFHandle, TAIL_KEY.getBytes(StandardCharsets.UTF_8), "0".getBytes());
    }

    public void enqueue(byte[] data) throws RocksDBException {
        long tail = getTail();
        tail++;
        db.put(queueCFHandle, String.valueOf(tail).getBytes(), data);
        setTail(tail);
    }

    public byte[] dequeue() throws RocksDBException {
        long head = getHead();
        long tail = getTail();

        // empty queue
        if (head >= tail) {
            return null;
        }
        long old = head;
        head++;
        byte[] dataBytes = db.get(queueCFHandle, String.valueOf(head).getBytes());
        setHead(head);
        db.delete(String.valueOf(old).getBytes(StandardCharsets.UTF_8));
        return dataBytes;
    }

    public void close() throws RocksDBException {
        db.close();
    }

    private long getHead() throws RocksDBException {
        byte[] headBytes = db.get(queueCFHandle, HEAD_KEY.getBytes());
        return Long.parseLong(new String(headBytes, StandardCharsets.UTF_8));
    }

    private long getTail() throws RocksDBException {
        byte[] tailBytes = db.get(queueCFHandle, TAIL_KEY.getBytes());
        return Long.parseLong(new String(tailBytes, StandardCharsets.UTF_8));
    }

    private void setHead(long head) throws RocksDBException {
        db.put(queueCFHandle, HEAD_KEY.getBytes(), String.valueOf(head).getBytes());
    }

    private void setTail(long tail) throws RocksDBException {
        db.put(queueCFHandle, TAIL_KEY.getBytes(), String.valueOf(tail).getBytes());
    }

    private OptionalInt queueCFIndex(Options options, String queueName) throws RocksDBException {
        List<byte[]> columnFamilies = RocksDB.listColumnFamilies(options, db.getName());
        String realQueueName = queueName(queueName);
        return IntStream.range(0, columnFamilies.size())
                .filter(index -> Arrays.equals(columnFamilies.get(index), realQueueName.getBytes()))
                .findFirst();
    }

    private String queueName(String queueName) {
        return QUEUE_CF + "_" + queueName;
    }
}
