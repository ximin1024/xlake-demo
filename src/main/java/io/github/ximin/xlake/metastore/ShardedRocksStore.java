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

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class ShardedRocksStore implements AutoCloseable {
    private final ConcurrentHashMap<Integer, RocksDB> shards;
    private final int numShards;

    public ShardedRocksStore(String basePath, int numShards) {
        this.numShards = numShards;
        this.shards = new ConcurrentHashMap<>();
        RocksDB.loadLibrary();

        for (int i = 0; i < numShards; i++) {
            try {
                String path = basePath + "/shard-" + i;
                new java.io.File(path).mkdirs();
                Options options = new Options()
                        .setCreateIfMissing(true)
                        .setWriteBufferSize(64 * 1024 * 1024) // 64MB
                        .setMaxWriteBufferNumber(4);
                RocksDB db = RocksDB.open(options, path);
                shards.put(i, db);
            } catch (RocksDBException e) {
                throw new RuntimeException("Failed to open shard " + i, e);
            }
        }
    }

    public void put(byte[] key, byte[] value) {
        int shardId = getShardId(key);
        try {
            shards.get(shardId).put(key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException("Put failed", e);
        }
    }

    public void delete(byte[] key) {
        int shardId = getShardId(key);
        try {
            shards.get(shardId).delete(key);
        } catch (RocksDBException e) {
            throw new RuntimeException("Delete failed", e);
        }
    }

    public byte[] get(byte[] key) {
        int shardId = getShardId(key);
        try {
            return shards.get(shardId).get(key);
        } catch (RocksDBException e) {
            throw new RuntimeException("Get failed", e);
        }
    }

    public RocksIterator newIterator(int shardId) {
        return shards.get(shardId).newIterator();
    }

    private int getShardId(byte[] key) {
        return Math.abs(Arrays.hashCode(key)) % numShards;
    }

    @Override
    public void close() {
        shards.values().forEach(RocksDB::close);
    }
}
