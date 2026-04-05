package io.github.ximin.xlake.metastore.storage;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ShardedRocksStore implements AutoCloseable {

    private final ConcurrentHashMap<Integer, RocksDB> shards;
    private final ConsistentHashRouter router;

    public ShardedRocksStore(String basePath, int numShards) {
        this(basePath, numShards, 150);
    }

    public ShardedRocksStore(String basePath, int numShards, int virtualNodesPerShard) {
        this.router = new ConsistentHashRouter(numShards, virtualNodesPerShard);
        this.shards = new ConcurrentHashMap<>();
        RocksDB.loadLibrary();

        for (int i = 0; i < numShards; i++) {
            try {
                String path = basePath + "/shard-" + i;
                new File(path).mkdirs();
                Options options = new Options()
                        .setCreateIfMissing(true)
                        .setWriteBufferSize(64 * 1024 * 1024)
                        .setMaxWriteBufferNumber(4)
                        .setCompactionStyle(org.rocksdb.CompactionStyle.UNIVERSAL);
                RocksDB db = RocksDB.open(options, path);
                shards.put(i, db);
            } catch (RocksDBException e) {
                throw new RuntimeException("Failed to open shard " + i, e);
            }
        }

        log.info("ShardedRocksStore initialized: {} shards with consistent hashing ({} vn/shard)",
                numShards, virtualNodesPerShard);
    }

    public void put(byte[] key, byte[] value) throws IOException {
        int shardId = router.getShard(key);
        try {
            shards.get(shardId).put(key, value);
        } catch (RocksDBException e) {
            throw new IOException("Put failed on shard " + shardId, e);
        }
    }

    public void delete(byte[] key) throws IOException {
        int shardId = router.getShard(key);
        try {
            shards.get(shardId).delete(key);
        } catch (RocksDBException e) {
            throw new IOException("Delete failed on shard " + shardId, e);
        }
    }

    public byte[] get(byte[] key) throws IOException {
        int shardId = router.getShard(key);
        try {
            return shards.get(shardId).get(key);
        } catch (RocksDBException e) {
            throw new IOException("Get failed on shard " + shardId, e);
        }
    }

    public List<byte[]> scanByPrefix(byte[] prefix) throws IOException {
        String prefixStr = new String(prefix, StandardCharsets.UTF_8);
        List<byte[]> results = new ArrayList<>();

        Set<Integer> candidateShards = router.getCandidateShardsForPrefix(prefix);
        log.debug("scanByPrefix '{}': scanning {} of {} candidate shards",
                prefixStr, candidateShards.size(), getNumShards());

        for (int shardId : candidateShards) {
            try (RocksIterator iterator = shards.get(shardId).newIterator()) {
                iterator.seek(prefix);
                while (iterator.isValid()) {
                    String key = new String(iterator.key(), StandardCharsets.UTF_8);
                    if (key.startsWith(prefixStr)) {
                        results.add(iterator.value());
                    } else {
                        break;
                    }
                    iterator.next();
                }
            }
        }
        return results;
    }

    public List<byte[]> scanByPrefix(byte[] prefix, int targetShardId) throws IOException {
        String prefixStr = new String(prefix, StandardCharsets.UTF_8);
        List<byte[]> results = new ArrayList<>();
        try (RocksIterator iterator = shards.get(targetShardId).newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (key.startsWith(prefixStr)) {
                    results.add(iterator.value());
                } else {
                    break;
                }
                iterator.next();
            }
        }
        return results;
    }

    public RocksIterator newIterator(int shardId) {
        return shards.get(shardId).newIterator();
    }

    public int getNumShards() {
        return shards.size();
    }

    public ConsistentHashRouter getRouter() {
        return router;
    }

    @Override
    public void close() {
        shards.values().forEach(RocksDB::close);
        log.info("ShardedRocksStore closed");
    }
}
