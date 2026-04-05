package io.github.ximin.xlake.metastore;

import io.github.ximin.xlake.meta.*;
import io.github.ximin.xlake.metastore.storage.ShardedRocksStore;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Slf4j
public class RocksdbMetastore extends AbstractMetastore implements AutoCloseable {

    private final ShardedRocksStore rocksStore;
    private volatile boolean closed = false;

    public RocksdbMetastore(String basePath, int numShards) {
        this(basePath, numShards, 150);
    }

    public RocksdbMetastore(String basePath, int numShards, int virtualNodesPerShard) {
        this.rocksStore = new ShardedRocksStore(basePath, numShards, virtualNodesPerShard);
        log.info("RocksdbMetastore initialized: path={}, shards={} (consistent hashing)",
                basePath, numShards);
    }

    @Override
    protected void kvPut(byte[] key, byte[] value) throws IOException {
        checkOpen();
        rocksStore.put(key, value);
    }

    @Override
    protected Optional<byte[]> kvGet(byte[] key) throws IOException {
        checkOpen();
        return Optional.ofNullable(rocksStore.get(key));
    }

    @Override
    protected void kvDelete(byte[] key) throws IOException {
        checkOpen();
        rocksStore.delete(key);
    }

    @Override
    protected List<byte[]> kvScanByPrefix(byte[] prefix) throws IOException {
        checkOpen();
        return rocksStore.scanByPrefix(prefix);
    }

    public ShardedRocksStore getStore() {
        return rocksStore;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            rocksStore.close();
            log.info("RocksdbMetastore closed");
        }
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("Metastore is closed");
        }
    }
}
