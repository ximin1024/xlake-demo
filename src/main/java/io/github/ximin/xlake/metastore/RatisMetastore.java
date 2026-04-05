package io.github.ximin.xlake.metastore;

import io.github.ximin.xlake.metastore.ratis.Client;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Slf4j
public class RatisMetastore extends AbstractMetastore implements AutoCloseable {

    private final Client ratisClient;
    private volatile boolean closed = false;

    public RatisMetastore(Client ratisClient) {
        this.ratisClient = ratisClient;
    }

    public static RatisMetastore create(List<String> peers) {
        return new RatisMetastore(Client.create(peers));
    }

    public static RatisMetastore createDefault() {
        return new RatisMetastore(Client.createDefault());
    }

    @Override
    protected void kvPut(byte[] key, byte[] value) throws IOException {
        checkOpen();
        ratisClient.write(key, value);
    }

    @Override
    protected Optional<byte[]> kvGet(byte[] key) throws IOException {
        checkOpen();
        return ratisClient.read(key);
    }

    @Override
    protected void kvDelete(byte[] key) throws IOException {
        checkOpen();
        ratisClient.delete(key);
    }

    @Override
    protected List<byte[]> kvScanByPrefix(byte[] prefix) throws IOException {
        checkOpen();
        return ratisClient.scanByPrefix(prefix);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            ratisClient.close();
            log.info("[Ratis] RatisMetastore closed");
        }
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("Metastore is closed");
        }
    }
}
