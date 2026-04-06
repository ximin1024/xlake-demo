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
