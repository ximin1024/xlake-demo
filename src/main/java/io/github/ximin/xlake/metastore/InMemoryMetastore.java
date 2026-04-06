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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class InMemoryMetastore extends AbstractMetastore implements AutoCloseable {

    private final ConcurrentMap<String, byte[]> store = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public static InMemoryMetastore create() {
        return new InMemoryMetastore();
    }

    @Override
    protected void kvPut(byte[] key, byte[] value) throws IOException {
        checkOpen();
        store.put(new String(key, StandardCharsets.UTF_8), value);
    }

    @Override
    protected Optional<byte[]> kvGet(byte[] key) throws IOException {
        checkOpen();
        return Optional.ofNullable(store.get(new String(key, StandardCharsets.UTF_8)));
    }

    @Override
    protected void kvDelete(byte[] key) throws IOException {
        checkOpen();
        store.remove(new String(key, StandardCharsets.UTF_8));
    }

    @Override
    protected List<byte[]> kvScanByPrefix(byte[] prefix) throws IOException {
        checkOpen();
        String prefixStr = new String(prefix, StandardCharsets.UTF_8);
        List<byte[]> results = new ArrayList<>();
        store.forEach((k, v) -> {
            if (k.startsWith(prefixStr)) {
                results.add(v);
            }
        });
        return results;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        store.clear();
        log.info("InMemoryMetastore closed");
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("Metastore is closed");
        }
    }
}
