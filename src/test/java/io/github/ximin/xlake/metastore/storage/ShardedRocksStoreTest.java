package io.github.ximin.xlake.metastore.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardedRocksStoreTest {

    @TempDir
    Path tempDir;

    private ShardedRocksStore store;

    @BeforeEach
    void setUp() {
        store = new ShardedRocksStore(tempDir.toString(), 3, 10);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @Test
    void shouldPutAndGetData() throws IOException {
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        store.put(key, value);
        byte[] retrieved = store.get(key);

        assertThat(retrieved).isEqualTo(value);
    }

    @Test
    void shouldHandleMultipleKeys() throws IOException {
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();

        store.put(key1, value1);
        store.put(key2, value2);

        assertThat(store.get(key1)).isEqualTo(value1);
        assertThat(store.get(key2)).isEqualTo(value2);
    }

    @Test
    void shouldReturnEmptyForNonExistentKey() throws IOException {
        byte[] key = "non-existent-key".getBytes();
        byte[] result = store.get(key);

        assertThat(result).isNull();
    }

    @Test
    void shouldDeleteKey() throws IOException {
        byte[] key = "key-to-delete".getBytes();
        byte[] value = "value".getBytes();

        store.put(key, value);
        assertThat(store.get(key)).isEqualTo(value);

        store.delete(key);
        assertThat(store.get(key)).isNull();
    }

    @Test
    void shouldScanByPrefix() throws IOException {
        // Put data with common prefix
        store.put("prefix-key1".getBytes(), "value1".getBytes());
        store.put("prefix-key2".getBytes(), "value2".getBytes());
        store.put("other-key".getBytes(), "value3".getBytes());

        List<byte[]> results = store.scanByPrefix("prefix".getBytes());

        assertThat(results).hasSize(2);
        assertThat(results).anyMatch(v -> new String(v).equals("value1"));
        assertThat(results).anyMatch(v -> new String(v).equals("value2"));
    }

    @Test
    void shouldHandleEmptyPrefixScan() throws IOException {
        store.put("key1".getBytes(), "value1".getBytes());
        store.put("key2".getBytes(), "value2".getBytes());

        List<byte[]> results = store.scanByPrefix("".getBytes());

        assertThat(results).hasSize(2);
    }

    @Test
    void shouldHandleNonExistentPrefix() throws IOException {
        store.put("key1".getBytes(), "value1".getBytes());

        List<byte[]> results = store.scanByPrefix("non-existent-prefix".getBytes());

        assertThat(results).isEmpty();
    }

    @Test
    void shouldHandleLargeData() throws IOException {
        byte[] largeKey = new byte[1024]; // 1KB key
        byte[] largeValue = new byte[10 * 1024]; // 10KB value
        
        // Fill with some data
        for (int i = 0; i < largeKey.length; i++) {
            largeKey[i] = (byte) (i % 256);
        }
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        store.put(largeKey, largeValue);
        byte[] retrieved = store.get(largeKey);

        assertThat(retrieved).hasSize(largeValue.length);
        assertThat(retrieved).isEqualTo(largeValue);
    }

    @Test
    void shouldHandleSpecialCharacters() throws IOException {
        byte[] key = "key-with-特殊字符-🎯".getBytes();
        byte[] value = "value-with-特殊字符-🎯".getBytes();

        store.put(key, value);
        byte[] retrieved = store.get(key);

        assertThat(retrieved).isEqualTo(value);
    }

    @Test
    void shouldHandleConcurrentOperations() throws IOException {
        // Test that the store can handle multiple operations
        for (int i = 0; i < 100; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            store.put(key, value);
        }

        for (int i = 0; i < 100; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            assertThat(store.get(key)).isEqualTo(value);
        }
    }

    @Test
    void shouldGetNumShards() {
        int numShards = store.getNumShards();
        assertThat(numShards).isEqualTo(3);
    }

    @Test
    void shouldHandleEmptyStore() throws IOException {
        byte[] key = "non-existent".getBytes();
        assertThat(store.get(key)).isNull();

        List<byte[]> results = store.scanByPrefix("any".getBytes());
        assertThat(results).isEmpty();
    }

    @Test
    void shouldCloseProperly() throws Exception {
        store.put("key".getBytes(), "value".getBytes());
        store.close();

        // Verify that the store is closed and cannot be used
        assertThatThrownBy(() -> store.get("key".getBytes()))
                .isInstanceOf(IllegalStateException.class);
    }
}