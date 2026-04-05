package io.github.ximin.xlake.metastore.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsistentHashRouterTest {

    private ConsistentHashRouter router;

    @BeforeEach
    void setUp() {
        router = new ConsistentHashRouter(3, 10);
    }

    @Test
    void shouldRouteKeysConsistently() {
        String key1 = "test-key-1";
        String key2 = "test-key-2";
        String key3 = "test-key-1"; // Same as key1

        int shard1 = router.getShard(key1);
        int shard2 = router.getShard(key2);
        int shard3 = router.getShard(key3);

        assertThat(shard1).isBetween(0, 2);
        assertThat(shard2).isBetween(0, 2);
        assertThat(shard3).isEqualTo(shard1); // Same key should route to same shard
    }

    @Test
    void shouldHandleByteArrayKeys() {
        byte[] key = "test-byte-key".getBytes();
        int shard = router.getShard(key);

        assertThat(shard).isBetween(0, 2);
    }

    @Test
    void shouldGetAllShards() {
        Set<Integer> allShards = router.getAllShards();

        assertThat(allShards).hasSize(3);
        assertThat(allShards).contains(0, 1, 2);
    }

    @Test
    void shouldGetCandidateShardsForPrefix() {
        byte[] prefix = "test-prefix".getBytes();
        Set<Integer> candidates = router.getCandidateShardsForPrefix(prefix);

        assertThat(candidates).isNotEmpty();
        assertThat(candidates.size()).isLessThanOrEqualTo(3);
    }

    @Test
    void shouldHandleEmptyRing() {
        ConsistentHashRouter emptyRouter = new ConsistentHashRouter(0);
        
        assertThatThrownBy(() -> emptyRouter.getShard("test-key"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Ring is empty");
    }

    @Test
    void shouldAddAndRemoveShards() {
        // Test adding a new shard
        router.addShard(3);
        Set<Integer> shardsAfterAdd = router.getAllShards();
        assertThat(shardsAfterAdd).hasSize(4);

        // Test removing a shard
        router.removeShard(0);
        Set<Integer> shardsAfterRemove = router.getAllShards();
        assertThat(shardsAfterRemove).hasSize(3);
        assertThat(shardsAfterRemove).doesNotContain(0);
    }

    @Test
    void shouldHandleEdgeCaseKeys() {
        // Test empty key
        int shard1 = router.getShard("");
        assertThat(shard1).isBetween(0, 2);

        // Test very long key
        String longKey = "a".repeat(1000);
        int shard2 = router.getShard(longKey);
        assertThat(shard2).isBetween(0, 2);

        // Test special characters
        int shard3 = router.getShard("key-with-特殊字符-🎯");
        assertThat(shard3).isBetween(0, 2);
    }
}