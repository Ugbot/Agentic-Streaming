package org.agentic.flink.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.memory.vector.ScoredItem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for {@link RedisHotVectorIndex} against a real Redis (Testcontainers). Runs only
 * under {@code -P integration-tests}. Verifies the distributed hot tier: cross-instance visibility
 * (ingest process vs query process), exact cosine KNN, and FIFO eviction past the window cap.
 */
@Tag("integration")
class RedisHotVectorIndexIT {

  private static GenericContainer<?> redis;

  @BeforeAll
  static void startRedis() {
    redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    redis.start();
  }

  @AfterAll
  static void stopRedis() {
    if (redis != null) {
      redis.stop();
    }
  }

  private static float[] randomVec(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) ThreadLocalRandom.current().nextGaussian();
    }
    return v;
  }

  @Test
  @DisplayName("a query-side instance sees an ingest-side instance's writes and finds the nearest")
  void crossInstanceKnn() {
    String name = "hot-" + UUID.randomUUID();
    RedisHotVectorIndex ingest = new RedisHotVectorIndex(name, redis.getHost(), redis.getMappedPort(6379));
    int dim = 16;
    float[] target = randomVec(dim);
    ingest.upsert("target", target, "the answer", java.util.Map.of("source_url", "u://t"));
    for (int i = 0; i < 40; i++) {
      ingest.upsert("noise-" + i, randomVec(dim), "noise " + i, null);
    }

    // A separate instance (as the query operator) over the same Redis.
    RedisHotVectorIndex query = new RedisHotVectorIndex(name, redis.getHost(), redis.getMappedPort(6379));
    assertEquals(41, query.size());
    List<ScoredItem> hits = query.search(target, 5);
    assertEquals(5, hits.size());
    assertEquals("target", hits.get(0).getId());
    assertTrue(hits.get(0).getScore() > 0.99, "self-cosine ~1, was " + hits.get(0).getScore());
    assertEquals("u://t", hits.get(0).getItem().getMetadata().get("source_url"));
    query.clear();
    assertEquals(0, query.size());
  }

  @Test
  @DisplayName("FIFO eviction keeps the window bounded to maxEntries")
  void fifoEviction() {
    String name = "evict-" + UUID.randomUUID();
    RedisHotVectorIndex hot = new RedisHotVectorIndex(name, redis.getHost(), redis.getMappedPort(6379), 10, 3600);
    float[] v = randomVec(8);
    for (int i = 0; i < 25; i++) {
      hot.upsert("doc-" + i, v.clone(), "d" + i, null);
    }
    assertEquals(10, hot.size(), "window must be bounded to maxEntries");
    List<String> ids = new ArrayList<>();
    for (ScoredItem s : hot.search(v, 10)) {
      ids.add(s.getId());
    }
    assertTrue(ids.contains("doc-24"), "newest retained");
    assertFalse(ids.contains("doc-0"), "oldest FIFO-evicted");
  }
}
