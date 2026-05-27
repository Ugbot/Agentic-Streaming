package org.agentic.flink.storage.vector;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.storage.VectorStore;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Verifies every built-in {@link VectorStore} is pluggable via {@link ServiceLoader}. Construction
 * here is connection-free (no-arg constructors); {@code initialize()} is what would connect.
 */
class VectorStoreDiscoveryTest {

  @Test
  void allBuiltInStoresAreDiscoverable() {
    Set<String> providers = new HashSet<>();
    for (VectorStore vs : ServiceLoader.load(VectorStore.class)) {
      providers.add(vs.getProviderName());
    }
    assertTrue(providers.contains("in-memory"), "in-memory store missing: " + providers);
    assertTrue(providers.contains("pgvector"), "pgvector store missing: " + providers);
    assertTrue(providers.contains("qdrant"), "qdrant store missing: " + providers);
    assertTrue(providers.contains("milvus"), "milvus store missing: " + providers);
  }
}
