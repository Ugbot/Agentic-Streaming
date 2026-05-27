package org.agentic.flink.function;

import org.agentic.flink.storage.vector.InMemoryVectorStore;
import java.util.HashMap;

/**
 * Process-wide default RAG components for the {@code useDefaults=true} path of the research
 * pipeline functions.
 *
 * <p>The legacy {@code DefaultEmbeddingStore} kept a static in-memory store so that a document
 * ingested by one operator instance was visible to a separate search operator instance in the
 * same JVM. The migrated {@link InMemoryVectorStore} is per-instance, so this holder reproduces
 * that shared-singleton behaviour for the zero-infra default path used by tests and local
 * development. Production paths ({@code useDefaults=false}) never touch this and instead build
 * their own store via {@code StorageFactory}.
 */
final class DefaultRagComponents {

  private static final InMemoryVectorStore SHARED_VECTOR_STORE = create();

  private DefaultRagComponents() {}

  private static InMemoryVectorStore create() {
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(new HashMap<>());
    return store;
  }

  /** The shared, in-JVM vector store used by the {@code useDefaults=true} pipeline path. */
  static InMemoryVectorStore sharedVectorStore() {
    return SHARED_VECTOR_STORE;
  }
}
