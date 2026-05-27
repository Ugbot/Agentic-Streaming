package org.agentic.flink.rag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agentic.flink.embedding.HashEmbeddingConnection;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.EchoChatConnection;
import org.agentic.flink.storage.vector.InMemoryVectorStore;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Zero-infrastructure tests for {@link KnowledgeBase} using the deterministic hash embedder, an
 * in-memory vector store, and an echo chat connection — no Ollama, no Claude, no network.
 */
class KnowledgeBaseTest {

  private KnowledgeBase newKb() {
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(Map.of("vector.similarity", "cosine"));
    return KnowledgeBase.builder()
        .withEmbedding(new HashEmbeddingConnection(), "hash", 64)
        .withVectorStore(store)
        .withChatConnection(
            new EchoChatConnection(),
            ChatSetup.builder().withModel("echo").withTemperature(0.0).build())
        .build();
  }

  @Test
  void ingestTextThenSearchRoundTrips() {
    KnowledgeBase kb = newKb();
    String src = "doc-" + UUID.randomUUID();
    String body =
        "Apache Flink is a distributed stream processing engine. "
            + "Agentic Flink layers LLM agents on top of Flink. "
            + "The knowledge base scrapes pages and answers questions.";
    int chunks = kb.ingestText(src, body, Map.of("title", "Intro"));
    assertTrue(chunks >= 1, "expected at least one chunk");

    List<KnowledgeBase.Passage> hits = kb.search("Apache Flink is a distributed stream processing engine.", 3);
    assertFalse(hits.isEmpty(), "search should return passages");
    // Deterministic hash embedder: the exact ingested chunk text ranks first.
    assertTrue(
        hits.get(0).text.contains("stream processing engine"),
        "top passage should be the matching chunk, got: " + hits.get(0).text);
    assertEquals("Intro", hits.get(0).title);
  }

  @Test
  void askGroundsAnswerInSourcesViaChatConnection() {
    KnowledgeBase kb = newKb();
    kb.ingestText("d1", "The capital of the demo realm is Flinkville.", Map.of("title", "Geo"));
    KnowledgeBase.Answer answer = kb.ask("What is the capital of the demo realm?", 3);
    assertNotNull(answer.text);
    assertFalse(answer.text.isBlank());
    assertFalse(answer.sources.isEmpty(), "answer should carry the retrieved sources");
  }

  @Test
  void askWithoutChatConnectionFails() {
    InMemoryVectorStore store = new InMemoryVectorStore();
    store.initialize(Map.of());
    KnowledgeBase kb =
        KnowledgeBase.builder()
            .withEmbedding(new HashEmbeddingConnection(), "hash", 64)
            .withVectorStore(store)
            .build();
    kb.ingestText("d", "some text", Map.of());
    assertThrows(IllegalStateException.class, () -> kb.ask("q?", 2));
  }

  @Test
  void statsReflectIngestedChunks() {
    KnowledgeBase kb = newKb();
    kb.ingestText("a", "alpha beta gamma delta. epsilon zeta eta theta.", Map.of());
    Object total = kb.stats().get("total_vectors");
    assertNotNull(total);
    assertTrue(((Number) total).intValue() >= 1);
  }
}
