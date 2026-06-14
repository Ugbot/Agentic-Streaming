package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.jagentic.core.store.PostgresLongTermStore;
import org.jagentic.core.store.QdrantVectorStore;
import org.jagentic.core.store.RedisConversationStore;

/** Phase C stores — in-memory offline; Qdrant/Postgres/Redis live (opt-in, skip). */
class StoreTest {

  private static String env(String k, String def) {
    String v = System.getenv(k);
    return v == null || v.isBlank() ? def : v;
  }

  private static String suffix() {
    return Long.toString(System.nanoTime());
  }

  @Test
  void inMemoryVectorStoreSearch() {
    InMemoryVectorStore vs = new InMemoryVectorStore();
    for (var e : Banking.KB.entrySet()) {
      vs.upsert(e.getKey(), Retrieval.embed(e.getValue(), Banking.DIM), e.getValue());
    }
    List<Retrieval.Scored> hits = vs.search(Retrieval.embed("tell me about crypto cash-back redemption", Banking.DIM), 2);
    assertEquals("kb_cards_crypto", hits.get(0).id());
  }

  @Test
  void inMemoryLongTermResumeAndFacts() {
    InMemoryLongTermStore s = new InMemoryLongTermStore();
    s.saveTurn("c1", "alice", "user", "hi");
    s.saveTurn("c1", "alice", "assistant", "hello");
    assertEquals(2, s.loadHistory("c1").size());
    s.saveFact("alice", "tier", "gold");
    assertEquals("gold", s.facts("alice").get("tier"));
    assertTrue(s.conversationsForUser("alice").contains("c1"));
  }

  @Test
  void qdrantColdTierIfAvailable() {
    QdrantVectorStore vs;
    try {
      vs = new QdrantVectorStore(env("AGENTIC_TEST_QDRANT_URL", "http://localhost:6333"),
          "agentic_java_test", Banking.DIM);
      for (var e : Banking.KB.entrySet()) {
        vs.upsert(e.getKey(), Retrieval.embed(e.getValue(), Banking.DIM), e.getValue());
      }
    } catch (Throwable t) {
      Assumptions.abort("Qdrant not reachable: " + t.getMessage());
      return;
    }
    List<Retrieval.Scored> hits = vs.search(Retrieval.embed("how do I dispute a charge", Banking.DIM), 2);
    assertEquals("kb_payments_dispute", hits.get(0).id());
  }

  @Test
  void postgresLongTermIfAvailable() {
    PostgresLongTermStore store;
    try {
      store = new PostgresLongTermStore(
          env("AGENTIC_TEST_PG_JDBC", "jdbc:postgresql://localhost:5434/agentic"), "agentic", "agentic", "agentic");
    } catch (Throwable t) {
      Assumptions.abort("Postgres not reachable: " + t.getMessage());
      return;
    }
    String cid = "c-" + suffix(), uid = "u-" + suffix();
    store.saveTurn(cid, uid, "user", "what is my balance?");
    store.saveTurn(cid, uid, "assistant", "1234.56");
    assertEquals(2, store.loadHistory(cid).size());
    store.saveFact(uid, "tier", "gold");
    assertEquals("gold", store.facts(uid).get("tier"));
    assertTrue(store.conversationsForUser(uid).contains(cid));
  }

  @Test
  void redisConversationStoreIfAvailable() {
    RedisConversationStore store;
    String cid = "c-" + suffix();
    try {
      store = new RedisConversationStore(env("AGENTIC_TEST_REDIS_URL", "redis://localhost:6380/0"), 200);
      store.append(cid, ChatMessage.user("hi"));
    } catch (Throwable t) {
      Assumptions.abort("Redis/Valkey not reachable: " + t.getMessage());
      return;
    }
    store.append(cid, ChatMessage.assistant("hello"));
    assertEquals(2, store.messageCount(cid));
    store.putAttribute(cid, RoutedGraph.PATH_ATTR, "cards");
    assertEquals("cards", store.getAttribute(cid, RoutedGraph.PATH_ATTR).orElseThrow());
    store.associateUser(cid, "alice");
    assertTrue(store.conversationsForUser("alice").contains(cid));
    store.clear(cid);
  }
}
