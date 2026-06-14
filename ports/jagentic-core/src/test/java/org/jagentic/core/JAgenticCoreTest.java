package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Runnable tests for the jagentic-core essence — no engine, no model, no network. */
class JAgenticCoreTest {

  private LocalRuntime runtime() {
    return new LocalRuntime(Banking.buildGraph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), Banking.defaultTools(), Banking.retriever());
  }

  @Test
  void conversationStoreTranscriptAndAttrs() {
    ConversationStore.InMemory s = new ConversationStore.InMemory(5);
    String cid = "c-" + UUID.randomUUID();
    for (int i = 0; i < 8; i++) s.append(cid, ChatMessage.user("m" + i));
    assertEquals(5, s.messageCount(cid));
    assertEquals("m7", s.history(cid).get(4).content());
    s.putAttribute(cid, "graph.phase", "router");
    assertEquals("router", s.getAttribute(cid, "graph.phase").orElseThrow());
    s.associateUser(cid, "alice");
    assertTrue(s.conversationsForUser("alice").contains(cid));
  }

  @Test
  void hotIndexKnnAndTwoTierDedup() {
    Retrieval.InMemoryHotVectorIndex hot = new Retrieval.InMemoryHotVectorIndex(100);
    hot.upsert("h1", Retrieval.embed("the cat sat on the mat", 64), "cat on mat");
    hot.upsert("h2", Retrieval.embed("quantum physics lecture", 64), "physics");
    List<Retrieval.Scored> top = hot.search(Retrieval.embed("where is the cat and the mat", 64), 2);
    assertEquals("h1", top.get(0).id());
    assertTrue(top.get(0).score() > 0.3);

    hot.upsert("shared", Retrieval.embed("the cat sat on the mat", 64), "shared HOT");
    Retrieval.ColdSearch cold = (q, k) ->
        List.of(new Retrieval.Scored("c1", 0.4, "cold doc"),
                new Retrieval.Scored("shared", 0.05, "shared COLD"));
    Retrieval.TwoTierRetriever r = new Retrieval.TwoTierRetriever(hot, cold, 5, 5);
    List<Retrieval.Scored> merged = r.retrieve(Retrieval.embed("cat mat", 64), 10);
    List<String> ids = merged.stream().map(Retrieval.Scored::id).toList();
    assertTrue(ids.contains("h1") && ids.contains("c1"));
    assertEquals(1, ids.stream().filter("shared"::equals).count());
    assertEquals("shared HOT", merged.stream().filter(s -> s.id().equals("shared")).findFirst().orElseThrow().text());
  }

  @Test
  void routedGraphRoutesPersistsPhaseAndCallsTool() {
    LocalRuntime rt = runtime();
    String cid = "c-" + UUID.randomUUID();
    TurnResult res = rt.submit(new Event(cid, "demo", "what card types do you offer?"));
    assertEquals("cards", res.path);
    assertTrue(res.ok);
    assertTrue(res.reply.toLowerCase().contains("card"));
    assertEquals("done", rt.store().getAttribute(cid, "graph.phase").orElseThrow());
    assertEquals("cards", rt.store().getAttribute(cid, "graph.path").orElseThrow());

    TurnResult bal = rt.submit(new Event("c-" + UUID.randomUUID(), "carol", "what is my balance?"));
    assertEquals("payments", bal.path);
    assertTrue(bal.reply.contains("1234.56"));
    assertTrue(bal.toolCalls.contains("get_balance"));
  }

  @Test
  void perConversationIsolationAndUserIndex() {
    LocalRuntime rt = runtime();
    rt.submit(new Event("conv-a", "u1", "card help"));
    rt.submit(new Event("conv-b", "u1", "transfer limit"));
    List<String> forU1 = rt.store().conversationsForUser("u1");
    assertTrue(forU1.contains("conv-a") && forU1.contains("conv-b"));
    assertEquals("cards", rt.store().getAttribute("conv-a", "graph.path").orElseThrow());
    assertEquals("payments", rt.store().getAttribute("conv-b", "graph.path").orElseThrow());
    assertFalse(rt.store().getAttribute("conv-a", "graph.path").orElseThrow().equals("payments"));
  }
}
