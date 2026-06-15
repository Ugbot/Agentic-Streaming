package org.jagentic.core.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.jagentic.core.Banking;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.TurnResult;

/** Phase 1: the stream substrate. Driving a channel of events must be identical to N× submit, and
 * observers must see every event — the seam later phases (CEP/windows/timers) build on. */
class StreamRuntimeTest {

  private static LocalRuntime banking() {
    return new LocalRuntime(Banking.buildGraph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), Banking.defaultTools(), Banking.retriever());
  }

  private static List<Event> turns() {
    return List.of(
        new Event("c1", "alice", "what is my balance?"),
        new Event("c2", "bob", "tell me about crypto cash-back"),
        new Event("c1", "alice", "hello there"));
  }

  @Test
  void streamingEqualsRepeatedSubmit() {
    // Reference: drive each event through submit individually.
    LocalRuntime direct = banking();
    List<String> viaSubmit = new ArrayList<>();
    for (Event e : turns()) {
      viaSubmit.add(e.conversationId() + "|" + direct.submit(e).path);
    }

    // Streaming form: a SeedChannel through a StreamRuntime over a fresh runtime.
    StreamRuntime stream = new StreamRuntime(banking());
    List<TurnResult> results = stream.run(new SeedChannel<>(turns()));
    List<String> viaStream = new ArrayList<>();
    for (int i = 0; i < results.size(); i++) {
      viaStream.add(turns().get(i).conversationId() + "|" + results.get(i).path);
    }

    assertEquals(viaSubmit, viaStream, "streaming must match repeated submit, in order");
    assertEquals(List.of("c1|payments", "c2|cards", "c1|general"), viaStream);
  }

  @Test
  void observersSeeEveryEventBeforeTheTurn() {
    List<String> seen = new ArrayList<>();
    StreamRuntime stream = new StreamRuntime(banking())
        .observe(e -> seen.add(e.conversationId()));
    stream.run(new SeedChannel<>(turns()));
    assertEquals(List.of("c1", "c2", "c1"), seen);
  }

  @Test
  void queueChannelDrainsInFifoOrder() {
    QueueChannel<Event> queue = new QueueChannel<>();
    queue.offer(new Event("c1", "u", "what is my balance?"))
         .offer(new Event("c1", "u", "tell me about crypto cash-back"));
    List<TurnResult> results = new StreamRuntime(banking()).run(queue);
    assertEquals(2, results.size());
    assertEquals("payments", results.get(0).path);
    assertEquals("cards", results.get(1).path);
    assertTrue(results.get(0).reply.contains("1234.56"));
  }
}
