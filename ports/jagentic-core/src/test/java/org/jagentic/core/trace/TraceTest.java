package org.jagentic.core.trace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.jagentic.core.Banking;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.stream.SeedChannel;
import org.jagentic.core.stream.StreamRuntime;
import org.jagentic.core.timers.InMemoryTimerService;

/** Phase 7: observability. The RecordingTracer captures a span per turn and per timer fire, with
 * the path taken and tool calls as attributes/events. */
class TraceTest {

  private static LocalRuntime banking() {
    return new LocalRuntime(Banking.buildGraph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), Banking.defaultTools(), Banking.retriever());
  }

  @Test
  void recordingTracerCapturesAttributesAndEvents() {
    RecordingTracer t = new RecordingTracer();
    t.start("demo").attr("k", "v").event("hello").end();
    assertEquals(1, t.spans().size());
    RecordingTracer.Recorded s = t.spans().get(0);
    assertEquals("demo", s.name());
    assertEquals("v", s.attrs().get("k"));
    assertEquals(List.of("hello"), s.events());
  }

  @Test
  void streamTracesEachTurnWithPathAndTools() {
    RecordingTracer t = new RecordingTracer();
    new StreamRuntime(banking()).withTracer(t)
        .run(new SeedChannel<>(List.of(new Event("c1", "alice", "what is my balance?"))));

    assertEquals(List.of("turn"), t.names());
    RecordingTracer.Recorded turn = t.spans().get(0);
    assertEquals("c1", turn.attrs().get("conversation"));
    assertEquals("payments", turn.attrs().get("path"));
    assertEquals("true", turn.attrs().get("ok"));
    assertTrue(turn.events().contains("tool:get_balance"), "tool call recorded: " + turn.events());
  }

  @Test
  void timerFiresAreTraced() {
    RecordingTracer t = new RecordingTracer();
    StreamRuntime stream = new StreamRuntime(banking()).withTracer(t);
    InMemoryTimerService timers = new InMemoryTimerService();
    timers.schedule("f", 1000, new Event("c1", "alice", "what is my balance?"));
    stream.fireDueTimers(timers, 1000);
    assertEquals(List.of("timer.fire"), t.names());
    assertEquals("payments", t.spans().get(0).attrs().get("path"));
  }

  @Test
  void noopTracerIsTheDefaultAndHarmless() {
    // no withTracer → Tracer.NOOP; run still works, nothing recorded anywhere observable
    var results = new StreamRuntime(banking())
        .run(new SeedChannel<>(List.of(new Event("c1", "u", "hello there"))));
    assertEquals("general", results.get(0).path);
  }
}
