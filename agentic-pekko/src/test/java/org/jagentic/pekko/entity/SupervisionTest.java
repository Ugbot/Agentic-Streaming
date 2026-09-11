package org.jagentic.pekko.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.TurnError;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;
import org.jagentic.pekko.testing.CountingGraph;

/**
 * Supervision: a {@link RuntimeException} inside a turn is a recorded {@code failed} turn; a fatal
 * {@link Error} escaping the turn crashes the entity, which the supervisor restarts from the
 * journal (no brain re-run, prior turns intact) so the conversation keeps working.
 */
class SupervisionTest {

  private CountingGraph graph;
  private PekkoSystem sys;
  private PekkoRuntime rt;

  @BeforeEach
  void boot() {
    graph = new CountingGraph();
    sys = new PekkoSystem(graph.deps());
    rt = new PekkoRuntime(sys.system(), Duration.ofSeconds(3));
  }

  @AfterEach
  void shutdown() {
    sys.close();
  }

  private static String rnd(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  void runtimeExceptionBecomesAFailedTurnInTheJournal() {
    graph.withCrash(() -> {
      throw new IllegalStateException("brain exploded " + UUID.randomUUID());
    });
    String cid = rnd("c");
    String t1 = rnd("t");
    TurnResult failed = rt.submit(Event.turn(cid, t1, "jon", "crash now"));
    assertEquals(TurnStatus.FAILED, failed.status);
    assertNotNull(failed.error);
    assertEquals(TurnError.ErrorClass.FATAL, failed.error.errorClass());
    assertTrue(failed.error.message().startsWith("brain exploded"));
    assertEquals(1, graph.brainCalls.get());

    TurnResult dup = rt.submit(Event.turn(cid, t1, "jon", "crash now"));
    assertEquals(TurnStatus.DUPLICATE, dup.status);
    assertEquals(failed.error, dup.error);
    assertEquals(1, graph.brainCalls.get(), "a failed turn is not retried on redelivery");
  }

  @Test
  void fatalErrorRestartsTheEntityFromTheJournalWithoutRerunningTurns() {
    graph.withCrash(() -> new OutOfMemoryError("simulated fatal " + UUID.randomUUID()));
    String cid = rnd("c");
    TurnResult before = rt.submit(Event.turn(cid, rnd("t"), "kim", "what is my balance?"));
    assertEquals(TurnStatus.COMPLETED, before.status);
    int journal = rt.state(cid).events().size();

    CompletionException boom = assertThrows(CompletionException.class,
        () -> rt.submitAsync(Event.turn(cid, rnd("t"), "kim", "crash")).join());
    assertNotNull(boom.getCause(), "the crashed turn gets no reply; the caller times out");
    assertEquals(2, graph.brainCalls.get());

    TurnResult after = rt.submit(Event.turn(cid, rnd("t"), "kim", "still alive?"));
    assertEquals(TurnStatus.COMPLETED, after.status);
    assertEquals(2L, after.state.get("turn_count"), "the crashed turn left nothing in the journal");
    assertEquals(journal, after.events.get(0).sequence(), "sequences continue from the recovered journal");
    assertEquals(3, graph.brainCalls.get(), "recovery after the crash re-ran no prior turn");
  }
}
