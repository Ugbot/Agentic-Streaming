package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jagentic.core.llm.ChatResult;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.core.store.PostgresEffectJournal;
import org.junit.jupiter.api.Test;

/**
 * What the journal has to survive that the fixtures do not reach: a store that fails or stalls at
 * each write, a re-run that is not the run it replays, and more than one thread on a turn. Each test
 * pins what must never happen: a call with no record behind it, or one call answered with another's
 * result.
 */
class EffectJournalAttackTest {

  /** A store with a switch on each kind of write. */
  private static final class Flaky implements EffectJournal {
    final EffectJournal.InMemory inner = new EffectJournal.InMemory();
    final AtomicBoolean refuseIntent = new AtomicBoolean();
    final AtomicBoolean refuseOutcome = new AtomicBoolean();
    final AtomicBoolean refuseRead = new AtomicBoolean();
    long stallMs = 0;

    private void stall() {
      try {
        Thread.sleep(stallMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public Optional<Entry> get(Key key) {
      if (refuseRead.get()) {
        throw new Unavailable("read refused", null);
      }
      return inner.get(key);
    }

    @Override
    public void put(Entry entry) {
      stall();
      if (refuseOutcome.get()) {
        throw new Unavailable("write refused", null);
      }
      inner.put(entry);
    }

    @Override
    public Optional<Entry> putIfAbsent(Entry entry) {
      stall();
      if (refuseIntent.get()) {
        throw new Unavailable("intent refused", null);
      }
      return inner.putIfAbsent(entry);
    }
  }

  private static final String TURN = "t1";
  private final AtomicInteger calls = new AtomicInteger();

  private ToolRegistry registry(String effect) {
    return new ToolRegistry().registerKeyed("pay", "pays", null, effect, (args, call) -> {
      calls.incrementAndGet();
      return "paid " + args.get("to");
    });
  }

  private static AgentContext context(ToolRegistry tools, EffectJournal journal, String scope) {
    AgentContext ctx = new AgentContext("c1", TURN, "u", new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), tools, null, new ConversationLog.InMemory(), Policies.DEFAULTS);
    ctx.journal = journal;
    ctx.journalScope = scope;
    ctx.policy = ToolPolicy.ALLOW_ALL;
    return ctx;
  }

  private AgentContext context(EffectJournal journal, String effect) {
    return context(registry(effect), journal, "");
  }

  // -- a stale replay must not return another call's result ------------------------------

  /** {@code "Aa".hashCode() == "BB".hashCode()}: a 32-bit hash cannot be the identity of a call. */
  @Test
  void argumentsThatCollideUnderStringHashCodeAreStillDifferentCalls() {
    EffectJournal journal = new EffectJournal.InMemory();
    context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", "Aa"));
    assertThrows(EffectJournal.Divergence.class,
        () -> context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", "BB")));
    assertEquals(1, calls.get());
  }

  @Test
  void aNumberAndItsStringAreDifferentArguments() {
    EffectJournal journal = new EffectJournal.InMemory();
    context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", 10));
    assertThrows(EffectJournal.Divergence.class,
        () -> context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", "10")));
  }

  @Test
  void theOrderOfAMapInsideAListDoesNotMakeADifferentCall() {
    EffectJournal journal = new EffectJournal.InMemory();
    Map<String, Object> one = new LinkedHashMap<>();
    one.put("a", 1);
    one.put("b", 2);
    Map<String, Object> other = new LinkedHashMap<>();
    other.put("b", 2);
    other.put("a", 1);
    context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", List.of(one)));
    context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", List.of(other)));
    assertEquals(1, calls.get(), "the same arguments are the same call");
  }

  @Test
  void aDifferentToolAtARecordedIndexDiverges() {
    EffectJournal journal = new EffectJournal.InMemory();
    context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", "x"));
    ToolRegistry other = new ToolRegistry().registerKeyed("refund", "r", null, ToolEffect.NON_IDEMPOTENT,
        (args, call) -> {
          calls.incrementAndGet();
          return "refunded";
        });
    assertThrows(EffectJournal.Divergence.class,
        () -> context(other, journal, "").callTool("refund", Map.of("to", "x")));
    assertEquals(1, calls.get());
  }

  @Test
  void anotherWorkflowOnTheSameStoreDoesNotInheritAnEntry() {
    EffectJournal journal = new EffectJournal.InMemory();
    context(registry(ToolEffect.READ), journal, "agent-one").callTool("pay", Map.of("to", "x"));
    assertThrows(EffectJournal.Divergence.class,
        () -> context(registry(ToolEffect.READ), journal, "agent-two").callTool("pay", Map.of("to", "x")));
    assertEquals(1, calls.get());
  }

  @Test
  void aChangedModelRequestAtARecordedIndexDiverges() {
    EffectJournal journal = new EffectJournal.InMemory();
    context(journal, ToolEffect.READ).chat("system: one", () -> ChatResult.text("first"));
    assertThrows(EffectJournal.Divergence.class,
        () -> context(journal, ToolEffect.READ).chat("system: two", () -> ChatResult.text("second")));
  }

  @Test
  void theSameModelRequestReusesItsAnswer() {
    EffectJournal journal = new EffectJournal.InMemory();
    context(journal, ToolEffect.READ).chat("system: one", () -> ChatResult.text("first"));
    ChatResult again = context(journal, ToolEffect.READ).chat("system: one", () -> {
      throw new AssertionError("the model must not be asked again");
    });
    assertEquals("first", again.text());
  }

  @Test
  void aCallRecordedAsNonIdempotentStaysSoWhenANewVersionRelaxesTheTool() {
    EffectJournal journal = new EffectJournal.InMemory();
    AgentContext first = context(journal, ToolEffect.NON_IDEMPOTENT);
    String intent = EffectJournal.intentOf("", "pay", Map.of("to", "x"));
    journal.put(EffectJournal.Entry.pending(new EffectJournal.Key("c1", TURN, EffectJournal.TOOL, 0),
        intent, ToolEffect.NON_IDEMPOTENT));
    assertThrows(TurnSuspendedOnUnknown.class,
        () -> context(journal, ToolEffect.IDEMPOTENT).callTool("pay", Map.of("to", "x")));
    assertEquals(0, calls.get(), "what a run recorded outranks what a later version declares");
    assertEquals(0, first.calls.size());
  }

  @Test
  void aReRunThatStopsShortOfARecordedCallIsNotAQuietSuccess() {
    Map<String, Object> first = workflow("pay", "saga-short-" + System.nanoTime());
    GraphBuilder.Built built = GraphBuilder.build(first, null);
    EffectJournal journal = new EffectJournal.InMemory();
    new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), null, new ConversationLog.InMemory()).withJournal(journal)
        .submit(Event.turn("c", "t", "u", "pay now"));

    Map<String, Object> changed = workflow("nothing-matches", "saga-short-b");
    GraphBuilder.Built next = GraphBuilder.build(changed, null);
    TurnResult r = new LocalRuntime(next.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), next.tools(), null, new ConversationLog.InMemory())
        .withJournal(journal).submit(Event.turn("c", "t", "u", "pay now"));

    assertEquals(TurnStatus.FAILED, r.status, "a recorded call was not reached: " + r);
    assertEquals(TurnError.ErrorClass.FATAL, r.error.errorClass());
  }

  private static Map<String, Object> workflow(String trigger, String world) {
    return Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "p"),
            "paths", Map.of("p", Map.of("brain", "rule", "tools", List.of("pay"), "tool_triggers", Map.of(trigger, "pay"))),
            "verifier", Map.of("kind", "none")),
        "tools", List.of(Map.of("id", "pay", "kind", "counting", "effect", "non_idempotent", "x-world", world)));
  }

  // -- a store that fails or stalls ------------------------------------------------------

  @Test
  void anIntentThatCannotBeWrittenStopsTheCall() {
    Flaky journal = new Flaky();
    journal.refuseIntent.set(true);
    assertThrows(EffectJournal.Unavailable.class,
        () -> context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", "x")));
    assertEquals(0, calls.get());
  }

  @Test
  void anOutcomeThatCannotBeWrittenLeavesTheCallUnknownNotRepeated() {
    Flaky journal = new Flaky();
    journal.refuseOutcome.set(true);
    assertThrows(EffectJournal.Unavailable.class,
        () -> context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", "x")));
    assertEquals(1, calls.get());
    journal.refuseOutcome.set(false);
    assertThrows(TurnSuspendedOnUnknown.class,
        () -> context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", "x")));
    assertEquals(1, calls.get(), "never a second call");
  }

  @Test
  void aSlowIntentHoldsTheCallBackUntilItIsDurable() {
    Flaky journal = new Flaky();
    journal.stallMs = 50;
    List<String> order = Collections.synchronizedList(new ArrayList<>());
    EffectJournal watched = new EffectJournal() {
      @Override public Optional<Entry> get(Key key) {
        return journal.get(key);
      }

      @Override public void put(Entry entry) {
        journal.put(entry);
        order.add("recorded " + entry.state());
      }

      @Override public Optional<Entry> putIfAbsent(Entry entry) {
        Optional<Entry> was = journal.putIfAbsent(entry);
        order.add("intent durable");
        return was;
      }
    };
    ToolRegistry tools = new ToolRegistry().registerKeyed("pay", "p", null, ToolEffect.NON_IDEMPOTENT,
        (a, c) -> {
          order.add("called");
          return "ok";
        });
    context(tools, watched, "").callTool("pay", Map.of());
    assertEquals(List.of("intent durable", "called", "recorded SUCCEEDED"), order);
  }

  /** A turn cannot end on a log it could not record the end of, and a saga must not undo on this. */
  @Test
  void anOutageInsideASagaIsAnOutageNotAStepFailure() {
    Map<String, Object> workflow = Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "p"),
            "paths", Map.of("p", Map.of("brain", "rule", "tools", List.of("reserve", "post", "release"))),
            "verifier", Map.of("kind", "none")),
        "tools", List.of(
            Map.of("id", "reserve", "kind", "counting", "effect", "idempotent", "compensation", "release",
                "x-world", "saga-outage"),
            Map.of("id", "post", "kind", "counting", "effect", "non_idempotent", "x-world", "saga-outage"),
            Map.of("id", "release", "kind", "counting", "effect", "idempotent", "x-world", "saga-outage")),
        "saga", Map.of("steps", List.of(Map.of("name", "reserve", "tool", "reserve"),
            Map.of("name", "post", "tool", "post"))));
    GraphBuilder.Built built = GraphBuilder.build(workflow, null);
    Flaky journal = new Flaky();
    ConversationLog log = new ConversationLog.InMemory();
    LocalRuntime rt = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), null, log).withJournal(new EffectJournal() {
          private int intents = 0;

          @Override public Optional<Entry> get(Key key) {
            return journal.get(key);
          }

          @Override public void put(Entry entry) {
            journal.put(entry);
          }

          @Override public Optional<Entry> putIfAbsent(Entry entry) {
            if (++intents == 2) {
              throw new Unavailable("the store went away", null);
            }
            return journal.putIfAbsent(entry);
          }
        });
    assertThrows(EffectJournal.Unavailable.class, () -> rt.submit(Event.turn("c", "t", "u", "go")));
    assertFalse(log.events("c").stream().anyMatch(e -> e.is(EventType.COMPENSATION_STARTED)),
        "an outage says nothing about the step");
    assertFalse(log.events("c").stream().anyMatch(e -> e.is(EventType.TURN_FAILED)),
        "the turn id stays unspent");
  }

  /** {@code on_unavailable: degrade} is for reads: a money tool must not run with no record. */
  @Test
  void aJournalThatCannotOpenIsNotDegradedUnderANonIdempotentTool() throws Exception {
    int closed;
    try (ServerSocket s = new ServerSocket(0)) {
      closed = s.getLocalPort();
    }
    Map<String, Object> workflow = Map.of(
        "tools", List.of(Map.of("id", "pay", "kind", "constant", "effect", "non_idempotent")),
        "stores", Map.of("effects", Map.of("kind", "postgres",
            "url", "jdbc:postgresql://127.0.0.1:" + closed + "/none", "on_unavailable", "degrade")));
    assertThrows(EffectJournal.Unavailable.class, () -> EffectJournals.open(workflow));
  }

  @Test
  void aJournalMayDegradeWhenEveryToolIsSafeToRepeat() throws Exception {
    int closed;
    try (ServerSocket s = new ServerSocket(0)) {
      closed = s.getLocalPort();
    }
    Map<String, Object> workflow = Map.of(
        "tools", List.of(Map.of("id", "look", "kind", "constant", "effect", "read")),
        "stores", Map.of("effects", Map.of("kind", "postgres",
            "url", "jdbc:postgresql://127.0.0.1:" + closed + "/none", "on_unavailable", "degrade")));
    try (EffectJournal journal = EffectJournals.open(workflow)) {
      assertFalse(journal.durable());
    }
  }

  /** A server that accepts and never answers: the turn must end, not hang the task for good. */
  @Test
  void aPostgresThatStopsAnsweringDoesNotHangForever() throws Exception {
    try (ServerSocket blackHole = new ServerSocket(0)) {
      String url = "jdbc:postgresql://127.0.0.1:" + blackHole.getLocalPort() + "/none";
      assertTimeoutPreemptively(Duration.ofSeconds(40), () ->
          assertThrows(EffectJournal.Unavailable.class, () -> new PostgresEffectJournal(url, "u", "p")));
    }
  }

  @Test
  void aResultTheStoreCannotSerializeStillGetsRecorded() {
    EffectJournal.Entry entry = EffectJournal.Entry.pending(
        new EffectJournal.Key("c", "t", EffectJournal.TOOL, 0), "pay:x", ToolEffect.NON_IDEMPOTENT)
        .succeeded(new Object() {
          @Override
          public String toString() {
            return "a receipt";
          }
        });
    EffectJournal.Entry back = EffectJournals.fromJson(EffectJournals.toJson(entry));
    assertEquals(EffectJournal.State.SUCCEEDED, back.state());
    assertEquals("a receipt", back.result());
  }

  // -- a journal behind the log -----------------------------------------------------------

  @Test
  void aLogThatShowsACallWhoseEntryIsGoneIsUnknownNotAFirstCall() {
    Map<String, Object> workflow = workflow("pay", "lost-" + System.nanoTime());
    GraphBuilder.Built built = GraphBuilder.build(workflow, null);
    ConversationLog log = new ConversationLog.InMemory();
    LocalRuntime dying = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), null, log).withJournal(new EffectJournal.InMemory())
        .withProbe(new TurnProbe() {
          private static final long serialVersionUID = 1L;

          @Override
          public void duringCall(String toolId) {
            throw new Died();
          }
        });
    assertThrows(Died.class, () -> dying.submit(Event.turn("c", "t", "u", "pay now")));

    TurnResult r = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), null, log).withJournal(new EffectJournal.InMemory())
        .submit(Event.turn("c", "t", "u", "pay now"));
    assertEquals(TurnStatus.SUSPENDED, r.status, "the log says it began: " + r);
    assertTrue(r.awaitsAPerson());
  }

  private static final class Died extends Error {
    private static final long serialVersionUID = 1L;
  }

  // -- more than one thread ---------------------------------------------------------------

  @Test
  void oneTurnDeliveredManyTimesAtOnceMakesOneCall() throws Exception {
    Map<String, Object> workflow = workflow("pay", "many-" + System.nanoTime());
    GraphBuilder.Built built = GraphBuilder.build(workflow, null);
    ToolRegistry tools = built.tools();
    LocalRuntime rt = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), tools, null, new ConversationLog.InMemory());
    int deliveries = 16;
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(deliveries);
    try {
      List<CompletableFuture<TurnResult>> all = new ArrayList<>();
      for (int i = 0; i < deliveries; i++) {
        all.add(CompletableFuture.supplyAsync(() -> {
          try {
            go.await();
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          }
          return rt.submit(Event.turn("c", "t", "u", "pay now"));
        }, pool));
      }
      go.countDown();
      List<TurnStatus> statuses = all.stream().map(f -> f.join().status).toList();
      assertEquals(1, statuses.stream().filter(s -> s == TurnStatus.COMPLETED).count(), statuses.toString());
      assertEquals(deliveries - 1, statuses.stream().filter(s -> s == TurnStatus.DUPLICATE).count());
    } finally {
      pool.shutdownNow();
    }
  }

  /** Two runtimes share a store and no log, as a zombie and its replacement do. */
  @Test
  void twoWritersOnOneKeyNeverBothCall() throws Exception {
    EffectJournal journal = new EffectJournal.InMemory();
    int writers = 8;
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<CompletableFuture<Object>> all = new ArrayList<>();
      for (int i = 0; i < writers; i++) {
        all.add(CompletableFuture.supplyAsync(() -> {
          try {
            go.await();
            return context(journal, ToolEffect.NON_IDEMPOTENT).callTool("pay", Map.of("to", "x"));
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          } catch (TurnSuspendedOnUnknown e) {
            return "unknown";
          }
        }, pool));
      }
      go.countDown();
      all.forEach(CompletableFuture::join);
      assertEquals(1, calls.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aJournalOutageBeforeAResolutionChangesNothing() {
    GraphBuilder.Built built = GraphBuilder.build(workflow("pay", "resolve-" + System.nanoTime()), null);
    ConversationLog log = new ConversationLog.InMemory();
    Flaky journal = new Flaky();
    LocalRuntime dying = new LocalRuntime(built.graph().withApprovers(Approvers.ofRoles("approver")), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), null, log).withJournal(journal)
        .withProbe(new TurnProbe() {
          private static final long serialVersionUID = 1L;

          @Override
          public void duringCall(String toolId) {
            throw new Died();
          }
        });
    assertThrows(Died.class, () -> dying.submit(Event.turn("c", "t", "u", "pay now")));
    LocalRuntime rt = new LocalRuntime(built.graph().withApprovers(Approvers.ofRoles("approver")),
        new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), null, log).withJournal(journal);
    assertEquals(TurnStatus.SUSPENDED, rt.submit(Event.turn("c", "t", "u", "pay now")).status);
    long before = log.events("c").size();
    journal.refuseRead.set(true);
    assertThrows(EffectJournal.Unavailable.class,
        () -> rt.submit(Event.resume("c", "t", Map.of("resolution", "not_applied")).withPrincipal(org.jagentic.core.Principal.of("ops", null, "approver"))));
    assertEquals(before, log.events("c").size());
  }

  // -- a tool nobody described ------------------------------------------------------------

  @Test
  void aToolThatDeclaresNothingIsNonIdempotent() {
    ToolRegistry tools = new ToolRegistry().register("x", "x", p -> "ok");
    assertEquals(ToolEffect.NON_IDEMPOTENT, tools.get("x").effect());
    assertEquals(ToolEffect.NON_IDEMPOTENT,
        new ToolRegistry().registerKeyed("x", "x", null, null, (p, c) -> "ok").get("x").effect());
    assertEquals(ToolEffect.NON_IDEMPOTENT,
        new ToolRegistry().registerKeyedPeer("x", "x", (p, c) -> "ok").get("x").effect());
  }

  @Test
  void aWorkflowThatRollsBackRefusesAnUndeclaredToolPeerOrServer() {
    assertEquals("t", EffectJournals.unsafeForRollback(Map.of("tools", List.of(Map.of("id", "t")))));
    assertEquals("p", EffectJournals.unsafeForRollback(Map.of("a2a", List.of(Map.of("name", "p")))));
    assertEquals("m", EffectJournals.unsafeForRollback(Map.of("mcp", List.of(Map.of("name", "m")))));
    assertEquals(null, EffectJournals.unsafeForRollback(Map.of(
        "tools", List.of(Map.of("id", "t", "effect", "read")),
        "a2a", List.of(Map.of("name", "p", "effect", "idempotent")),
        "mcp", List.of(Map.of("name", "m", "effect", "read")))));
  }

  /** The client's own retries are a second call too. */
  @Test
  void anUndeclaredPeerIsAskedOnceAndNoAnswerIsUnknown() throws Exception {
    com.sun.net.httpserver.HttpServer peer =
        com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
    peer.createContext("/agent", exchange -> {
      calls.incrementAndGet();
      exchange.sendResponseHeaders(503, -1);
      exchange.close();
    });
    peer.start();
    try {
      Map<String, Object> workflow = Map.of(
          "spec_version", "agentic/v1",
          "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "p"),
              "paths", Map.of("p", Map.of("brain", "rule", "tools", List.of("peer"), "tool_triggers", Map.of("ask", "peer"))),
              "verifier", Map.of("kind", "none")),
          "a2a", List.of(Map.of("name", "peer", "transport", "http",
              "url", "http://127.0.0.1:" + peer.getAddress().getPort())));
      GraphBuilder.Built built = GraphBuilder.build(workflow, null);
      TurnResult r = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
          new KeyedStateStore.InMemory(), built.tools(), null, new ConversationLog.InMemory())
          .submit(Event.turn("c", "t", "u", "ask them"));
      assertEquals(1, calls.get(), "no retry inside the client");
      assertEquals(TurnStatus.SUSPENDED, r.status, "a 503 may have landed: " + r);
    } finally {
      peer.stop(0);
    }
  }

  /**
   * A rollback loses the log and the clock moves on, so a timer due on the re-run was not due on the
   * first run and takes the first index. The payment is not repeated: the turn stops, fatal, and a
   * person has to look. It does not finish, which is the limit of index-keyed replay.
   */
  @Test
  void aTimerThatShiftsTheIndicesStopsTheTurnAndRepeatsNothing() {
    String world = "timer-shift-" + System.nanoTime();
    Map<String, Object> workflow = Map.of(
        "spec_version", "agentic/v1",
        "timers", List.of(Map.of("id", "nudge", "after_ms", 1000, "tool", "note")),
        "policies", Map.of("unattended_tools", List.of("note")),
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "p"),
            "paths", Map.of("p", Map.of("brain", "rule", "tools", List.of("pay"), "tool_triggers", Map.of("pay", "pay"))),
            "verifier", Map.of("kind", "none")),
        "tools", List.of(
            Map.of("id", "pay", "kind", "counting", "effect", "non_idempotent", "x-world", world),
            Map.of("id", "note", "kind", "constant", "effect", "read", "value", "noted")));
    GraphBuilder.Built built = GraphBuilder.build(workflow, null);
    EffectJournal journal = new EffectJournal.InMemory();
    LogicalClock.Manual clock = new LogicalClock.Manual();
    ConversationLog beforeTheCrash = new ConversationLog.InMemory();
    LocalRuntime first = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), null, beforeTheCrash, clock).withJournal(journal);
    first.submit(Event.turn("c", "t0", "u", "hello"));
    ConversationLog checkpoint = new ConversationLog.InMemory();
    beforeTheCrash.events("c").forEach(e -> checkpoint.append("c", e.turnId(),
        EventType.valueOf(e.type().toUpperCase()), e.payload(), e.timestamp(), e.metadata()));
    assertEquals(TurnStatus.COMPLETED, first.submit(Event.turn("c", "t1", "u", "pay now")).status);

    clock.advance(5000);
    TurnResult rerun = new LocalRuntime(built.graph(), new ConversationStore.InMemory(),
        new KeyedStateStore.InMemory(), built.tools(), null, checkpoint, clock).withJournal(journal)
        .submit(Event.turn("c", "t1", "u", "pay now"));

    assertEquals(TurnStatus.FAILED, rerun.status, rerun.toString());
    assertEquals(TurnError.ErrorClass.FATAL, rerun.error.errorClass());
    assertEquals(1, FixtureWorld.count(world, "pay/c", "", false) - 1, "one call, then this look");
  }

  @Test
  void aPeerMayDeclareItsEffect() {
    Map<String, Object> workflow = Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "p"),
            "paths", Map.of("p", Map.of("brain", "rule")), "verifier", Map.of("kind", "none")),
        "a2a", List.of(Map.of("name", "peer", "transport", "inproc", "effect", "idempotent")));
    org.jagentic.core.pipeline.WorkflowValidator.validate(workflow);
    assertEquals(ToolEffect.IDEMPOTENT, GraphBuilder.build(workflow, null).tools().get("peer").effect());
  }
}
