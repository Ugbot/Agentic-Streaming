package org.jagentic.pekko.conformance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.jagentic.core.Event;
import org.jagentic.core.conformance.ConformanceHarness;
import org.jagentic.core.conformance.Crash;
import org.jagentic.core.conformance.FixtureHttpStub;
import org.jagentic.core.LogicalClock;
import org.jagentic.core.TimerState;
import org.jagentic.core.TurnResult;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.runtime.PekkoRuntime;
import org.jagentic.pekko.runtime.PekkoSystem;

/**
 * Pekko binding of {@code spec/conformance/v1}: loads each fixture YAML straight from the repository
 * (never copied), builds its workflow with the canonical {@link GraphBuilder}, drives the turns
 * through {@link PekkoRuntime} — one event-sourced entity per conversation, in-memory journal — and
 * compares the normalized result documents ({@code spec/v1/result.schema.json}) through the core
 * {@link ConformanceHarness}, the one JVM port of {@code spec/tools/run_conformance.py}.
 *
 * <p>{@code restart_runtime} passivates every entity the fixture has touched so far: the next turn
 * recreates them from the journal alone, and the fixture's processing clock is rebuilt from the
 * {@code processing_time_ms} the recovered journals recorded. {@code concurrent_with} submits without
 * waiting, so turns for different conversations are in flight on their own entities at once
 * ({@code parallelism}) while each entity mailbox decides the order of its own turns; results are
 * collected in the fixture's declared order. {@code advance_time_ms} moves the
 * {@link LogicalClock.Manual} the entities read through {@link AgentDeps#clock()}; a turn's
 * {@code metadata} (including {@code event_time_ms}) is carried on the event.</p>
 */
public final class PekkoConformanceHarness {

  /** Capability terms ({@code spec/v1/primitives.md}) the Pekko runtime implements. */
  public static final Set<String> CAPABILITIES = Set.of(
      "routing", "rule_brain", "llm_brain", "tools", "structured_tool_args", "guardrails", "verifier",
      "ordering", "idempotency", "retry", "memory", "retrieval", "context_window", "replay", "suspend_resume",
      "saga", "a2a", "parallelism", "durable_store", "cep", "event_time", "timers", "checkpoint_recovery",
      "crash_recovery", "effect_journal");

  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  /** Outcome of one fixture: exactly one of passed, skipped (with reason), or failed (with problems). */
  public record Outcome(String id, String skipReason, List<String> problems) {
    public boolean skipped() {
      return skipReason != null;
    }

    public boolean passed() {
      return skipReason == null && problems.isEmpty();
    }
  }

  private PekkoConformanceHarness() {}

  /** {@code <repo>/spec/conformance/v1/fixtures}, found by walking up from the module directory. */
  public static Path fixturesDir() {
    Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    while (dir != null) {
      Path candidate = dir.resolve("spec").resolve("conformance").resolve("v1").resolve("fixtures");
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
      dir = dir.getParent();
    }
    throw new IllegalStateException("spec/conformance/v1/fixtures not found above " + System.getProperty("user.dir"));
  }

  public static List<Path> fixtureFiles() {
    try (Stream<Path> s = Files.list(fixturesDir())) {
      return s.filter(p -> p.toString().endsWith(".yaml")).sorted().toList();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> load(Path path) {
    try {
      return YAML.readValue(Files.readString(path), Map.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static Outcome run(Path fixturePath) throws Exception {
    Map<String, Object> fixture = load(fixturePath);
    String id = String.valueOf(fixture.get("id"));
    Set<String> missing = new LinkedHashSet<>((List<String>) fixture.get("requires"));
    missing.removeAll(CAPABILITIES);
    if (!missing.isEmpty()) {
      return new Outcome(id, "requires " + missing, List.of());
    }
    Map<String, Object> workflow = (Map<String, Object>) fixture.get("workflow");
    if (workflow == null) {
      workflow = load(fixturePath.getParent().resolve(String.valueOf(fixture.get("workflow_ref"))).normalize());
    }
    List<Map<String, Object>> results = new ArrayList<>();
    RestartableClock clock = new RestartableClock();
    Crash.Probe probe = Crash.newProbe();
    try (FixtureHttpStub stub = new FixtureHttpStub()) {
      workflow = ConformanceHarness.prepare(workflow, stub.base());
      // provider: stub is resolved by GraphBuilder itself; anything else is not a fixture provider.
      GraphBuilder.Built built = GraphBuilder.build(workflow, llm -> {
        throw new IllegalStateException("conformance fixtures only use llm.provider: stub, got " + llm.get("provider"));
      });
      try (PekkoSystem sys = new PekkoSystem(new AgentDeps(built.graph(), built.tools(), built.retriever(), clock, probe));
           PekkoRuntime runtime = new PekkoRuntime(sys.system(), TIMEOUT)) {
        List<Object> delivered = new ArrayList<>();
        List<CompletableFuture<TurnResult>> pending = new ArrayList<>();
        Set<String> touched = new LinkedHashSet<>();
        for (Map<String, Object> turn : (List<Map<String, Object>>) fixture.get("turns")) {
          if (Boolean.TRUE.equals(turn.get("restart_runtime"))) {
            pending.forEach(CompletableFuture::join);
            for (String cid : touched) {
              runtime.passivate(cid);
            }
            clock.recover(runtime, touched);
          }
          if (turn.get("advance_time_ms") instanceof Number advance) {
            pending.forEach(CompletableFuture::join);
            clock.advance(advance.longValue());
          }
          touched.add(String.valueOf(turn.get("conversation_id")));
          Event event = ConformanceHarness.eventOf(turn);
          if (turn.get("crash_at") != null) {
            pending.forEach(CompletableFuture::join);
            delivered.add(crashOn(runtime, probe, event, turn));
          } else if (turn.get("concurrent_with") != null) {
            CompletableFuture<TurnResult> f = runtime.submitAsync(event);
            pending.add(f);
            delivered.add(f);
          } else {
            pending.forEach(CompletableFuture::join);
            delivered.add(runtime.submit(event));
          }
        }
        for (Object d : delivered) {
          if (d instanceof CompletableFuture<?> f) {
            results.add(((TurnResult) f.join()).toMap());
          } else if (d instanceof TurnResult r) {
            results.add(r.toMap());
          } else {
            results.add((Map<String, Object>) d);
          }
        }
      }
    }
    ConformanceHarness.writeResults(id, results);
    return new Outcome(id, null, ConformanceHarness.compare(fixture, results));
  }

  /**
   * Kills one delivery. The crash loses what the entity had staged and not persisted, and stopping
   * it means the next command rebuilds it from the journal alone, which is what a node loss leaves.
   */
  private static Object crashOn(PekkoRuntime runtime, Crash.Probe probe, Event event, Map<String, Object> turn)
      throws Exception {
    probe.arm((Map<String, Object>) turn.get("crash_at"));
    try {
      CompletableFuture<TurnResult> reply = runtime.submitAsync(event);
      if (probe.awaitFired(TIMEOUT.toMillis())) {
        runtime.passivate(event.conversationId());
        return ConformanceHarness.crashed(turn);
      }
      return reply.get(TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    } finally {
      probe.disarm();
    }
  }

  /**
   * The fixture's processing clock. It only moves on {@code advance_time_ms}; on {@code restart_runtime}
   * it is rebuilt from the {@code processing_time_ms} recorded in the recovered journals, the way a
   * runtime that keeps its clock in the log would recover it.
   */
  static final class RestartableClock implements LogicalClock {
    private volatile LogicalClock.Manual current = new LogicalClock.Manual();

    @Override
    public long nowMs() {
      return current.nowMs();
    }

    void advance(long ms) {
      current.advance(ms);
    }

    void recover(PekkoRuntime runtime, Set<String> conversations) {
      long recorded = 0L;
      for (String cid : conversations) {
        Long t = TimerState.fold(runtime.state(cid).events()).processingTimeMs();
        if (t != null) {
          recorded = Math.max(recorded, t);
        }
      }
      current = new LogicalClock.Manual(recorded);
    }
  }

  /** Convenience for reports: fixture id to outcome, in file order. */
  public static Map<String, Outcome> runAll() throws Exception {
    Map<String, Outcome> out = new LinkedHashMap<>();
    for (Path p : fixtureFiles()) {
      Outcome o = run(p);
      out.put(o.id(), o);
    }
    return out;
  }
}
