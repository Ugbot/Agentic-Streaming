package org.agentic.flink.conformance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.agentic.flink.runtime.FlinkRuntimeOptions;
import org.agentic.flink.runtime.testkit.FixtureProcessingClock;
import org.agentic.flink.runtime.testkit.MiniClusterWorkflowDriver;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.conformance.ConformanceHarness;
import org.jagentic.core.conformance.Crash;
import org.jagentic.core.conformance.FixtureHttpStub;

/**
 * Flink binding of {@code spec/conformance/v1}: loads each fixture YAML straight from the
 * repository, runs its workflow as a Flink job on a {@link MiniCluster} (via {@link
 * MiniClusterWorkflowDriver}), and applies the comparator of {@code spec/tools/run_conformance.py}
 * to the normalized result documents.
 *
 * <p>Fixture verbs map onto Flink as follows: a turn is an element keyed by its conversation id;
 * {@code concurrent_with} submits the turns back to back into the same keyed stream, which the job
 * runs at {@link #PARALLELISM} subtasks so different conversations execute at the same time while
 * Flink's per-key ordering keeps each conversation serial; {@code restart_runtime} is
 * stop-with-savepoint followed by a fresh job restored from that savepoint, so only checkpointed
 * state survives; {@code advance_time_ms} moves the {@link FixtureProcessingClock} the job's
 * operator reads as the spec's processing clock, and a turn's {@code metadata} (including {@code
 * event_time_ms}) is carried on the event.
 *
 * <p>{@code crash_at} kills the job at the named point and Flink restarts it from a savepoint taken
 * just before the delivery, so the turn's log is lost and the outside world is not. Comparison goes
 * through {@link ConformanceHarness}, the one JVM port of the comparator.
 */
public final class FlinkConformanceHarness {

  /**
   * Capability terms ({@code spec/v1/primitives.md}) the Flink runtime implements and this suite
   * exercises.
   */
  public static final Set<String> CAPABILITIES =
      Set.of(
          "routing",
          "rule_brain",
          "llm_brain",
          "tools",
          "structured_tool_args",
          "guardrails",
          "verifier",
          "ordering",
          "idempotency",
          "retry",
          "memory",
          "retrieval",
          "context_window",
          "replay",
          "suspend_resume",
          "saga",
          "a2a",
          "parallelism",
          "durable_store",
          "cep",
          "event_time",
          "timers",
          "checkpoint_recovery",
          "crash_recovery",
          "effect_journal",
          "tool_policy",
          "principal",
          "tool_approval");

  /**
   * Operator parallelism of every fixture job; more than one subtask is what {@code parallelism}
   * claims.
   */
  public static final int PARALLELISM = 2;

  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  /**
   * Outcome of one fixture: exactly one of passed, skipped (with reason), or failed (with
   * problems).
   */
  public record Outcome(String id, String skipReason, List<String> problems) {
    public boolean skipped() {
      return skipReason != null;
    }

    public boolean passed() {
      return skipReason == null && problems.isEmpty();
    }
  }

  private FlinkConformanceHarness() {}

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
    throw new IllegalStateException(
        "spec/conformance/v1/fixtures not found above " + System.getProperty("user.dir"));
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
  public static Outcome run(Path fixturePath, MiniCluster cluster, Path savepointDir)
      throws Exception {
    Map<String, Object> fixture = load(fixturePath);
    String id = String.valueOf(fixture.get("id"));
    Set<String> missing = new LinkedHashSet<>((List<String>) fixture.get("requires"));
    missing.removeAll(CAPABILITIES);
    if (!missing.isEmpty()) {
      return new Outcome(id, "requires " + missing, List.of());
    }
    Map<String, Object> workflow = (Map<String, Object>) fixture.get("workflow");
    if (workflow == null) {
      workflow =
          load(
              fixturePath
                  .getParent()
                  .resolve(String.valueOf(fixture.get("workflow_ref")))
                  .normalize());
    }

    List<Map<String, Object>> results = new ArrayList<>();
    Crash.Probe probe = Crash.newProbe();
    try (FixtureHttpStub stub = new FixtureHttpStub();
        FixtureProcessingClock clock = new FixtureProcessingClock()) {
      workflow = ConformanceHarness.prepare(workflow, stub.base());
      try (MiniClusterWorkflowDriver driver =
          new MiniClusterWorkflowDriver(
              cluster,
              workflow,
              FlinkRuntimeOptions.fromSpec(workflow).withProcessingClock(clock).withProbe(probe),
              savepointDir.resolve(id),
              PARALLELISM)) {
        driver.start();
        List<Event> batch = new ArrayList<>();
        for (Map<String, Object> turn : (List<Map<String, Object>>) fixture.get("turns")) {
          if (Boolean.TRUE.equals(turn.get("restart_runtime"))) {
            flush(driver, batch, results);
            driver.restart();
          }
          if (turn.get("advance_time_ms") instanceof Number advance) {
            flush(driver, batch, results);
            clock.advance(advance.longValue());
          }
          Event event = ConformanceHarness.eventOf(turn);
          if (turn.get("crash_at") != null) {
            flush(driver, batch, results);
            crashOn(driver, probe, event, turn, results);
          } else if (turn.get("concurrent_with") != null) {
            batch.add(event);
          } else {
            flush(driver, batch, results);
            batch.add(event);
          }
        }
        flush(driver, batch, results);
      }
    }
    ConformanceHarness.writeResults(id, results);
    return new Outcome(id, null, ConformanceHarness.compare(fixture, results));
  }

  /**
   * Kills one delivery. A savepoint first, so that "the last checkpoint" is exactly the state
   * before this turn. The crash then fails the task and Flink restarts the job from that savepoint:
   * the turn's log is gone, which is the window the journal exists for.
   */
  @SuppressWarnings("unchecked")
  private static void crashOn(
      MiniClusterWorkflowDriver driver,
      Crash.Probe probe,
      Event event,
      Map<String, Object> turn,
      List<Map<String, Object>> results)
      throws Exception {
    driver.restart();
    probe.arm((Map<String, Object>) turn.get("crash_at"));
    try {
      if (driver.submitUntilCrash(event, () -> probe.awaitFired(60_000))) {
        results.add(ConformanceHarness.crashed(turn));
      } else {
        results.add(driver.awaitNext().toMap());
      }
    } finally {
      probe.disarm();
    }
  }

  private static void flush(
      MiniClusterWorkflowDriver driver, List<Event> batch, List<Map<String, Object>> results)
      throws Exception {
    if (batch.isEmpty()) {
      return;
    }
    for (TurnResult r : driver.submitAll(List.copyOf(batch))) {
      results.add(r.toMap());
    }
    batch.clear();
  }

  /** Convenience for reports: fixture id to outcome, in file order. */
  public static Map<String, Outcome> runAll(MiniCluster cluster, Path savepointDir)
      throws Exception {
    Map<String, Outcome> out = new LinkedHashMap<>();
    for (Path p : fixtureFiles()) {
      Outcome o = run(p, cluster, savepointDir);
      out.put(o.id(), o);
    }
    return out;
  }
}
