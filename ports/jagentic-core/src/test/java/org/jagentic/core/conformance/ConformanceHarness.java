package org.jagentic.core.conformance;

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
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.EffectJournals;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.LogicalClock;
import org.jagentic.core.TurnResult;
import org.jagentic.core.pipeline.GraphBuilder;

/**
 * JUnit-side binding of {@code spec/conformance/v1}: loads the fixture YAML straight from the
 * repository, drives the {@link LocalRuntime}, and re-implements the comparator of
 * {@code spec/tools/run_conformance.py} over the normalized result documents
 * ({@code spec/v1/result.schema.json}).
 *
 * <p>{@code concurrent_with} turns are handed to {@link LocalRuntime#submitAsync} without waiting
 * for one another, so they are in flight together on the runtime's per-conversation writers
 * ({@code parallelism}); their results are collected in the fixture's declared order.
 */
public final class ConformanceHarness {

  /** Capability terms ({@code spec/v1/primitives.md}) the JVM local runtime implements. */
  public static final Set<String> CAPABILITIES = Set.of(
      "routing", "rule_brain", "llm_brain", "tools", "structured_tool_args", "guardrails", "verifier",
      "ordering", "idempotency", "retry", "memory", "retrieval", "context_window", "replay", "suspend_resume",
      "saga", "a2a", "parallelism", "durable_store", "cep", "event_time", "timers", "checkpoint_recovery",
      "crash_recovery", "effect_journal", "tool_policy", "principal", "tool_approval");

  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON = new ObjectMapper();

  /** Outcome of one fixture: exactly one of passed, skipped (with reason), or failed (with problems). */
  public record Outcome(String id, String skipReason, List<String> problems) {
    public boolean skipped() {
      return skipReason != null;
    }

    public boolean passed() {
      return skipReason == null && problems.isEmpty();
    }
  }

  private ConformanceHarness() {}

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
  public static Outcome run(Path fixturePath) {
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
    try (FixtureHttpStub stub = new FixtureHttpStub()) {
      workflow = prepare(workflow, stub.base());
      // provider: stub is resolved by GraphBuilder itself; anything else is not a fixture provider.
      GraphBuilder.Built built = GraphBuilder.build(workflow, llm -> {
        throw new IllegalStateException("conformance fixtures only use llm.provider: stub, got " + llm.get("provider"));
      });
      Crash.Probe probe = Crash.newProbe();
      ConversationLog log = new ConversationLog.InMemory();
      LocalRuntime runtime = newRuntime(built, log, new LogicalClock.Manual(), probe);

      List<CompletableFuture<TurnResult>> pending = new ArrayList<>();
      List<Object> delivered = new ArrayList<>();
      for (Map<String, Object> turn : (List<Map<String, Object>>) fixture.get("turns")) {
        if (Boolean.TRUE.equals(turn.get("restart_runtime"))) {
          pending.forEach(CompletableFuture::join);
          runtime = newRuntime(built, log, LogicalClock.Manual.recoveredFrom(log), probe);
        }
        if (turn.get("advance_time_ms") != null) {
          pending.forEach(CompletableFuture::join);
          ((LogicalClock.Manual) runtime.clock()).advance(((Number) turn.get("advance_time_ms")).longValue());
        }
        Event event = eventOf(turn);
        if (turn.get("crash_at") != null) {
          pending.forEach(CompletableFuture::join);
          probe.arm((Map<String, Object>) turn.get("crash_at"));
          try {
            delivered.add(runtime.submit(event));
          } catch (Crash.SimulatedCrash crash) {
            delivered.add(crashed(turn));
            runtime = newRuntime(built, log, LogicalClock.Manual.recoveredFrom(log), probe);
          } finally {
            probe.disarm();
          }
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
    writeResults(id, results);
    return new Outcome(id, null, compare(fixture, results));
  }

  /** The event a fixture turn delivers: a signal, or a turn, under its {@code user_id}. */
  @SuppressWarnings("unchecked")
  public static Event eventOf(Map<String, Object> turn) {
    String conversationId = String.valueOf(turn.get("conversation_id"));
    String turnId = String.valueOf(turn.get("turn_id"));
    String user = String.valueOf(turn.getOrDefault("user_id", "anonymous"));
    Map<String, Object> signal = (Map<String, Object>) turn.get("signal");
    org.jagentic.core.Principal principal = org.jagentic.core.Principal.fromMap(turn.get("principal"));
    return signal != null
        ? new Event(conversationId, turnId, user, "", Map.of(), signal, principal)
        : new Event(conversationId, turnId, user, String.valueOf(turn.getOrDefault("text", "")),
            metadata(turn), null, principal);
  }

  /** The result a harness records for a delivery its {@code crash_at} killed. */
  public static Map<String, Object> crashed(Map<String, Object> turn) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("conversation_id", turn.get("conversation_id"));
    m.put("turn_id", turn.get("turn_id"));
    m.put("status", "crashed");
    m.put("events", List.of());
    return m;
  }

  /**
   * The workflow as this run sees it: {@code {{http}}} urls point at this binding's stub, and the
   * fixture's stateful tools count in a world of their own, which outlives a crash.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> prepare(Map<String, Object> workflow, String httpBase) {
    Map<String, Object> doc = JSON.convertValue(workflow, Map.class);
    String world = java.util.UUID.randomUUID().toString();
    for (Map<String, Object> tool : (List<Map<String, Object>>) doc.getOrDefault("tools", List.of())) {
      if (tool.get("url") instanceof String url) {
        tool.put("url", url.replace("{{http}}", httpBase));
      }
      tool.put(GraphBuilder.WORLD_KEY, world);
    }
    if (doc.get("llm") instanceof Map<?, ?> llm) {
      ((Map<String, Object>) llm).put(GraphBuilder.WORLD_KEY, world);
    }
    // A journal the rebuilt graph of a restarted job finds again, as it would a durable one.
    Map<String, Object> stores = new LinkedHashMap<>((Map<String, Object>) doc.getOrDefault("stores", Map.of()));
    stores.put("effects", Map.of("kind", "memory", "name", world, EffectJournals.PROCESS_LOCAL_OK, true));
    doc.put("stores", stores);
    return doc;
  }

  /** Every expectation against the results, in fixture order. */
  @SuppressWarnings("unchecked")
  public static List<String> compare(Map<String, Object> fixture, List<Map<String, Object>> results) {
    List<String> problems = new ArrayList<>();
    List<Map<String, Object>> expectations = (List<Map<String, Object>>) fixture.get("expect");
    for (int i = 0; i < expectations.size(); i++) {
      Map<String, Object> expected = expectations.get(i);
      if (i >= results.size()) {
        problems.add("expect[" + i + "]: no result produced");
        continue;
      }
      for (String p : checkExpectation(expected, results.get(i))) {
        problems.add("expect[" + i + "] (" + expected.get("turn_id") + ") " + p);
      }
    }
    return problems;
  }

  /**
   * Leaves the normalized results for {@code spec/tools/conformance_matrix.py}, which compares every
   * runtime with the one comparator, {@code run_conformance.check_expectation}.
   */
  public static void writeResults(String fixtureId, List<Map<String, Object>> results) {
    Path dir = Path.of(System.getProperty("user.dir"), "target", "conformance-results");
    try {
      Files.createDirectories(dir);
      JSON.writeValue(dir.resolve(fixtureId + ".json").toFile(), results);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The fixture turn's {@code metadata} (string values, as the spec carries them), empty if none. */
  public static Map<String, String> metadata(Map<String, Object> turn) {
    Map<String, String> out = new java.util.LinkedHashMap<>();
    if (turn.get("metadata") instanceof Map<?, ?> m) {
      for (Map.Entry<?, ?> e : m.entrySet()) {
        out.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
      }
    }
    return out;
  }

  /**
   * A restart keeps only the log: every materialized view, pending timers and the logical clock
   * included, is rebuilt from it.
   */
  private static LocalRuntime newRuntime(GraphBuilder.Built built, ConversationLog log, LogicalClock clock,
                                         Crash.Probe probe) {
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), built.retriever(), log, clock).withProbe(probe).withJournal(built.journal());
  }

  /** Port of {@code run_conformance.check_expectation}: the comparison rules of the conformance README. */
  @SuppressWarnings("unchecked")
  public static List<String> checkExpectation(Map<String, Object> expected, Map<String, Object> actual) {
    List<String> problems = new ArrayList<>();
    for (String field : List.of("conversation_id", "status", "path", "reply")) {
      if (expected.containsKey(field) && !java.util.Objects.equals(expected.get(field), actual.get(field))) {
        problems.add(mismatch(field, expected.get(field), actual.get(field)));
      }
    }
    if (expected.get("status_any") instanceof List<?> any && !any.contains(actual.get("status"))) {
      problems.add(mismatch("status_any", any, actual.get("status")));
    }
    if (expected.containsKey("reply_matches")) {
      String reply = actual.get("reply") == null ? "" : String.valueOf(actual.get("reply"));
      if (!Pattern.compile(String.valueOf(expected.get("reply_matches"))).matcher(reply).find()) {
        problems.add(mismatch("reply_matches", expected.get("reply_matches"), reply));
      }
    }
    if (expected.containsKey("error_class")) {
      Map<String, Object> error = (Map<String, Object>) actual.get("error");
      Object got = error == null ? null : error.get("class");
      if (!java.util.Objects.equals(expected.get("error_class"), got)) {
        problems.add(mismatch("error_class", expected.get("error_class"), got));
      }
    }
    if (expected.containsKey("tool_calls")) {
      List<Map<String, Object>> want = (List<Map<String, Object>>) expected.get("tool_calls");
      List<Map<String, Object>> got = (List<Map<String, Object>>) actual.getOrDefault("tool_calls", List.of());
      if (want.size() != got.size()) {
        problems.add(mismatch("tool_calls length", want.size(), got.size()));
      } else {
        for (int i = 0; i < want.size(); i++) {
          Map<String, Object> w = want.get(i);
          Map<String, Object> g = got.get(i);
          if (!java.util.Objects.equals(w.get("tool"), g.get("tool"))) {
            problems.add(mismatch("tool_calls[" + i + "].tool", w.get("tool"), g.get("tool")));
          }
          for (String key : List.of("index", "attempt", "args", "result")) {
            if (w.containsKey(key) && !sameValue(w.get(key), g.get(key))) {
              problems.add(mismatch("tool_calls[" + i + "]." + key, w.get(key), g.get(key)));
            }
          }
          boolean wantFailed = Boolean.TRUE.equals(w.get("failed"));
          if (wantFailed != (g.get("error") != null)) {
            problems.add(mismatch("tool_calls[" + i + "].failed", wantFailed, g.get("error")));
          }
        }
      }
    }
    List<String> types = new ArrayList<>();
    for (Map<String, Object> e : (List<Map<String, Object>>) actual.getOrDefault("events", List.of())) {
      types.add(String.valueOf(e.get("type")));
    }
    if (expected.containsKey("events_include")) {
      List<String> remaining = new ArrayList<>(types);
      for (Object wantedObj : (List<Object>) expected.get("events_include")) {
        String wanted = String.valueOf(wantedObj);
        int at = remaining.indexOf(wanted);
        if (at >= 0) {
          remaining = new ArrayList<>(remaining.subList(at + 1, remaining.size()));
        } else {
          problems.add("events_include: " + wanted + " missing or out of order in " + types);
        }
      }
    }
    for (Object unwanted : (List<Object>) expected.getOrDefault("events_exclude", List.of())) {
      if (types.contains(String.valueOf(unwanted))) {
        problems.add("events_exclude: " + unwanted + " present in " + types);
      }
    }
    if (expected.get("events") instanceof List<?> exact && !exact.equals(types)) {
      problems.add(mismatch("events", exact, types));
    }
    List<Map<String, Object>> events = (List<Map<String, Object>>) actual.getOrDefault("events", List.of());
    for (Map<String, Object> wanted : (List<Map<String, Object>>) expected.getOrDefault("event_payloads", List.of())) {
      int occurrence = wanted.get("occurrence") instanceof Number n ? n.intValue() : 1;
      List<Map<String, Object>> found = events.stream().filter(e -> wanted.get("type").equals(e.get("type"))).toList();
      if (found.size() < occurrence) {
        problems.add("event_payloads: no " + wanted.get("type") + " occurrence " + occurrence + " in " + types);
        continue;
      }
      Map<String, Object> payload = (Map<String, Object>) found.get(occurrence - 1).getOrDefault("payload", Map.of());
      for (Map.Entry<String, Object> e : ((Map<String, Object>) wanted.get("payload")).entrySet()) {
        if (!sameValue(e.getValue(), payload.get(e.getKey()))) {
          problems.add(mismatch("event_payloads." + wanted.get("type") + "." + e.getKey(), e.getValue(),
              payload.get(e.getKey())));
        }
      }
    }
    for (Object name : (List<Object>) expected.getOrDefault("event_fields", List.of())) {
      for (Map<String, Object> e : events) {
        if (e.get(String.valueOf(name)) == null) {
          problems.add("event_fields: " + e.get("type") + " has no " + name);
        }
      }
    }
    Map<String, Object> stateIncludes = (Map<String, Object>) expected.get("state_includes");
    Map<String, Object> state = (Map<String, Object>) actual.getOrDefault("state", Map.of());
    if (stateIncludes != null) {
      for (Map.Entry<String, Object> e : stateIncludes.entrySet()) {
        Object got = state.get(e.getKey());
        if (!sameValue(e.getValue(), got)) {
          problems.add(mismatch("state." + e.getKey(), e.getValue(), got));
        }
      }
    }
    return problems;
  }

  /** YAML integers arrive as Integer while the fold may produce Long: compare numerically. */
  private static boolean sameValue(Object want, Object got) {
    if (want instanceof Number a && got instanceof Number b) {
      return a.longValue() == b.longValue() && a.doubleValue() == b.doubleValue();
    }
    if (want instanceof Map<?, ?> || want instanceof List<?>) {
      return JSON.valueToTree(want).equals(JSON.valueToTree(got));
    }
    return java.util.Objects.equals(want, got);
  }

  private static String mismatch(String field, Object want, Object got) {
    return field + ": expected " + want + ", got " + got;
  }

  /** Convenience for reports: fixture id to outcome, in file order. */
  public static Map<String, Outcome> runAll() {
    Map<String, Outcome> out = new LinkedHashMap<>();
    for (Path p : fixtureFiles()) {
      Outcome o = run(p);
      out.put(o.id(), o);
    }
    return out;
  }
}
