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
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.TurnResult;
import org.jagentic.core.pipeline.GraphBuilder;

/**
 * JUnit-side binding of {@code spec/conformance/v1}: loads the fixture YAML straight from the
 * repository, drives the {@link LocalRuntime}, and re-implements the comparator of
 * {@code spec/tools/run_conformance.py} over the normalized result documents
 * ({@code spec/v1/result.schema.json}).
 */
public final class ConformanceHarness {

  /** Capability terms ({@code spec/v1/primitives.md}) the JVM local runtime implements. */
  public static final Set<String> CAPABILITIES = Set.of(
      "routing", "rule_brain", "tools", "structured_tool_args", "guardrails", "verifier",
      "ordering", "idempotency", "retry", "memory", "retrieval", "context_window", "replay", "suspend_resume",
      "saga", "a2a", "durable_store");

  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

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

    GraphBuilder.Built built = GraphBuilder.build(workflow, llm -> {
      throw new IllegalStateException("conformance fixtures do not use an LLM");
    });
    ConversationLog log = new ConversationLog.InMemory();
    LocalRuntime runtime = newRuntime(built, log);

    List<CompletableFuture<TurnResult>> pending = new ArrayList<>();
    for (Map<String, Object> turn : (List<Map<String, Object>>) fixture.get("turns")) {
      if (Boolean.TRUE.equals(turn.get("restart_runtime"))) {
        pending.forEach(CompletableFuture::join);
        runtime = newRuntime(built, log);
      }
      String conversationId = String.valueOf(turn.get("conversation_id"));
      String turnId = String.valueOf(turn.get("turn_id"));
      Map<String, Object> signal = (Map<String, Object>) turn.get("signal");
      Event event = signal != null
          ? Event.resume(conversationId, turnId, signal)
          : Event.turn(conversationId, turnId, "anonymous", String.valueOf(turn.getOrDefault("text", "")));
      if (turn.get("concurrent_with") != null) {
        pending.add(runtime.submitAsync(event));
      } else {
        pending.forEach(CompletableFuture::join);
        pending.add(CompletableFuture.completedFuture(runtime.submit(event)));
      }
    }
    List<Map<String, Object>> results = pending.stream().map(f -> f.join().toMap()).toList();

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
    return new Outcome(id, null, problems);
  }

  /** A restart keeps only the log: every materialized view is rebuilt from it. */
  private static LocalRuntime newRuntime(GraphBuilder.Built built, ConversationLog log) {
    return new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), built.retriever(), log);
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
          for (String key : List.of("index", "attempt", "args")) {
            if (w.containsKey(key) && !java.util.Objects.equals(w.get(key), g.get(key))) {
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
