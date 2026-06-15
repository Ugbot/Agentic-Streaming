package org.jagentic.core.cep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TurnResult;
import org.jagentic.core.pipeline.PipelineLoader;

/** Phase B: the declarative {@code cep:} weave. A spec compiles to a wiring that fires its action on
 * a match (recursion-guarded), and the shared incident.yaml escalates through the loaded pipeline. */
class CepSpecTest {

  private static Map<String, Object> incidentRule(Map<String, Object> onMatch) {
    return Map.of(
        "name", "incident", "key", "conversation_id", "ts", "metadata.ts", "within", 300000,
        "pattern", List.of(
            Map.of("stage", "first", "where", Map.of("text_contains", "anomaly")),
            Map.of("stage", "second", "where", Map.of("text_contains", "anomaly"), "contiguity", "followedBy"),
            Map.of("stage", "third", "where", Map.of("text_contains", "anomaly"), "contiguity", "followedBy")),
        "on_match", onMatch);
  }

  private static Event anomaly(String host, long ts) {
    return new Event(host, "monitor", "anomaly cpu", Map.of("ts", Long.toString(ts)));
  }

  @Test
  void submitActionFiresOnceAndDoesNotRecurse() {
    List<CepWiring> wirings = CepSpec.compile(
        List.of(incidentRule(Map.of("kind", "submit", "text", "incident on {key}"))));
    CepWiring w = wirings.get(0);

    List<Event> fired = new ArrayList<>();
    Runtime recording = e -> {
      fired.add(e);
      return new TurnResult(e.conversationId(), "ok", List.of());
    };
    ToolRegistry tools = new ToolRegistry();

    w.onEvent(anomaly("h1", 0), recording, tools);
    w.onEvent(anomaly("h1", 60_000), recording, tools);
    assertTrue(fired.isEmpty(), "only 2 anomalies so far");

    w.onEvent(anomaly("h1", 120_000), recording, tools);
    assertEquals(1, fired.size(), "3rd anomaly fires the escalation");
    assertEquals("incident on h1", fired.get(0).text());
    assertTrue(fired.get(0).metadata().containsKey(CepWiring.DERIVED));

    // re-feeding the derived event must NOT match again (recursion guard)
    w.onEvent(fired.get(0), recording, tools);
    assertEquals(1, fired.size());
  }

  @Test
  void toolActionCallsTheRegisteredTool() {
    List<CepWiring> wirings = CepSpec.compile(
        List.of(incidentRule(Map.of("kind", "tool", "tool", "open_ticket", "args", Map.of("sev", "high")))));
    List<Map<String, Object>> calls = new ArrayList<>();
    ToolRegistry tools = new ToolRegistry();
    tools.register("open_ticket", "open a ticket", params -> {
      calls.add(params);
      return "TICKET-1";
    });
    Runtime noop = e -> new TurnResult(e.conversationId(), "ok", List.of());

    CepWiring w = wirings.get(0);
    w.onEvent(anomaly("h1", 0), noop, tools);
    w.onEvent(anomaly("h1", 1), noop, tools);
    w.onEvent(anomaly("h1", 2), noop, tools);
    assertEquals(1, calls.size(), "tool fired once on the match");
    assertEquals("high", calls.get(0).get("sev"));
  }

  @Test
  void withinExpiryAndKeyIndependence() {
    CepWiring w = CepSpec.compile(
        List.of(incidentRule(Map.of("kind", "submit", "text", "x")))).get(0);
    List<Event> fired = new ArrayList<>();
    Runtime rec = e -> { fired.add(e); return new TurnResult(e.conversationId(), "ok", List.of()); };
    ToolRegistry tools = new ToolRegistry();

    w.onEvent(anomaly("h1", 0), rec, tools);
    w.onEvent(anomaly("h2", 0), rec, tools);          // different host
    w.onEvent(anomaly("h1", 60_000), rec, tools);
    w.onEvent(anomaly("h1", 400_000), rec, tools);    // beyond 5 min from the first → expired
    assertTrue(fired.isEmpty(), "no host reached 3 within the window");
  }

  @Test
  void conditionMiniLanguage() {
    // metadata_gt
    Condition gt = CepSpec.condition(Map.of("metadata_gt", Map.of("score", 0.9)));
    assertTrue(gt.test(new Event("k", "u", "x", Map.of("score", "0.95")), List.of()));
    assertFalse(gt.test(new Event("k", "u", "x", Map.of("score", "0.5")), List.of()));
    // metadata_equals
    Condition eq = CepSpec.condition(Map.of("metadata_equals", Map.of("sev", "high")));
    assertTrue(eq.test(new Event("k", "u", "x", Map.of("sev", "high")), List.of()));
    assertFalse(eq.test(new Event("k", "u", "x", Map.of("sev", "low")), List.of()));
  }

  @Test
  void incidentYamlEscalatesThroughTheLoadedPipeline() {
    Path yaml = Path.of("../../examples/pipelines/incident.yaml");
    Assumptions.assumeTrue(Files.exists(yaml), "incident.yaml not found");
    PipelineLoader.PipelineSystem sys = PipelineLoader.load(yaml, "local");

    for (long ts : new long[] {0, 60_000, 120_000}) {
      sys.submit(new Event("host-7", "monitor", "anomaly: cpu high", Map.of("ts", Long.toString(ts))));
    }
    // the 3rd anomaly fired a derived "incident ..." event that routed to the escalate path,
    // recorded as the last path attribute for that host.
    assertEquals("escalate",
        sys.conversation.getAttribute("host-7", org.jagentic.core.RoutedGraph.PATH_ATTR).orElse("<none>"));
  }
}
