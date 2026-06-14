package org.jagentic.core.pipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.Agent;
import org.jagentic.core.AgentContext;
import org.jagentic.core.Brain;
import org.jagentic.core.Event;
import org.jagentic.core.Guardrail;
import org.jagentic.core.RegexGuardrail;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.llm.ChatClient;
import org.jagentic.core.llm.LlmBrain;

/**
 * Compiles a declarative spec (a plain {@code Map}, e.g. parsed from {@code pipeline.yaml})
 * into a {@link RoutedGraph} + {@link ToolRegistry} + retriever — the Java peer of
 * pyagentic's {@code builder}. The same YAML schema builds the agentic system on any
 * language/backend.
 */
public final class GraphBuilder {

  private GraphBuilder() {}

  /** What a build produces. */
  public record Built(RoutedGraph graph, ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {}

  /** Supplies a ChatClient for an {@code llm:} spec (lets the loader choose the provider). */
  @FunctionalInterface
  public interface ChatClientFactory {
    ChatClient create(Map<String, Object> llmSpec);
  }

  private static final ObjectMapper JSON = new ObjectMapper();

  @SuppressWarnings("unchecked")
  public static Built build(Map<String, Object> spec, ChatClientFactory chatClientFactory) {
    Map<String, Object> agent = (Map<String, Object>) spec.getOrDefault("agent", Map.of());
    Map<String, Object> pathSpecs = (Map<String, Object>) agent.get("paths");
    if (pathSpecs == null || pathSpecs.isEmpty()) {
      throw new IllegalArgumentException("pipeline spec needs agent.paths");
    }

    ToolRegistry tools = buildTools((List<Map<String, Object>>) spec.get("tools"));
    Map<String, Object> retrievalSpec = (Map<String, Object>) spec.get("retrieval");
    int dim = retrievalSpec == null ? 256 : ((Number) retrievalSpec.getOrDefault("dim", 256)).intValue();
    Retrieval.TwoTierRetriever retriever = buildRetriever(retrievalSpec, dim);

    Map<String, Agent> paths = new LinkedHashMap<>();
    for (Map.Entry<String, Object> e : pathSpecs.entrySet()) {
      String name = e.getKey();
      Map<String, Object> ps = (Map<String, Object>) e.getValue();
      String prompt = (String) ps.getOrDefault("prompt", "You answer " + name + " questions.");
      String brainKind = (String) ps.getOrDefault("brain", "rule");
      Brain brain;
      if ("llm".equals(brainKind)) {
        if (chatClientFactory == null) {
          throw new IllegalArgumentException("spec uses an llm brain but no ChatClientFactory was provided");
        }
        ChatClient client = chatClientFactory.create((Map<String, Object>) spec.getOrDefault("llm", Map.of()));
        brain = new LlmBrain(client, name, prompt, (List<String>) ps.get("tools"),
            ((Number) ps.getOrDefault("max_iterations", 6)).intValue());
      } else if ("rule".equals(brainKind)) {
        brain = new KeywordBrain(name, dim, (Map<String, String>) ps.get("tool_triggers"),
            ((Number) ps.getOrDefault("threshold", 0.15)).doubleValue());
      } else {
        throw new IllegalArgumentException("unknown brain kind " + brainKind + " for path " + name);
      }
      paths.put(name, new Agent(name, prompt, brain));
    }

    RoutedGraph.Router router = buildRouter((Map<String, Object>) agent.get("router"), new ArrayList<>(paths.keySet()));
    RoutedGraph.Verifier verifier = null;
    Map<String, Object> vspec = (Map<String, Object>) agent.getOrDefault("verifier", Map.of());
    if ("prefix".equals(vspec.getOrDefault("kind", "prefix"))) {
      verifier = (reply, ctx) -> new RoutedGraph.Verifier.Result(reply != null && reply.startsWith("["), reply);
    }

    List<Guardrail> guardrails = new ArrayList<>();
    for (Map<String, Object> g : (List<Map<String, Object>>) spec.getOrDefault("guardrails", List.of())) {
      if ("regex".equals(g.getOrDefault("kind", "regex"))) {
        guardrails.add(new RegexGuardrail((List<String>) g.getOrDefault("deny", List.of()),
            (String) g.getOrDefault("reason", "blocked by policy"),
            Boolean.TRUE.equals(g.get("check_outputs"))));
      }
    }

    RoutedGraph graph = new RoutedGraph(router, paths, verifier, guardrails, List.of());
    return new Built(graph, tools, retriever);
  }

  @SuppressWarnings("unchecked")
  private static ToolRegistry buildTools(List<Map<String, Object>> specs) {
    ToolRegistry reg = new ToolRegistry();
    if (specs == null) {
      return reg;
    }
    for (Map<String, Object> t : specs) {
      String id = (String) t.get("id");
      String kind = (String) t.getOrDefault("kind", "constant");
      String desc = (String) t.getOrDefault("description", id);
      if ("constant".equals(kind)) {
        Object value = t.get("value");
        reg.register(id, desc, params -> value);
      } else if ("http".equals(kind) || "agent".equals(kind)) {
        // "agent" is an alias: call another agent/gateway's /agent endpoint (A2A-as-tool).
        String url = (String) t.get("url");
        reg.register(id, desc, httpTool(url));
      } else {
        throw new IllegalArgumentException("unknown tool kind " + kind + " for " + id);
      }
    }
    return reg;
  }

  private static Function<Map<String, Object>, Object> httpTool(String url) {
    HttpClient http = HttpClient.newHttpClient();
    return params -> {
      try {
        String body = JSON.writeValueAsString(params == null ? Map.of() : params);
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return JSON.readValue(resp.body(), Map.class);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static Retrieval.TwoTierRetriever buildRetriever(Map<String, Object> spec, int dim) {
    Retrieval.InMemoryHotVectorIndex hot = new Retrieval.InMemoryHotVectorIndex();
    if (spec != null) {
      for (Map<String, Object> doc : (List<Map<String, Object>>) spec.getOrDefault("kb", List.of())) {
        String text = (String) doc.get("text");
        hot.upsert((String) doc.get("id"), Retrieval.embed(text, dim), text);
      }
    }
    return new Retrieval.TwoTierRetriever(hot, null, 4, 4);
  }

  @SuppressWarnings("unchecked")
  private static RoutedGraph.Router buildRouter(Map<String, Object> spec, List<String> paths) {
    Map<String, Object> s = spec == null ? Map.of() : spec;
    String kind = (String) s.getOrDefault("kind", "keyword");
    if (!"keyword".equals(kind)) {
      throw new IllegalArgumentException("router kind " + kind + " not supported (use 'keyword')");
    }
    String fallback = (String) s.get("default");
    String defaultPath = fallback != null ? fallback : (paths.isEmpty() ? null : paths.get(paths.size() - 1));
    Map<String, List<String>> rules = (Map<String, List<String>>) s.getOrDefault("rules", Map.of());
    return (Event event, AgentContext ctx) -> {
      String low = event.text().toLowerCase();
      for (Map.Entry<String, List<String>> r : rules.entrySet()) {
        for (String kw : r.getValue()) {
          if (low.contains(kw.toLowerCase())) {
            return r.getKey();
          }
        }
      }
      return defaultPath;
    };
  }
}
