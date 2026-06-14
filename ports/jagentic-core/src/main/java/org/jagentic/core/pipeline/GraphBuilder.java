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
import org.jagentic.core.ContextWindowManager;
import org.jagentic.core.Event;
import org.jagentic.core.Guardrail;
import org.jagentic.core.HnswVectorStore;
import org.jagentic.core.InMemoryVectorStore;
import org.jagentic.core.RegexGuardrail;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.VectorStore;
import org.jagentic.core.embedding.Embedder;
import org.jagentic.core.embedding.Embedders;
import org.jagentic.core.embedding.HashingEmbedder;
import org.jagentic.core.inference.Classifier;
import org.jagentic.core.inference.ClassifierGuardrail;
import org.jagentic.core.inference.EmbeddingClassifier;
import org.jagentic.core.inference.LexiconClassifier;
import org.jagentic.core.llm.ChatClient;
import org.jagentic.core.llm.LlmBrain;
import org.jagentic.core.store.McpStdioClient;

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
    registerMcp(tools, (List<Map<String, Object>>) spec.get("mcp"));
    registerA2A(tools, (List<Map<String, Object>>) spec.get("a2a"));

    Map<String, Object> retrievalSpec = (Map<String, Object>) spec.get("retrieval");
    // Resolve the embed function + dim. An embeddings: section picks a real provider via the
    // Embedder SPI; otherwise the deterministic FNV hashing embedder at the retrieval dim.
    Embedder embedder = buildEmbedder((Map<String, Object>) spec.get("embeddings"), retrievalSpec);
    int dim = embedder.dim();
    Retrieval.TwoTierRetriever retriever = buildRetriever(retrievalSpec, embedder);

    ContextWindowManager contextManager = null;
    Map<String, Object> ctxSpec = (Map<String, Object>) spec.get("context");
    if (ctxSpec != null) {
      int budget = ctxSpec.containsKey("max_tokens")
          ? ((Number) ctxSpec.get("max_tokens")).intValue()
          : ((Number) ctxSpec.getOrDefault("max_items", 12)).intValue() * 64;
      contextManager = new ContextWindowManager(budget);
    }

    org.jagentic.core.skill.SkillRegistry skills =
        org.jagentic.core.skill.SkillRegistry.fromSpecs((List<Map<String, Object>>) spec.get("skills"));

    Map<String, Agent> paths = new LinkedHashMap<>();
    for (Map.Entry<String, Object> e : pathSpecs.entrySet()) {
      String name = e.getKey();
      Map<String, Object> ps = (Map<String, Object>) e.getValue();
      String prompt = (String) ps.getOrDefault("prompt", "You answer " + name + " questions.");
      var expanded = skills.expand((List<String>) ps.get("skills"));
      if (!expanded.promptFragment().isBlank()) {
        prompt = prompt + "\n" + expanded.promptFragment();
      }
      String brainKind = (String) ps.getOrDefault("brain", "rule");
      Brain brain;
      if ("llm".equals(brainKind)) {
        if (chatClientFactory == null) {
          throw new IllegalArgumentException("spec uses an llm brain but no ChatClientFactory was provided");
        }
        ChatClient client = chatClientFactory.create((Map<String, Object>) spec.getOrDefault("llm", Map.of()));
        List<String> pathTools = new ArrayList<>();
        if (ps.get("tools") instanceof List<?> declared) {
          for (Object t : declared) pathTools.add(String.valueOf(t));
        }
        for (String t : expanded.tools()) {
          if (!pathTools.contains(t)) pathTools.add(t);
        }
        LlmBrain lb = new LlmBrain(client, name, prompt, pathTools.isEmpty() ? null : pathTools,
            ((Number) ps.getOrDefault("max_iterations", 6)).intValue());
        if (ps.get("output_schema") instanceof Map<?, ?> os) {
          lb.withOutputSchema((Map<String, Object>) os);
        }
        if (contextManager != null) {
          lb.withContextManager(contextManager);
        }
        brain = lb;
      } else if ("rule".equals(brainKind)) {
        brain = new KeywordBrain(name, embedder, (Map<String, String>) ps.get("tool_triggers"),
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
      guardrails.add(buildGuardrail(g, embedder));
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
        String url = resolveEnv((String) t.get("url"));
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

  /** Resolve the embed function + dim (mirrors pyagentic's {@code _build_embedder}). An
   * {@code embeddings:} section picks a real provider via the Embedder SPI; otherwise the
   * deterministic FNV hashing embedder at the retrieval {@code dim} (default 256). */
  private static Embedder buildEmbedder(Map<String, Object> embSpec, Map<String, Object> retrievalSpec) {
    if (embSpec != null) {
      return Embedders.make(embSpec);
    }
    int dim = retrievalSpec == null ? 256 : ((Number) retrievalSpec.getOrDefault("dim", 256)).intValue();
    return new HashingEmbedder(dim);
  }

  /** Build the two-tier retriever. The hot tier is always an in-memory window seeded with the
   * {@code kb}; a {@code vector_store:} section adds a real cold tier, also seeded so cold
   * recall works. Mirrors pyagentic's {@code _build_retriever}. */
  @SuppressWarnings("unchecked")
  private static Retrieval.TwoTierRetriever buildRetriever(Map<String, Object> spec, Embedder embedder) {
    if (spec == null) {
      return null;
    }
    Retrieval.InMemoryHotVectorIndex hot = new Retrieval.InMemoryHotVectorIndex();
    VectorStore store = buildVectorStore((Map<String, Object>) spec.get("vector_store"), embedder.dim());
    for (Map<String, Object> doc : (List<Map<String, Object>>) spec.getOrDefault("kb", List.of())) {
      String text = (String) doc.get("text");
      float[] vec = embedder.embed(text);
      hot.upsert((String) doc.get("id"), vec, text);
      if (store != null) {
        store.upsert((String) doc.get("id"), vec, text);
      }
    }
    Retrieval.ColdSearch cold = store == null ? null : store::search;
    return new Retrieval.TwoTierRetriever(hot, cold, 4, 4);
  }

  /** Build a cold-tier {@link VectorStore} by kind: memory | hnsw | qdrant. A qdrant failure
   * (no server reachable) degrades to no cold tier rather than failing the build. */
  private static VectorStore buildVectorStore(Map<String, Object> spec, int dim) {
    if (spec == null) {
      return null;
    }
    String kind = String.valueOf(spec.getOrDefault("kind", "memory")).toLowerCase();
    switch (kind) {
      case "memory":
        return new InMemoryVectorStore();
      case "hnsw":
        int m = ((Number) spec.getOrDefault("m", 16)).intValue();
        int efC = ((Number) spec.getOrDefault("ef_construction", 200)).intValue();
        int efS = ((Number) spec.getOrDefault("ef_search", 50)).intValue();
        long seed = ((Number) spec.getOrDefault("seed", 42)).longValue();
        return new HnswVectorStore(m, efC, efS, seed);
      case "qdrant":
        try {
          String url = resolveEnv(String.valueOf(spec.getOrDefault("url", "http://localhost:6333")));
          String collection = String.valueOf(spec.getOrDefault("collection", "agentic"));
          return new org.jagentic.core.store.QdrantVectorStore(url, collection, dim);
        } catch (RuntimeException e) {
          return null; // no reachable server: skip the cold tier
        }
      default:
        throw new IllegalArgumentException("unknown vector_store kind " + kind + "; choose memory|hnsw|qdrant");
    }
  }

  /** Build one guardrail from its spec (mirrors pyagentic's {@code _build_guardrail}).
   * kind = regex (default) | classifier (lexicon | embedding). */
  @SuppressWarnings("unchecked")
  private static Guardrail buildGuardrail(Map<String, Object> g, Embedder embedder) {
    String kind = String.valueOf(g.getOrDefault("kind", "regex"));
    if ("regex".equals(kind)) {
      return new RegexGuardrail((List<String>) g.getOrDefault("deny", List.of()),
          (String) g.getOrDefault("reason", "blocked by policy"),
          Boolean.TRUE.equals(g.get("check_outputs")));
    }
    if ("classifier".equals(kind)) {
      String ctype = String.valueOf(g.getOrDefault("classifier", "lexicon")).toLowerCase();
      Classifier clf;
      if ("lexicon".equals(ctype)) {
        clf = new LexiconClassifier((Map<String, List<String>>) g.get("lexicon"),
            (String) g.getOrDefault("default_label", "other"));
      } else if ("embedding".equals(ctype)) {
        clf = new EmbeddingClassifier(embedder, 10.0).fit((Map<String, List<String>>) g.get("examples"));
      } else {
        throw new IllegalArgumentException("unknown classifier " + ctype + "; choose lexicon|embedding");
      }
      return new ClassifierGuardrail(clf, (List<String>) g.getOrDefault("blocked", List.of()),
          ((Number) g.getOrDefault("threshold", 0.5)).doubleValue(),
          (String) g.getOrDefault("reason", "blocked by classifier policy"),
          Boolean.TRUE.equals(g.get("check_outputs")));
    }
    throw new IllegalArgumentException("unknown guardrail kind " + kind + "; choose regex|classifier");
  }

  /** Connect to each declared MCP server (stdio transport only) and register its tools
   * (id-prefixed by name). Mirrors pyagentic's {@code _register_mcp}. */
  @SuppressWarnings("unchecked")
  private static void registerMcp(ToolRegistry tools, List<Map<String, Object>> specs) {
    if (specs == null) {
      return;
    }
    for (Map<String, Object> m : specs) {
      String transport = String.valueOf(m.getOrDefault("transport", "stdio")).toLowerCase();
      if (!"stdio".equals(transport)) {
        throw new IllegalArgumentException("mcp transport " + transport + " not supported (use 'stdio')");
      }
      Object raw = m.get("command");
      List<String> command = new ArrayList<>();
      if (raw instanceof List<?> parts) {
        for (Object c : parts) command.add(resolveEnv(String.valueOf(c)));
      } else {
        command.add(resolveEnv(String.valueOf(raw)));
        for (Object a : (List<Object>) m.getOrDefault("args", List.of())) {
          command.add(resolveEnv(String.valueOf(a)));
        }
      }
      try {
        McpStdioClient client = new McpStdioClient(command);
        client.register(tools, m.getOrDefault("name", "mcp") + "_");
      } catch (java.io.IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }
  }

  /** Register each declared peer agent as a tool (peer-as-tool over A2A HTTP). Mirrors
   * pyagentic's {@code _register_a2a}. */
  private static void registerA2A(ToolRegistry tools, List<Map<String, Object>> specs) {
    if (specs == null) {
      return;
    }
    for (Map<String, Object> a : specs) {
      String url = resolveEnv((String) a.get("url"));
      String id = (String) a.get("id");
      tools.register(id, (String) a.getOrDefault("description", "Delegate to peer agent " + id),
          org.jagentic.core.A2AClient.peerTool(url, ((Number) a.getOrDefault("retries", 2)).intValue()));
    }
  }

  /** Expand a {@code ${ENV}} connection link (used for agent/http/mcp/a2a URLs). */
  static String resolveEnv(String value) {
    if (value != null && value.startsWith("${") && value.endsWith("}")) {
      String v = System.getenv(value.substring(2, value.length() - 1));
      return v == null ? "" : v;
    }
    return value;
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
