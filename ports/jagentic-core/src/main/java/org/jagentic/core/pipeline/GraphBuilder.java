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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.jagentic.core.Agent;
import org.jagentic.core.AgentContext;
import org.jagentic.core.Brain;
import org.jagentic.core.ContextWindow;
import org.jagentic.core.ContextWindowManager;
import org.jagentic.core.Event;
import org.jagentic.core.Guardrail;
import org.jagentic.core.HnswVectorStore;
import org.jagentic.core.InMemoryVectorStore;
import org.jagentic.core.Policies;
import org.jagentic.core.RegexGuardrail;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.Runtime;
import org.jagentic.core.SagaPlan;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TurnResult;
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
import org.jagentic.core.llm.ScriptedChatClient;
import org.jagentic.core.store.McpStdioClient;

/**
 * Compiles a declarative spec (a plain {@code Map}, e.g. parsed from {@code pipeline.yaml})
 * into a {@link RoutedGraph} + {@link ToolRegistry} + retriever — the Java peer of
 * pyagentic's {@code builder}. The same YAML schema builds the agentic system on any
 * language/backend.
 */
public final class GraphBuilder {

  private GraphBuilder() {}

  /**
   * What a build produces. {@code degradations} lists every configured store that fell back to
   * memory under {@code on_unavailable: degrade}; empty when everything connected.
   */
  public record Built(RoutedGraph graph, ToolRegistry tools, Retrieval.TwoTierRetriever retriever,
                      List<String> degradations) {
    public Built(RoutedGraph graph, ToolRegistry tools, Retrieval.TwoTierRetriever retriever) {
      this(graph, tools, retriever, List.of());
    }

    public Built {
      degradations = degradations == null ? List.of() : List.copyOf(degradations);
    }
  }

  /** Tool ids that {@code kind: failing} tools count attempts under; shared across turns like the reference. */
  public static final String FAIL_ATTEMPTS_KEY = "x-fail-attempts";

  /**
   * Supplies a ChatClient for an {@code llm:} spec (lets the loader choose the provider). The
   * spec's deterministic {@code provider: stub} needs no factory: {@link #build} resolves it to a
   * {@link ScriptedChatClient} itself, so the factory is only consulted for real providers.
   */
  @FunctionalInterface
  public interface ChatClientFactory {
    ChatClient create(Map<String, Object> llmSpec);
  }

  /** True when {@code spec} has an {@code llm} brain whose provider is the scripted {@code stub}. */
  @SuppressWarnings("unchecked")
  public static boolean usesScriptedLlm(Map<String, Object> spec) {
    return spec.get("llm") instanceof Map<?, ?> llm && ScriptedChatClient.accepts((Map<String, Object>) llm);
  }

  private static final ObjectMapper JSON = new ObjectMapper();

  public static Built build(Map<String, Object> spec, ChatClientFactory chatClientFactory) {
    return build(spec, chatClientFactory, Map.of());
  }

  /**
   * @param inprocPeers runtimes reachable in-process for {@code a2a} peers with {@code transport:
   *     inproc}, keyed by peer name. A peer with no runtime bound is answered by the spec's
   *     acknowledging stand-in ({@code "[name] delegated"}), exactly like the reference runtime.
   */
  @SuppressWarnings("unchecked")
  public static Built build(Map<String, Object> spec, ChatClientFactory chatClientFactory,
                            Map<String, Runtime> inprocPeers) {
    WorkflowValidator.validate(spec);
    StoreAvailability availability = new StoreAvailability();
    Map<String, Object> agent = (Map<String, Object>) spec.get("agent");
    Map<String, Object> pathSpecs = (Map<String, Object>) agent.get("paths");

    ToolRegistry tools = buildTools((List<Map<String, Object>>) spec.get("tools"));
    registerMcp(tools, (List<Map<String, Object>>) spec.get("mcp"));
    registerA2A(tools, (List<Map<String, Object>>) spec.get("a2a"), inprocPeers);
    Policies policies = Policies.fromMap((Map<String, Object>) spec.get("policies"));
    SagaPlan saga = SagaPlan.fromMap((Map<String, Object>) spec.get("saga"));
    if (saga != null) {
      saga = saga.resolve(compensations((List<Map<String, Object>>) spec.get("tools")));
    }

    Map<String, Object> retrievalSpec = (Map<String, Object>) spec.get("retrieval");
    // Resolve the embed function + dim. An embeddings: section picks a real provider via the
    // Embedder SPI; otherwise the deterministic FNV hashing embedder at the retrieval dim.
    Embedder embedder = buildEmbedder((Map<String, Object>) spec.get("embeddings"), retrievalSpec);
    int dim = embedder.dim();
    Retrieval.TwoTierRetriever retriever = buildRetriever(retrievalSpec, embedder, availability);

    ContextWindowManager contextManager = null;
    Map<String, Object> ctxSpec = (Map<String, Object>) spec.get("context");
    ContextWindow contextWindow = ContextWindow.fromMap(ctxSpec);
    // compaction: window bounds the transcript by message count; the MoSCoW token budget applies
    // on top of it only when max_tokens is declared.
    if (ctxSpec != null && (!contextWindow.bounded() || ctxSpec.containsKey("max_tokens"))) {
      int budget = ctxSpec.containsKey("max_tokens")
          ? ((Number) ctxSpec.get("max_tokens")).intValue()
          : ((Number) ctxSpec.getOrDefault("max_items", 12)).intValue() * 64;
      contextManager = new ContextWindowManager(budget);
    }

    org.jagentic.core.skill.SkillRegistry skills =
        org.jagentic.core.skill.SkillRegistry.fromSpecs((List<Map<String, Object>>) spec.get("skills"));

    int topK = retrievalSpec == null ? 4 : ((Number) retrievalSpec.getOrDefault("top_k", 4)).intValue();
    Map<String, Agent> paths = new LinkedHashMap<>();
    Map<String, String> suspendUntil = new LinkedHashMap<>();
    Map<String, RoutedGraph.Verifier> pathVerifiers = new LinkedHashMap<>();
    for (Map.Entry<String, Object> e : pathSpecs.entrySet()) {
      String name = e.getKey();
      Map<String, Object> ps = (Map<String, Object>) e.getValue();
      if (ps.get(RoutedGraph.SUSPEND_UNTIL) != null) {
        suspendUntil.put(name, String.valueOf(ps.get(RoutedGraph.SUSPEND_UNTIL)));
      }
      if (ps.get("verifier") != null) {
        RoutedGraph.Verifier own = buildVerifier((Map<String, Object>) ps.get("verifier"),
            "agent.paths." + name + ".verifier");
        pathVerifiers.put(name, own == null ? RoutedGraph.Verifier.ACCEPT : own);
      }
      String prompt = (String) ps.getOrDefault("prompt", "You answer " + name + " questions.");
      var expanded = skills.expand((List<String>) ps.get("skills"));
      if (!expanded.promptFragment().isBlank()) {
        prompt = prompt + "\n" + expanded.promptFragment();
      }
      String brainKind = (String) ps.getOrDefault("brain", "rule");
      Brain brain;
      if ("llm".equals(brainKind)) {
        Map<String, Object> llmSpec = (Map<String, Object>) spec.getOrDefault("llm", Map.of());
        boolean scripted = ScriptedChatClient.accepts(llmSpec);
        if (!scripted && chatClientFactory == null) {
          throw new IllegalArgumentException("spec uses an llm brain but no ChatClientFactory was provided");
        }
        ChatClient client = scripted ? ScriptedChatClient.fromSpec(llmSpec) : chatClientFactory.create(llmSpec);
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
        if (scripted) {
          lb.withVerbatimReply().withStrictTools();
        }
        brain = lb;
      } else if ("rule".equals(brainKind)) {
        brain = new KeywordBrain(name, embedder, (Map<String, String>) ps.get("tool_triggers"),
            ((Number) ps.getOrDefault("threshold", 0.15)).doubleValue(), topK);
      } else {
        throw new IllegalArgumentException("unknown brain kind " + brainKind + " for path " + name);
      }
      paths.put(name, new Agent(name, prompt, brain));
    }

    RoutedGraph.Router router = buildRouter((Map<String, Object>) agent.get("router"), new ArrayList<>(paths.keySet()));
    RoutedGraph.Verifier verifier = buildVerifier((Map<String, Object>) agent.get("verifier"), "agent.verifier");

    List<Guardrail> guardrails = new ArrayList<>();
    for (Map<String, Object> g : (List<Map<String, Object>>) spec.getOrDefault("guardrails", List.of())) {
      guardrails.add(buildGuardrail(g, embedder));
    }

    RoutedGraph graph = new RoutedGraph(router, paths, verifier, pathVerifiers, guardrails, List.of(), policies,
        saga, suspendUntil, contextWindow);
    return new Built(graph, tools, retriever, availability.degradations());
  }

  /**
   * kind = prefix (default) | regex | none; {@code none} is returned as null. {@code schema} and
   * {@code llm} verifiers are not implemented. {@code where} is the spec location for error messages
   * ({@code agent.verifier} or {@code agent.paths.<name>.verifier}).
   */
  static RoutedGraph.Verifier buildVerifier(Map<String, Object> vspec, String where) {
    Map<String, Object> v = vspec == null ? Map.of() : vspec;
    String kind = String.valueOf(v.getOrDefault("kind", "prefix"));
    switch (kind) {
      case "prefix":
        return (reply, ctx) -> new RoutedGraph.Verifier.Result(reply != null && reply.startsWith("["), reply);
      case "none":
        return null;
      case "regex":
        Object pattern = v.get("pattern");
        if (pattern == null) {
          throw new WorkflowValidator.WorkflowValidationException(where + ".pattern",
              "regex verifier needs a pattern");
        }
        Pattern p = Pattern.compile(String.valueOf(pattern));
        return (reply, ctx) -> new RoutedGraph.Verifier.Result(reply != null && p.matcher(reply).find(), reply);
      default:
        throw new WorkflowValidator.WorkflowValidationException(where + ".kind",
            "verifier kind " + kind + " is not implemented by this runtime (prefix|regex|none)");
    }
  }

  private static Map<String, String> compensations(List<Map<String, Object>> specs) {
    Map<String, String> out = new LinkedHashMap<>();
    for (Map<String, Object> t : specs == null ? List.<Map<String, Object>>of() : specs) {
      if (t.get("compensation") != null) {
        out.put(String.valueOf(t.get("id")), String.valueOf(t.get("compensation")));
      }
    }
    return out;
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
      Map<String, Object> parameters = (Map<String, Object>) t.get("parameters");
      if ("constant".equals(kind)) {
        Object value = t.get("value");
        reg.register(id, desc, parameters, params -> value);
      } else if ("http".equals(kind) || "agent".equals(kind)) {
        // "agent" is an alias: call another agent/gateway's /agent endpoint (A2A-as-tool).
        String url = resolveEnv((String) t.get("url"));
        if (url == null || url.isBlank()) {
          throw new WorkflowValidator.WorkflowValidationException("tools[" + id + "].url",
              kind + " tools need a url");
        }
        reg.register(id, desc, parameters, httpTool(url));
      } else if ("failing".equals(kind)) {
        reg.register(id, desc, parameters, failingTool(id, t));
      } else {
        throw new WorkflowValidator.WorkflowValidationException("tools[" + id + "].kind",
            "tool kind " + kind + " cannot be bound from a document by this runtime"
                + " (constant|http|agent|failing)");
      }
    }
    return reg;
  }

  /**
   * The spec's deterministic failure tool: raises on every call, or on the first
   * {@code x-fail-attempts} calls and then returns {@code value}. Attempts are counted per tool
   * across turns, matching the reference runtime.
   */
  private static Function<Map<String, Object>, Object> failingTool(String id, Map<String, Object> t) {
    Object budget = t.get(FAIL_ATTEMPTS_KEY);
    int failFirst = budget == null ? Integer.MAX_VALUE : ((Number) budget).intValue();
    Object value = t.get("value");
    AtomicInteger seen = new AtomicInteger();
    return params -> {
      if (seen.incrementAndGet() <= failFirst) {
        throw new IllegalStateException(id + " failed");
      }
      return value;
    };
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
   * recall works. The hot window fetches at least {@code top_k} so ranking is stable. */
  @SuppressWarnings("unchecked")
  private static Retrieval.TwoTierRetriever buildRetriever(Map<String, Object> spec, Embedder embedder,
                                                          StoreAvailability availability) {
    if (spec == null) {
      return null;
    }
    Retrieval.InMemoryHotVectorIndex hot = new Retrieval.InMemoryHotVectorIndex();
    VectorStore store = buildVectorStore((Map<String, Object>) spec.get("vector_store"), embedder.dim(),
        availability);
    for (Map<String, Object> doc : (List<Map<String, Object>>) spec.getOrDefault("kb", List.of())) {
      String text = (String) doc.get("text");
      float[] vec = embedder.embed(text);
      hot.upsert((String) doc.get("id"), vec, text);
      if (store != null) {
        store.upsert((String) doc.get("id"), vec, text);
      }
    }
    Retrieval.ColdSearch cold = store == null ? null : store::search;
    int k = Math.max(4, ((Number) spec.getOrDefault("top_k", 4)).intValue());
    return new Retrieval.TwoTierRetriever(hot, cold, k, k);
  }

  /** Build a cold-tier {@link VectorStore} by kind: memory | hnsw | qdrant. An unreachable qdrant
   * fails the build unless the section sets {@code on_unavailable: degrade}, which drops the cold
   * tier (spec rule 7: no silent substitution). */
  private static VectorStore buildVectorStore(Map<String, Object> spec, int dim,
                                             StoreAvailability availability) {
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
        String url = resolveEnv(String.valueOf(spec.getOrDefault("url", "http://localhost:6333")));
        String collection = String.valueOf(spec.getOrDefault("collection", "agentic"));
        return availability.connect("retrieval.vector_store", spec,
            () -> new org.jagentic.core.store.QdrantVectorStore(url, collection, dim), () -> null);
      default:
        throw new IllegalArgumentException("unknown vector_store kind " + kind + "; choose memory|hnsw|qdrant");
    }
  }

  /** Build one guardrail from its spec (mirrors pyagentic's {@code _build_guardrail}).
   * kind = regex (default) | classifier (lexicon | embedding); {@code stage} = input (default) |
   * output | both. */
  @SuppressWarnings("unchecked")
  private static Guardrail buildGuardrail(Map<String, Object> g, Embedder embedder) {
    String kind = String.valueOf(g.getOrDefault("kind", "regex"));
    String stage = String.valueOf(g.getOrDefault("stage", "input"));
    boolean input = "input".equals(stage) || "both".equals(stage);
    boolean output = "output".equals(stage) || "both".equals(stage);
    if (!input && !output) {
      throw new WorkflowValidator.WorkflowValidationException("guardrails.stage",
          "must be input, output or both, got " + stage);
    }
    Guardrail rail = buildGuardrailOfKind(g, kind, embedder, output);
    return input ? rail : new Guardrail() {
      @Override
      public String checkOutput(String reply) {
        return rail.checkOutput(reply);
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static Guardrail buildGuardrailOfKind(Map<String, Object> g, String kind, Embedder embedder,
                                                boolean checkOutputs) {
    if ("regex".equals(kind)) {
      return new RegexGuardrail((List<String>) g.getOrDefault("deny", List.of()),
          (String) g.getOrDefault("reason", "blocked by policy"), checkOutputs);
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
          (String) g.getOrDefault("reason", "blocked by classifier policy"), checkOutputs);
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

  /**
   * Register each declared peer agent as a tool. {@code transport: http} (default) delegates over
   * A2A HTTP; {@code inproc} submits to a runtime from {@code inprocPeers} (or the acknowledging
   * stand-in when none is bound); {@code grpc} is not implemented and fails the build.
   */
  private static void registerA2A(ToolRegistry tools, List<Map<String, Object>> specs,
                                  Map<String, Runtime> inprocPeers) {
    if (specs == null) {
      return;
    }
    for (Map<String, Object> a : specs) {
      String name = String.valueOf(a.get("name") != null ? a.get("name") : a.get("id"));
      String desc = (String) a.getOrDefault("description", "Delegate to peer agent " + name);
      String transport = String.valueOf(a.getOrDefault("transport", "http"));
      switch (transport) {
        case "http":
          String url = resolveEnv((String) a.get("url"));
          if (url == null || url.isBlank()) {
            throw new WorkflowValidator.WorkflowValidationException("a2a[" + name + "].url",
                "http peers need a url");
          }
          tools.registerPeer(name, desc,
              org.jagentic.core.A2AClient.peerTool(url, ((Number) a.getOrDefault("retries", 2)).intValue()));
          break;
        case "inproc":
          Runtime peer = inprocPeers == null ? null : inprocPeers.get(name);
          tools.registerPeer(name, desc, peer == null ? params -> "[" + name + "] delegated"
              : inprocPeer(name, peer));
          break;
        default:
          throw new WorkflowValidator.WorkflowValidationException("a2a[" + name + "].transport",
              "transport " + transport + " is not implemented by this runtime (http|inproc)");
      }
    }
  }

  /**
   * Delegation to an in-process peer: the peer sees a turn keyed by the caller's conversation and
   * a turn id derived from the caller's ({@code <turn_id>/<peer>}), so redelivery is idempotent on
   * the peer too.
   */
  private static Function<Map<String, Object>, Object> inprocPeer(String name, Runtime peer) {
    return params -> {
      String conversationId = String.valueOf(params.getOrDefault("conversation_id", "a2a"));
      String turnId = params.get("turn_id") == null ? null : params.get("turn_id") + "/" + name;
      TurnResult r = peer.submit(Event.turn(conversationId, turnId,
          String.valueOf(params.getOrDefault("user", params.getOrDefault("user_id", "anonymous"))),
          String.valueOf(params.getOrDefault("text", ""))));
      return r.reply;
    };
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
    String defaultPath = (String) s.get("default");
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
