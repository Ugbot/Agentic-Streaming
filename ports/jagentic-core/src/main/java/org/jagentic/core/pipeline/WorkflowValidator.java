package org.jagentic.core.pipeline;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Structural and semantic validation of a workflow document against {@code spec/v1}: the closed key
 * sets of {@code workflow.schema.json} (unknown keys are rejected except under the {@code x-} and
 * top-level {@code runtime:} extension points) and the seven cross-reference rules of
 * {@code spec/README.md}. Every failure is a {@link WorkflowValidationException} whose message
 * starts with the offending key path.
 */
public final class WorkflowValidator {

  /** A {@code validation}-class error: the document is not executable by this runtime. */
  public static final class WorkflowValidationException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final String path;

    public WorkflowValidationException(String path, String message) {
      super(path + ": " + message);
      this.path = path;
    }

    public String path() {
      return path;
    }
  }

  public static final String SPEC_VERSION = "agentic/v1";

  private static final Set<String> TOP = Set.of("spec_version", "backend", "backend_config", "agent",
      "policies", "tools", "skills", "guardrails", "llm", "embeddings", "retrieval", "context", "stores",
      "mcp", "a2a", "saga", "timers", "cep", "channels", "runtime");
  private static final Set<String> AGENT = Set.of("id", "router", "paths", "verifier");
  private static final Set<String> ROUTER = Set.of("kind", "default", "rules", "prompt", "classifier",
      "threshold");
  private static final Set<String> PATH = Set.of("brain", "prompt", "tools", "tool_triggers", "skills",
      "guardrails", "verifier", "max_iterations", "threshold", "output_schema");
  private static final Set<String> VERIFIER = Set.of("kind", "pattern", "schema", "prompt");
  private static final Set<String> POLICIES = Set.of("ordering", "idempotency", "retry", "verification",
      "on_tool_error");
  private static final Set<String> RETRY = Set.of("kind", "max_attempts", "initial_delay_ms", "multiplier",
      "max_delay_ms", "jitter");
  private static final Set<String> VERIFICATION = Set.of("max_attempts", "on_exhausted");
  private static final Set<String> TOOL = Set.of("id", "kind", "description", "value", "url", "method",
      "timeout_ms", "parameters", "compensation");
  private static final Set<String> SKILL = Set.of("name", "prompt", "tools", "facts");
  private static final Set<String> GUARDRAIL = Set.of("name", "kind", "stage", "deny", "allow", "reason",
      "classifier", "lexicon", "blocked", "threshold");
  private static final Set<String> LLM = Set.of("provider", "model", "base_url", "temperature",
      "max_tokens", "script");
  private static final Set<String> LLM_SCRIPT_STEP = Set.of("text", "tool", "args");
  private static final Set<String> EMBEDDINGS = Set.of("provider", "model", "dim", "base_url");
  private static final Set<String> RETRIEVAL = Set.of("dim", "top_k", "embedder", "kb", "vector_store");
  private static final Set<String> KB_DOC = Set.of("id", "text", "metadata");
  private static final Set<String> CONTEXT = Set.of("max_items", "max_tokens", "compaction");
  private static final Set<String> STORES = Set.of("conversation", "keyed_state", "long_term", "vector");
  private static final Set<String> PEER = Set.of("name", "id", "url", "transport", "description", "retries",
      "timeout_ms");
  private static final Set<String> SAGA = Set.of("steps");
  private static final Set<String> SAGA_STEP = Set.of("name", "tool", "args", "compensate_with");
  private static final Set<String> TIMER = Set.of("id", "after_ms", "clock", "tool", "payload");
  private static final Set<String> CHANNEL = Set.of("id", "kind", "direction", "config");

  private WorkflowValidator() {}

  /** Validates and returns the document unchanged; throws {@link WorkflowValidationException}. */
  public static Map<String, Object> validate(Map<String, Object> spec) {
    if (spec == null) {
      throw new WorkflowValidationException("$", "workflow document is empty");
    }
    Object version = spec.get("spec_version");
    if (version != null && !SPEC_VERSION.equals(version)) {
      throw new WorkflowValidationException("spec_version",
          "unsupported spec_version " + version + " (this runtime implements " + SPEC_VERSION + ")");
    }
    keys("$", spec, TOP, true);
    Map<String, Object> agent = requireMap("agent", spec.get("agent"));
    keys("agent", agent, AGENT, true);
    Map<String, Object> paths = requireMap("agent.paths", agent.get("paths"));
    if (paths.isEmpty()) {
      throw new WorkflowValidationException("agent.paths", "at least one path is required");
    }
    for (Map.Entry<String, Object> e : paths.entrySet()) {
      String p = "agent.paths." + e.getKey();
      Map<String, Object> path = requireMap(p, e.getValue());
      keys(p, path, PATH, true);
      if (path.get("verifier") != null) {
        keys(p + ".verifier", requireMap(p + ".verifier", path.get("verifier")), VERIFIER, true);
      }
    }
    Map<String, Object> router = optionalMap("agent.router", agent.get("router"));
    keys("agent.router", router, ROUTER, true);
    keys("agent.verifier", optionalMap("agent.verifier", agent.get("verifier")), VERIFIER, true);

    Map<String, Object> policies = optionalMap("policies", spec.get("policies"));
    keys("policies", policies, POLICIES, true);
    keys("policies.retry", optionalMap("policies.retry", policies.get("retry")), RETRY, false);
    keys("policies.verification", optionalMap("policies.verification", policies.get("verification")),
        VERIFICATION, false);

    Set<String> toolIds = new LinkedHashSet<>();
    Map<String, String> compensations = new LinkedHashMap<>();
    int i = 0;
    for (Map<String, Object> t : listOfMaps("tools", spec.get("tools"))) {
      String p = "tools[" + i++ + "]";
      keys(p, t, TOOL, true);
      String id = requireString(p + ".id", t.get("id"));
      if (!toolIds.add(id)) {
        throw new WorkflowValidationException(p + ".id", "duplicate tool id " + id);
      }
      if (t.get("parameters") != null) {
        requireMap(p + ".parameters", t.get("parameters"));
      }
      if (t.get("compensation") != null) {
        compensations.put(id, requireString(p + ".compensation", t.get("compensation")));
      }
    }
    i = 0;
    for (Map<String, Object> m : listOfMaps("mcp", spec.get("mcp"))) {
      requireMap("mcp[" + i++ + "]", m);
    }
    i = 0;
    for (Map<String, Object> a : listOfMaps("a2a", spec.get("a2a"))) {
      String p = "a2a[" + i++ + "]";
      keys(p, a, PEER, false);
      Object name = a.get("name") != null ? a.get("name") : a.get("id");
      if (name == null) {
        throw new WorkflowValidationException(p, "a peer needs a name");
      }
      if (!toolIds.add(String.valueOf(name))) {
        throw new WorkflowValidationException(p, "peer " + name + " collides with a tool id");
      }
    }
    i = 0;
    Map<String, Map<String, Object>> skills = new LinkedHashMap<>();
    for (Map<String, Object> s : listOfMaps("skills", spec.get("skills"))) {
      String p = "skills[" + i++ + "]";
      keys(p, s, SKILL, false);
      skills.put(requireString(p + ".name", s.get("name")), s);
      for (Object t : listOf(p + ".tools", s.get("tools"))) {
        requireTool(p + ".tools", String.valueOf(t), toolIds, spec);
      }
    }
    i = 0;
    for (Map<String, Object> g : listOfMaps("guardrails", spec.get("guardrails"))) {
      keys("guardrails[" + i++ + "]", g, GUARDRAIL, true);
    }
    Map<String, Object> llm = optionalMap("llm", spec.get("llm"));
    keys("llm", llm, LLM, true);
    i = 0;
    for (Map<String, Object> step : listOfMaps("llm.script", llm.get("script"))) {
      keys("llm.script[" + i++ + "]", step, LLM_SCRIPT_STEP, false);
    }
    Map<String, Object> embeddings = optionalMap("embeddings", spec.get("embeddings"));
    keys("embeddings", embeddings, EMBEDDINGS, false);
    Map<String, Object> retrieval = optionalMap("retrieval", spec.get("retrieval"));
    keys("retrieval", retrieval, RETRIEVAL, false);
    i = 0;
    for (Map<String, Object> doc : listOfMaps("retrieval.kb", retrieval.get("kb"))) {
      keys("retrieval.kb[" + i++ + "]", doc, KB_DOC, false);
    }
    keys("context", optionalMap("context", spec.get("context")), CONTEXT, false);
    Map<String, Object> stores = optionalMap("stores", spec.get("stores"));
    keys("stores", stores, STORES, false);
    for (Map.Entry<String, Object> e : stores.entrySet()) {
      Map<String, Object> st = requireMap("stores." + e.getKey(), e.getValue());
      Object on = st.get("on_unavailable");
      if (on != null && !"fail".equals(on) && !"degrade".equals(on)) {
        throw new WorkflowValidationException("stores." + e.getKey() + ".on_unavailable",
            "must be fail or degrade, got " + on);
      }
    }
    i = 0;
    for (Map<String, Object> t : listOfMaps("timers", spec.get("timers"))) {
      String path = "timers[" + i++ + "]";
      keys(path, t, TIMER, false);
      Object clock = t.get("clock");
      if (clock != null && !"processing".equals(clock) && !"event".equals(clock)) {
        throw new WorkflowValidationException(path + ".clock", "must be processing or event, got " + clock);
      }
    }
    i = 0;
    for (Map<String, Object> c : listOfMaps("channels", spec.get("channels"))) {
      keys("channels[" + i++ + "]", c, CHANNEL, false);
    }

    // Rule 1: router targets exist.
    Object dflt = router.get("default");
    if (dflt != null && !paths.containsKey(String.valueOf(dflt))) {
      throw new WorkflowValidationException("agent.router.default", "unknown path " + dflt);
    }
    for (String target : optionalMap("agent.router.rules", router.get("rules")).keySet()) {
      if (!paths.containsKey(target)) {
        throw new WorkflowValidationException("agent.router.rules." + target, "unknown path " + target);
      }
    }
    // Rules 2 and 4: tool references resolve; llm brains have an llm block.
    boolean hasLlm = spec.get("llm") != null;
    for (Map.Entry<String, Object> e : paths.entrySet()) {
      String p = "agent.paths." + e.getKey();
      Map<String, Object> path = requireMap(p, e.getValue());
      for (Object t : listOf(p + ".tools", path.get("tools"))) {
        requireTool(p + ".tools", String.valueOf(t), toolIds, spec);
      }
      for (Object t : optionalMap(p + ".tool_triggers", path.get("tool_triggers")).values()) {
        requireTool(p + ".tool_triggers", String.valueOf(t), toolIds, spec);
      }
      for (Object s : listOf(p + ".skills", path.get("skills"))) {
        if (!skills.containsKey(String.valueOf(s))) {
          throw new WorkflowValidationException(p + ".skills", "unknown skill " + s);
        }
      }
      if ("llm".equals(path.get("brain")) && !hasLlm) {
        throw new WorkflowValidationException(p + ".brain", "brain: llm requires a top-level llm block");
      }
    }
    // Rule 5: dims agree.
    if (retrieval.get("dim") != null && embeddings.get("dim") != null
        && ((Number) retrieval.get("dim")).intValue() != ((Number) embeddings.get("dim")).intValue()) {
      throw new WorkflowValidationException("retrieval.dim",
          "disagrees with embeddings.dim (" + retrieval.get("dim") + " vs " + embeddings.get("dim") + ")");
    }
    // Rules 2 and 6: saga steps and their compensations resolve.
    Map<String, Object> saga = optionalMap("saga", spec.get("saga"));
    keys("saga", saga, SAGA, false);
    i = 0;
    for (Map<String, Object> step : listOfMaps("saga.steps", saga.get("steps"))) {
      String p = "saga.steps[" + i++ + "]";
      keys(p, step, SAGA_STEP, false);
      String tool = requireString(p + ".tool", step.get("tool"));
      requireTool(p + ".tool", tool, toolIds, spec);
      if (step.get("compensate_with") != null) {
        requireTool(p + ".compensate_with", String.valueOf(step.get("compensate_with")), toolIds, spec);
      }
      if (step.get("args") != null) {
        requireMap(p + ".args", step.get("args"));
      }
    }
    for (Map.Entry<String, String> c : compensations.entrySet()) {
      requireTool("tools[" + c.getKey() + "].compensation", c.getValue(), toolIds, spec);
    }
    return spec;
  }

  /** Tool references may also resolve to tools an {@code mcp} server contributes at connect time. */
  private static void requireTool(String path, String id, Set<String> toolIds, Map<String, Object> spec) {
    if (toolIds.contains(id)) {
      return;
    }
    for (Map<String, Object> m : listOfMaps("mcp", spec.get("mcp"))) {
      String prefix = m.getOrDefault("name", "mcp") + "_";
      if (id.startsWith(prefix)) {
        return;
      }
    }
    throw new WorkflowValidationException(path, "unknown tool " + id);
  }

  private static void keys(String path, Map<String, Object> m, Set<String> allowed, boolean extensions) {
    for (String k : m.keySet()) {
      if (allowed.contains(k) || (extensions && k.startsWith("x-"))) {
        continue;
      }
      throw new WorkflowValidationException(path + "." + k, "unknown key (allowed: " + allowed + ")");
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> requireMap(String path, Object o) {
    if (!(o instanceof Map<?, ?>)) {
      throw new WorkflowValidationException(path, "expected a mapping");
    }
    return (Map<String, Object>) o;
  }

  private static Map<String, Object> optionalMap(String path, Object o) {
    return o == null ? Map.of() : requireMap(path, o);
  }

  private static List<Object> listOf(String path, Object o) {
    if (o == null) {
      return List.of();
    }
    if (!(o instanceof List<?> l)) {
      throw new WorkflowValidationException(path, "expected a list");
    }
    @SuppressWarnings("unchecked")
    List<Object> out = (List<Object>) l;
    return out;
  }

  private static List<Map<String, Object>> listOfMaps(String path, Object o) {
    List<Object> raw = listOf(path, o);
    List<Map<String, Object>> out = new java.util.ArrayList<>(raw.size());
    for (int i = 0; i < raw.size(); i++) {
      out.add(requireMap(path + "[" + i + "]", raw.get(i)));
    }
    return out;
  }

  private static String requireString(String path, Object o) {
    if (!(o instanceof String s) || s.isBlank()) {
      throw new WorkflowValidationException(path, "expected a non-empty string");
    }
    return s;
  }
}
