package org.agentic.flink.example.banking;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.a2a.A2AClient;
import org.agentic.flink.a2a.A2AClientFactory;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APart;
import org.agentic.flink.a2a.A2ATask;
import org.agentic.flink.a2a.A2ATransport;
import org.agentic.flink.a2a.RemoteAgentSpec;
import org.agentic.flink.example.banking.env.EnvApiClient;
import org.agentic.flink.example.banking.env.EnvApiToolExecutor;
import org.agentic.flink.example.banking.env.ListEnvToolsExecutor;
import org.agentic.flink.example.banking.safety.AuthorizationToolGuard;
import org.agentic.flink.example.banking.safety.BankingScreening;
import org.agentic.flink.example.banking.safety.SessionAuthState;
import org.agentic.flink.tools.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assembles a role-specific banking agent (the personal assistant or the bank's customer-service
 * agent) from environment configuration: the chat model, the dynamic env toolset, RAG (CS),
 * the personal→CS A2A round-trip, the system prompt, and the threat-screening pipeline.
 *
 * <p>Shared by the standalone gateway executor and any Flink job, so both run identical agents.
 * Built once at startup (not serialized); the bounded {@link ReActTurnBrain} it produces is what
 * actually drives the LLM.
 */
public final class BankingAgentSetup {

  private static final Logger LOG = LoggerFactory.getLogger(BankingAgentSetup.class);

  public enum Role {
    PERSONAL,
    CS
  }

  private final Role role;
  private final ReActTurnBrain brain;
  private final BankingScreening screening;
  private final BankingTurnContext.CustomerServiceClient cs; // personal only
  private final long turnDeadlineMs;
  private final int maxRoundTrips;
  private final int maxIterations;

  private BankingAgentSetup(
      Role role,
      ReActTurnBrain brain,
      BankingScreening screening,
      BankingTurnContext.CustomerServiceClient cs,
      long turnDeadlineMs,
      int maxRoundTrips,
      int maxIterations) {
    this.role = role;
    this.brain = brain;
    this.screening = screening;
    this.cs = cs;
    this.turnDeadlineMs = turnDeadlineMs;
    this.maxRoundTrips = maxRoundTrips;
    this.maxIterations = maxIterations;
  }

  public Role role() {
    return role;
  }

  public ReActTurnBrain brain() {
    return brain;
  }

  public BankingScreening screening() {
    return screening;
  }

  public BankingTurnContext.CustomerServiceClient cs() {
    return cs;
  }

  public long turnDeadlineMs() {
    return turnDeadlineMs;
  }

  public int maxRoundTrips() {
    return maxRoundTrips;
  }

  public int maxIterations() {
    return maxIterations;
  }

  /** Build the agent for {@code role} from environment variables. */
  public static BankingAgentSetup fromEnv(Role role) {
    return fromEnv(role, null);
  }

  /**
   * Build the agent for {@code role}, optionally overriding the personal→CS client (e.g. with a
   * minimal spec-conformant HTTP client instead of the SDK one).
   */
  public static BankingAgentSetup fromEnv(
      Role role, BankingTurnContext.CustomerServiceClient csOverride) {
    BankingModel model = BankingModel.fromEnv();
    SessionAuthState authState = new SessionAuthState();
    Map<String, ToolExecutor> tools = new LinkedHashMap<>();

    // Dynamic env toolset (optional: only when a harness env API is configured).
    String envApiUrl = System.getenv("ENV_API_URL");
    if (envApiUrl != null && !envApiUrl.isBlank()) {
      EnvApiClient envClient = EnvApiClient.fromEnv();
      tools.put("list_env_tools", new ListEnvToolsExecutor(envClient));
      // Placeholder-blocking guard on the generic env-tool call (name-aware verification gating is
      // a follow-up once the harness's high-risk tool names are enumerated).
      tools.put(
          "call_env_tool",
          new AuthorizationToolGuard(
              EnvApiToolExecutor.fallback(envClient), false, false, authState));
    } else {
      LOG.warn("ENV_API_URL not set — agent runs without environment tools (chat/RAG only)");
    }

    String systemPrompt;
    BankingTurnContext.CustomerServiceClient cs = null;
    if (role == Role.CS) {
      String kbDir = env("KB_PATH", "kb/documents");
      if (BankingEmbeddings.isKeyword()) {
        tools.put("kb_search", KbSearchTool.fromDirectory(kbDir));
      } else {
        tools.put(
            "kb_search",
            VectorKbSearchTool.build(
                kbDir, env("EMBED_CACHE_DIR", ".kb-cache"), BankingEmbeddings.fromEnv()));
      }
      systemPrompt = csSystemPrompt();
    } else {
      systemPrompt = personalSystemPrompt();
      cs = csOverride != null ? csOverride : customerServiceClient();
    }

    int maxRoundTrips = intEnv("A2A_MAX_ROUND_TRIPS", 4);
    int maxIterations = intEnv("A2A_MAX_ITERATIONS", 12);
    long turnDeadlineMs = (long) intEnv("A2A_TURN_DEADLINE_MS", 240_000);
    long toolTimeoutMs = (long) intEnv("A2A_TOOL_TIMEOUT_MS", 30_000);

    ReActTurnBrain brain =
        new ReActTurnBrain(model.connection(), model.setup(), systemPrompt, tools, toolTimeoutMs);
    LOG.info(
        "Banking agent [{}] ready: tools={}, model={}", role, tools.keySet(), model.setup().getModelName());
    return new BankingAgentSetup(
        role, brain, BankingScreening.defaults(), cs, turnDeadlineMs, maxRoundTrips, maxIterations);
  }

  /** Personal→CS round-trip over A2A, propagating the session contextId (a hard harness rule). */
  private static BankingTurnContext.CustomerServiceClient customerServiceClient() {
    String csUrl = env("CS_AGENT_URL", null);
    if (csUrl == null) {
      LOG.warn("CS_AGENT_URL not set — ask_customer_service will be unavailable");
      return null;
    }
    RemoteAgentSpec spec = RemoteAgentSpec.endpoint("cs-agent", csUrl, A2ATransport.JSONRPC);
    A2AClient client = A2AClientFactory.discovering().create(spec);
    return (contextId, message) -> {
      A2AMessage msg =
          new A2AMessage(
              A2AMessage.Role.USER,
              UUID.randomUUID().toString(),
              List.of(A2APart.text(message)),
              contextId, // propagate the session contextId to CS
              null,
              null);
      A2ATask task = client.sendAndAwait(msg, spec.pollIntervalMs(), spec.requestTimeoutMs());
      StringBuilder sb = new StringBuilder();
      task.getArtifacts().forEach(a -> sb.append(a.textContent()).append('\n'));
      return sb.toString().trim();
    };
  }

  private static String csSystemPrompt() {
    String policy = readOrEmpty(env("KB_POLICY_PATH", "kb/policy.md"));
    return policy
        + "\n\n## Knowledge base\n"
        + "Before answering policy questions or performing procedures, call kb_search(query) to find"
        + " the relevant document. Do not invent policies. If the customer's request is outside your"
        + " capabilities or the knowledge base, follow the escalation/transfer guidance above.";
  }

  private static String personalSystemPrompt() {
    return "You are the user's personal banking assistant for their Rho-Bank accounts.\n"
        + "- Use your own environment tools (discover them with list_env_tools, call them with"
        + " call_env_tool) for the user's banking actions.\n"
        + "- For anything bank-side — account lookups, policy questions, disputes — contact customer"
        + " service with ask_customer_service, relaying the user's request faithfully and reporting"
        + " back.\n"
        + "- Customer service usually needs to verify the user's identity; ask the user for exactly"
        + " the details requested and pass them along.\n"
        + "- Never use placeholder argument values; if you don't know a required detail, ask the"
        + " user first. Be concise and never invent account details or policies.";
  }

  private static String readOrEmpty(String path) {
    try {
      return Files.readString(Path.of(path));
    } catch (Exception e) {
      LOG.warn("Could not read policy at {}: {}", path, e.getMessage());
      return "You are a helpful, careful Rho-Bank customer service agent.";
    }
  }

  private static String env(String key, String def) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? def : v;
  }

  private static int intEnv(String key, int def) {
    String v = System.getenv(key);
    try {
      return v == null || v.isBlank() ? def : Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      return def;
    }
  }
}
