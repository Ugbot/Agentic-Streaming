package org.agentic.flink.example.banking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.OutputSchema;
import org.agentic.flink.tools.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TurnBrain} that runs the framework's ReAct loop (the same Thought/Action/Observation +
 * structured-output pattern as {@link ReActProcessFunction}) but bounded by the turn's {@link
 * org.agentic.flink.example.banking.safety.RoutingBudget}: every loop iteration consumes
 * {@code allowIteration()}, so the agent cannot out-loop the harness timeout.
 *
 * <p>Tools come from a name→{@link ToolExecutor} map (env tools, KB search, …). The pseudo-tool
 * {@code ask_customer_service} is special-cased to the budget-gated {@link
 * BankingTurnContext#askCustomerService} so the personal↔CS round-trip is counted and capped.
 * The chat model is provided via a {@link ChatConnection} (OpenAI in dev, Gemini for marked runs),
 * bound lazily on the task side.
 */
public final class ReActTurnBrain implements TurnBrain {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ReActTurnBrain.class);

  /** Tool name the brain routes to the budget-gated CS round-trip (personal agent only). */
  public static final String ASK_CS = "ask_customer_service";

  private final ChatConnection connection;
  private final ChatSetup setup;
  private final String systemPrompt;
  private final Map<String, ToolExecutor> tools;
  private final long toolTimeoutMs;

  private transient volatile ChatClient client;
  private transient volatile OutputSchema<ReActStep> schema;

  public ReActTurnBrain(
      ChatConnection connection,
      ChatSetup setup,
      String systemPrompt,
      Map<String, ToolExecutor> tools,
      long toolTimeoutMs) {
    this.connection = java.util.Objects.requireNonNull(connection, "connection");
    this.setup = java.util.Objects.requireNonNull(setup, "setup");
    this.systemPrompt = systemPrompt == null ? "" : systemPrompt;
    this.tools = tools == null ? Map.of() : Map.copyOf(tools);
    this.toolTimeoutMs = toolTimeoutMs <= 0 ? 30_000 : toolTimeoutMs;
  }

  @Override
  public String respond(String userText, BankingTurnContext ctx) {
    return converse(List.of(ChatMessage.user(userText == null ? "" : userText)), ctx);
  }

  /**
   * Run the bounded ReAct loop over a pre-seeded conversation (prior dialogue ending with the
   * current user turn) — used by the routed graph so each path brain sees the multi-turn history
   * from {@link org.agentic.flink.example.banking.graph.ConversationMemory}.
   */
  public String converse(List<ChatMessage> conversation, BankingTurnContext ctx) {
    List<ChatMessage> messages = new ArrayList<>();
    messages.add(ChatMessage.system(composeSystemPrompt()));
    if (conversation != null) {
      messages.addAll(conversation);
    }

    ChatSetup turnSetup =
        setup.hasOutputSchema() ? setup : setup.toBuilder().withOutputSchema(schema()).build();
    String lastText = "";

    while (ctx.withinDeadline() && ctx.budget().allowIteration()) {
      ChatResponse response = client().chat(messages, turnSetup);
      lastText = response.getText() == null ? "" : response.getText();

      ReActStep step;
      try {
        step = response.as(schema());
      } catch (OutputSchema.SchemaViolation e) {
        // Model answered in prose — treat as the final reply.
        return lastText;
      }

      String type = step.getType() == null ? "" : step.getType().toLowerCase(Locale.ROOT);
      if ("final".equals(type)) {
        return step.getAnswer() != null ? step.getAnswer() : lastText;
      }

      // Record the model's step, then always continue with a USER turn so the transcript ends on a
      // user message before the next call (Anthropic rejects assistant-terminated conversations).
      messages.add(ChatMessage.assistant(lastText));
      if ("action".equals(type) && step.getTool() != null) {
        Map<String, Object> args = step.getArguments() == null ? new HashMap<>() : step.getArguments();
        String observation = runTool(step.getTool(), args, ctx);
        messages.add(ChatMessage.user("Observation from " + step.getTool() + ": " + observation));
      } else {
        messages.add(ChatMessage.user("Continue: respond with your next step or a final answer."));
      }
    }

    // Budget exhausted — answer with what we have, never spin into the timeout.
    LOG.info("ReAct brain stopped early for ctx {}: {}", ctx.contextId(), ctx.budget().lastDenial());
    return lastText.isBlank()
        ? "I'm sorry — I wasn't able to complete that within the available time. Please try again."
        : lastText;
  }

  private String runTool(String toolName, Map<String, Object> args, BankingTurnContext ctx) {
    if (ASK_CS.equalsIgnoreCase(toolName)) {
      Object msg = args.containsKey("message") ? args.get("message") : args.get("question");
      return ctx.askCustomerService(msg == null ? "" : msg.toString());
    }
    ToolExecutor executor = tools.get(toolName);
    if (executor == null) {
      return "ERROR: tool '" + toolName + "' is not available";
    }
    try {
      return String.valueOf(executor.execute(args).get(toolTimeoutMs, TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      return "ERROR: " + e.getMessage();
    }
  }

  private String composeSystemPrompt() {
    StringBuilder sb = new StringBuilder();
    if (!systemPrompt.isEmpty()) {
      sb.append(systemPrompt).append("\n\n");
    }
    sb.append("You can call tools: ");
    List<String> names = new ArrayList<>(tools.keySet());
    if (!names.contains(ASK_CS) && hasCsTool()) {
      names.add(ASK_CS);
    }
    sb.append(String.join(", ", names)).append(".\n\n");
    sb.append(
        "Respond ONLY with valid JSON of this schema:\n"
            + "{ \"type\": \"thought\"|\"action\"|\"final\", \"thought\": string,"
            + " \"tool\": string-or-null, \"arguments\": object, \"answer\": string-or-null }\n"
            + "Use 'action' to call a tool (set tool + arguments), 'thought' to reason, and"
            + " 'final' with 'answer' to reply to the user. Be concise and never invent account"
            + " details or policies.");
    return sb.toString();
  }

  private boolean hasCsTool() {
    return true; // ask_customer_service is always offered; the context refuses it if no peer.
  }

  private ChatClient client() {
    ChatClient c = client;
    if (c == null) {
      synchronized (this) {
        if (client == null) {
          try {
            client = connection.bind(null);
          } catch (Exception e) {
            throw new RuntimeException("Failed to bind chat client", e);
          }
        }
        c = client;
      }
    }
    return c;
  }

  private OutputSchema<ReActStep> schema() {
    OutputSchema<ReActStep> s = schema;
    if (s == null) {
      synchronized (this) {
        if (schema == null) {
          schema = OutputSchema.of(ReActStep.class);
        }
        s = schema;
      }
    }
    return s;
  }
}
