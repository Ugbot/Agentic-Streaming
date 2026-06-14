package org.agentic.flink.function;

import org.agentic.flink.dsl.Agent;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.OutputSchema;
import org.agentic.flink.tool.ToolRegistry;
import org.agentic.flink.tools.ToolExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Canonical ReAct loop, packaged as a Flink {@code KeyedProcessFunction}.
 *
 * <p>The function ingests an event for a given conversation key, prompts the LLM through the
 * agent's {@link ChatConnection} with the running {@code Thought/Action/Observation} transcript,
 * parses the response under a fixed {@link OutputSchema} ({@link ReActStep}), executes any tool
 * call against the agent's {@link ToolRegistry}, appends the observation to per-key Flink state,
 * and loops for at most {@link Agent#getMaxIterations()} steps before emitting the final answer.
 *
 * <p>State held per key:
 *
 * <ul>
 *   <li>{@code iterationsState} — count of completed ReAct turns for this key.
 *   <li>{@code transcriptState} — append-only list of chat messages, used to feed each turn.
 *   <li>{@code finishedState} — once {@code true}, additional events are passed through unchanged.
 * </ul>
 */
public final class ReActProcessFunction<E> extends KeyedProcessFunction<String, E, E> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ReActProcessFunction.class);

  /** Schema the model is asked to fill on each ReAct iteration. */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ReActStep {
    /** "thought" | "action" | "final" — what the model is asking the runtime to do. */
    private String type;
    private String thought;
    /** Tool name when type=action; null otherwise. */
    private String tool;
    private Map<String, Object> arguments;
    /** Final answer when type=final. */
    private String answer;
  }

  private final Agent agent;
  private final ToolRegistry toolRegistry;

  private transient ChatClient chatClient;
  private transient AgentEventListener listener;
  private transient OutputSchema<ReActStep> stepSchema;

  // Flink state
  private transient ValueState<Integer> iterationsState;
  private transient ListState<ChatMessage> transcriptState;
  private transient ValueState<Boolean> finishedState;

  public ReActProcessFunction(Agent agent, ToolRegistry toolRegistry) {
    this.agent = agent;
    this.toolRegistry = toolRegistry;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    ChatConnection connection = agent.getChatConnection();
    if (connection == null) {
      // Fall back to the ServiceLoader-discovered default; never null because the
      // LangChain4j default is registered in META-INF/services.
      ServiceLoader<ChatConnection> loader = ServiceLoader.load(ChatConnection.class);
      java.util.Iterator<ChatConnection> it = loader.iterator();
      if (it.hasNext()) connection = it.next();
    }
    if (connection == null) {
      throw new IllegalStateException(
          "ReActProcessFunction requires a ChatConnection; none configured and no default "
              + "registered via ServiceLoader");
    }
    chatClient = connection.bind(getRuntimeContext());

    List<AgentEventListener> listeners = agent.getListeners();
    listener =
        listeners == null || listeners.isEmpty()
            ? new AgentEventListener() {}
            : AgentEventListener.fanOut(listeners);

    stepSchema = OutputSchema.of(ReActStep.class);

    iterationsState =
        getRuntimeContext().getState(new ValueStateDescriptor<>("react.iter", Integer.class));
    transcriptState =
        getRuntimeContext()
            .getListState(new ListStateDescriptor<>("react.transcript", ChatMessage.class));
    finishedState =
        getRuntimeContext().getState(new ValueStateDescriptor<>("react.done", Boolean.class));

    listener.onAgentStart(agent.getAgentId());
  }

  @Override
  public void processElement(E event, Context ctx, Collector<E> out) throws Exception {
    Boolean done = finishedState.value();
    if (done != null && done) {
      out.collect(event);
      return;
    }

    // Seed the transcript on the first iteration.
    List<ChatMessage> messages = currentTranscript();
    if (messages.isEmpty()) {
      String system = composeSystemPrompt(agent.getSystemPrompt());
      messages.add(ChatMessage.system(system));
      messages.add(ChatMessage.user(event == null ? "" : event.toString()));
    }

    int iteration = iterationsState.value() == null ? 0 : iterationsState.value();
    int maxIterations = agent.getMaxIterations();
    int toolCalls = 0; // tool calls performed during this loop run
    int stallNudges = 0; // pushbacks on a "I need a tool" non-action final

    while (iteration < maxIterations) {
      iteration++;

      ChatSetup setup = withSchema(agent.getChatSetup(), stepSchema);

      listener.onChatRequest(agent.getAgentId(), setup.getModelName(), messages.size());
      ChatResponse response;
      try {
        response = chatClient.chat(messages, setup);
      } catch (Exception e) {
        listener.onError(agent.getAgentId(), "chat", e);
        throw e;
      }
      listener.onChatResponse(
          agent.getAgentId(),
          setup.getModelName(),
          response.getText() == null ? 0 : response.getText().length(),
          response.getTokensUsed());

      ReActStep step;
      try {
        step = response.as(stepSchema);
      } catch (OutputSchema.SchemaViolation e) {
        // Treat schema violation as a final answer — pass the raw text through so the model
        // isn't penalised for refusing to fit our format on the way out.
        LOG.debug("ReAct step did not parse; treating as final answer");
        messages.add(ChatMessage.assistant(response.getText()));
        markFinished(iteration, messages);
        out.collect(event);
        return;
      }

      messages.add(ChatMessage.assistant(response.getText()));

      String type = step.getType() == null ? "" : step.getType().toLowerCase();
      if ("final".equals(type)) {
        // Action-adherence guard: small models often END the turn by narrating that they need to
        // call a tool ("I need to inspect the tools first" / "I don't have access…") without ever
        // emitting the action step. If the agent has tools, has called none this run, and the final
        // reads like that stall, push back and make it act (bounded; a genuine final still passes).
        String answer = step.getAnswer() != null ? step.getAnswer() : response.getText();
        if (org.agentic.flink.llm.ReActGuard.shouldNudge(
            answer, !toolRegistry.getToolNames().isEmpty(), toolCalls, stallNudges)) {
          stallNudges++;
          messages.add(
              ChatMessage.user(org.agentic.flink.llm.ReActGuard.stallNudge(toolRegistry.getToolNames())));
          continue;
        }
        markFinished(iteration, messages);
        out.collect(event);
        return;
      }
      if ("action".equals(type) && step.getTool() != null) {
        String toolCallId = "react-" + iteration;
        String toolName = step.getTool();
        Map<String, Object> args = step.getArguments() == null ? new HashMap<>() : step.getArguments();

        java.util.Optional<ToolExecutor> executorOpt = toolRegistry.getExecutor(toolName);
        if (executorOpt.isEmpty()) {
          messages.add(
              ChatMessage.tool(toolCallId, toolName, "ERROR: tool '" + toolName + "' not registered"));
          continue;
        }
        ToolExecutor executor = executorOpt.get();
        listener.onToolCallStart(agent.getAgentId(), toolName, toolCallId);
        long started = System.nanoTime();
        Object result;
        boolean success = false;
        try {
          result =
              executor
                  .execute(args)
                  .get(agent.getToolTimeout().toMillis(), TimeUnit.MILLISECONDS);
          success = true;
        } catch (Exception e) {
          result = "ERROR: " + e.getMessage();
          listener.onError(agent.getAgentId(), "tool:" + toolName, e);
        }
        long durationMs = (System.nanoTime() - started) / 1_000_000;
        listener.onToolCallEnd(agent.getAgentId(), toolName, toolCallId, success, durationMs);

        messages.add(ChatMessage.tool(toolCallId, toolName, String.valueOf(result)));
        toolCalls++;
        continue;
      }

      // Type is "thought" or unrecognized — keep iterating. Append the thought to the transcript
      // and prompt again.
    }

    // Hit the iteration budget without a final answer. Persist the transcript and emit the
    // pass-through event so the surrounding job can decide what to do.
    LOG.info(
        "ReAct agent {} hit iteration budget {} without final answer",
        agent.getAgentId(), maxIterations);
    markFinished(iteration, messages);
    out.collect(event);
  }

  @Override
  public void close() throws Exception {
    if (chatClient != null) {
      chatClient.close();
    }
    super.close();
  }

  // ---- helpers ----

  private List<ChatMessage> currentTranscript() throws Exception {
    List<ChatMessage> out = new ArrayList<>();
    for (ChatMessage m : transcriptState.get()) {
      out.add(m);
    }
    return out;
  }

  private void markFinished(int iteration, List<ChatMessage> messages) throws Exception {
    iterationsState.update(iteration);
    transcriptState.update(messages);
    finishedState.update(true);
  }

  private static ChatSetup withSchema(ChatSetup base, OutputSchema<?> schema) {
    if (base == null) {
      return ChatSetup.builder()
          .withModel("qwen2.5:3b")
          .withTemperature(0.3)
          .withMaxResponseTokens(1024)
          .withOutputSchema(schema)
          .build();
    }
    return base.hasOutputSchema() ? base : base.toBuilder().withOutputSchema(schema).build();
  }

  private static String composeSystemPrompt(String base) {
    StringBuilder sb = new StringBuilder();
    if (base != null && !base.isEmpty()) {
      sb.append(base).append("\n\n");
    }
    sb.append(
        "Respond ONLY with valid JSON conforming to this schema:\n"
            + "{ \"type\": \"thought\"|\"action\"|\"final\","
            + " \"thought\": \"...\","
            + " \"tool\": \"<tool name when type=action, else null>\","
            + " \"arguments\": { ... },"
            + " \"answer\": \"<final answer when type=final>\" }\n"
            + "Loop: 'thought' to reason, 'action' to call a tool, 'final' to stop.");
    return sb.toString();
  }
}
