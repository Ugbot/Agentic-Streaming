package org.agentic.flink.execution;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.GuardrailDecision;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatRole;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.ChatToolCall;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client wrapper for LangChain4J LLM integration.
 *
 * <p>Real implementation using LangChain4J for:
 * <ul>
 *   <li>Chat completions with Ollama, OpenAI, etc.</li>
 *   <li>Message history management</li>
 *   <li>Tool calling support</li>
 *   <li>Temperature and token control</li>
 * </ul>
 *
 * <p><b>Supported Models:</b>
 * <ul>
 *   <li>Ollama (local): qwen2.5:3b, qwen2.5:7b, llama3:8b, etc.</li>
 *   <li>OpenAI: gpt-5.5, gpt-5.4 / gpt-5.4-mini / gpt-5.4-nano</li>
 * </ul>
 *
 * <p><b>Tool calls.</b> Tool calls come from the provider's structured tool execution requests
 * ({@code AiMessage.toolExecutionRequests()} surfaced as {@link ChatResponse#getToolCalls()}),
 * with arguments parsed by Jackson. Only when a response carries no structured requests does
 * {@link #parseToolCallsFromText} apply the text protocol ({@code TOOL_CALL: name {json}} or
 * {@code TOOL_CALL: name(k=v, ...)}) for providers without tool support.
 *
 * <p><b>Serialization.</b> The listener is process-local and transient; after Java
 * deserialization it is re-resolved to a no-op until {@link #withGuardrails} attaches one again.
 *
 * @author Agentic Flink Team
 * @deprecated Part of the legacy Flink DSL execution path. Prefer the event-sourced runtime in
 *     {@link org.agentic.flink.runtime.WorkflowTurnFunction}.
 */
@Deprecated
public class LLMClient implements Serializable {

  private static final long serialVersionUID = 2L;
  private static final Logger LOG = LoggerFactory.getLogger(LLMClient.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
  private static final Pattern TEXT_JSON_CALL =
      Pattern.compile("TOOL_CALL:\\s*([a-zA-Z0-9_-]+)\\s*\\{");
  private static final Pattern TEXT_KV_CALL =
      Pattern.compile("TOOL_CALL:\\s*([a-zA-Z0-9_-]+)\\s*\\(([^)]*)\\)");

  private final String modelName;
  private final double temperature;
  private final int maxTokens;
  private final String baseUrl;
  private final Duration timeout;
  private final ChatConnection chatConnection;

  // Lazily bound on first use. Not serialized — the connection is.
  private transient ChatClient chatClient;
  private transient ChatSetup chatSetup;

  // Optional guardrails — invoked before/after chat(). Empty list = unchanged behaviour.
  private List<Guardrail> guardrails = Collections.emptyList();
  private String agentId = "llm-client";
  private transient AgentEventListener listener = new AgentEventListener() {};

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    listener = new AgentEventListener() {};
  }

  /** Listener currently receiving guardrail hook events (never {@code null}). */
  public AgentEventListener getListener() {
    return listener;
  }

  private LLMClient(
      String modelName,
      double temperature,
      int maxTokens,
      String baseUrl,
      Duration timeout,
      ChatConnection chatConnection) {
    this.modelName = modelName;
    this.temperature = temperature;
    this.maxTokens = maxTokens;
    this.baseUrl = baseUrl;
    this.timeout = timeout;
    this.chatConnection =
        chatConnection != null
            ? chatConnection
            : LangChain4jChatConnection.ollama(baseUrl == null ? ConfigKeys.DEFAULT_OLLAMA_BASE_URL : baseUrl);
  }

  /** Returns the underlying {@link ChatClient}, binding it lazily on first call. */
  private ChatClient client() {
    if (chatClient == null) {
      try {
        chatClient = chatConnection.bind(null);
      } catch (Exception e) {
        throw new RuntimeException("Failed to bind ChatConnection: " + e.getMessage(), e);
      }
    }
    return chatClient;
  }

  /** Attach guardrails, an agent id, and (optionally) a listener for guardrail hook events. */
  public LLMClient withGuardrails(
      List<Guardrail> guardrails, String agentId, AgentEventListener listener) {
    this.guardrails = guardrails == null ? Collections.emptyList() : List.copyOf(guardrails);
    if (agentId != null) this.agentId = agentId;
    this.listener = listener == null ? new AgentEventListener() {} : listener;
    return this;
  }

  private ChatSetup setup() {
    if (chatSetup == null) {
      chatSetup =
          ChatSetup.builder()
              .withModel(modelName)
              .withTemperature(temperature)
              .withMaxResponseTokens(maxTokens)
              .build();
    }
    return chatSetup;
  }

  /**
   * Sends a chat request to the LLM with full conversation history.
   *
   * @param messages List of messages (system, user, assistant, tool)
   * @return LLM response
   */
  public LLMResponse chat(List<Map<String, Object>> messages) {
    LOG.debug("Sending chat request with {} messages to model: {}", messages.size(), modelName);

    try {
      List<ChatMessage> chatMessages = convertMessages(messages);

      // Pre-LLM guardrails.
      for (Guardrail g : guardrails) {
        GuardrailDecision d = g.beforeChat(agentId, chatMessages);
        if (d.isBlock()) {
          listener.onGuardrailBlock(agentId, d.getModelName(), d.getReason());
          LLMResponse blocked = new LLMResponse();
          blocked.setText(d.getReason() == null ? "Blocked by guardrail" : d.getReason());
          blocked.setModel(modelName);
          blocked.setToolCalls(new ArrayList<>());
          return blocked;
        }
        if (d.isRewrite() && d.getRewrittenPayload() != null) {
          listener.onGuardrailRewrite(agentId, d.getModelName(), d.getReason());
          chatMessages = replaceLastUserMessage(chatMessages, d.getRewrittenPayload());
        }
      }

      ChatResponse response = client().chat(chatMessages, setup());

      // Post-LLM guardrails.
      for (Guardrail g : guardrails) {
        GuardrailDecision d = g.afterChat(agentId, response);
        if (d.isBlock()) {
          listener.onGuardrailBlock(agentId, d.getModelName(), d.getReason());
          response =
              new ChatResponse(
                  d.getReason() == null ? "Blocked by guardrail" : d.getReason(),
                  response.getModelName(),
                  java.util.Collections.emptyList(),
                  response.getTokensUsed(),
                  response.getFinishReason());
          break;
        }
        if (d.isRewrite() && d.getRewrittenPayload() != null) {
          listener.onGuardrailRewrite(agentId, d.getModelName(), d.getReason());
          response =
              new ChatResponse(
                  d.getRewrittenPayload(),
                  response.getModelName(),
                  response.getToolCalls(),
                  response.getTokensUsed(),
                  response.getFinishReason());
        }
      }

      LLMResponse llmResponse = new LLMResponse();
      String responseText = response.getText();
      llmResponse.setText(responseText);
      llmResponse.setModel(modelName);
      if (response.getTokensUsed() != null) {
        llmResponse.setTokenUsage(response.getTokensUsed().intValue());
      }

      List<ToolCall> toolCalls =
          response.hasToolCalls()
              ? fromStructured(response.getToolCalls())
              : parseToolCallsFromText(responseText);
      llmResponse.setToolCalls(toolCalls);

      LOG.debug(
          "LLM response received: {} characters, {} tool calls",
          responseText.length(), toolCalls.size());
      return llmResponse;

    } catch (Exception e) {
      LOG.error("Error calling LLM: {}", e.getMessage(), e);
      throw new RuntimeException("LLM call failed: " + e.getMessage(), e);
    }
  }

  /**
   * Sends a simple prompt to the LLM.
   *
   * @param prompt The prompt text
   * @return LLM response text
   */
  public String generate(String prompt) {
    List<Map<String, Object>> messages = new ArrayList<>();
    messages.add(Map.of("role", "user", "content", prompt));
    return chat(messages).getText();
  }

  /**
   * Replaces the most recent user message with the guardrail's rewritten payload, keeping the
   * rest of the conversation (system prompt, earlier turns, tool results) intact. When the
   * conversation has no user message the rewrite is appended as one.
   */
  static List<ChatMessage> replaceLastUserMessage(List<ChatMessage> messages, String rewritten) {
    List<ChatMessage> out = new ArrayList<>(messages);
    for (int i = out.size() - 1; i >= 0; i--) {
      if (out.get(i).getRole() == ChatRole.USER) {
        out.set(i, ChatMessage.user(rewritten));
        return out;
      }
    }
    out.add(ChatMessage.user(rewritten));
    return out;
  }

  /** Maps the provider's structured tool requests onto the executor's {@link ToolCall}. */
  static List<ToolCall> fromStructured(List<ChatToolCall> calls) {
    List<ToolCall> out = new ArrayList<>(calls.size());
    int index = 0;
    for (ChatToolCall c : calls) {
      String id = c.getId() == null || c.getId().isBlank() ? "call_" + index : c.getId();
      out.add(new ToolCall(id, c.getName(), new HashMap<>(c.getArguments())));
      index++;
    }
    return out;
  }

  /**
   * Text fallback for providers without structured tool support. Recognizes
   * {@code TOOL_CALL: name {json}} (arguments parsed by Jackson) and, when no JSON form is
   * present, {@code TOOL_CALL: name(k=v, ...)}. Unparseable calls are skipped with a warning.
   */
  static List<ToolCall> parseToolCallsFromText(String responseText) {
    List<ToolCall> toolCalls = new ArrayList<>();
    if (responseText == null || responseText.isEmpty()) {
      return toolCalls;
    }

    int callCount = 0;
    Matcher json = TEXT_JSON_CALL.matcher(responseText);
    while (json.find()) {
      String toolName = json.group(1).trim();
      int end = closingBrace(responseText, json.end() - 1);
      if (end < 0) {
        LOG.warn("Text tool call {} has unterminated JSON arguments", toolName);
        break;
      }
      String jsonParams = responseText.substring(json.end() - 1, end + 1);
      try {
        Map<String, Object> parameters = JSON.readValue(jsonParams, MAP_TYPE);
        toolCalls.add(new ToolCall("call_" + (callCount++), toolName, parameters));
      } catch (IOException e) {
        LOG.warn("Text tool call {} has invalid JSON arguments {}: {}", toolName, jsonParams,
            e.getMessage());
      }
    }

    if (toolCalls.isEmpty()) {
      Matcher kv = TEXT_KV_CALL.matcher(responseText);
      while (kv.find()) {
        String toolName = kv.group(1).trim();
        Map<String, Object> parameters = parseKeyValueParameters(kv.group(2));
        toolCalls.add(new ToolCall("call_" + (callCount++), toolName, parameters));
      }
    }
    return toolCalls;
  }

  /**
   * Index of the brace closing the JSON object that opens at {@code open}, honouring braces
   * inside quoted strings; {@code -1} when the object is unterminated.
   */
  private static int closingBrace(String text, int open) {
    int depth = 0;
    boolean inString = false;
    for (int i = open; i < text.length(); i++) {
      char c = text.charAt(i);
      if (inString) {
        if (c == '\\') {
          i++;
        } else if (c == '"') {
          inString = false;
        }
      } else if (c == '"') {
        inString = true;
      } else if (c == '{') {
        depth++;
      } else if (c == '}' && --depth == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Parses key=value parameter format.
   */
  private static Map<String, Object> parseKeyValueParameters(String paramsStr) {
    Map<String, Object> params = new HashMap<>();

    String[] pairs = paramsStr.split(",");
    for (String pair : pairs) {
      String[] kv = pair.split("=", 2);
      if (kv.length == 2) {
        String key = kv[0].trim();
        String value = kv[1].trim();

        // Try to parse as number
        try {
          if (value.contains(".")) {
            params.put(key, Double.parseDouble(value));
          } else {
            params.put(key, Integer.parseInt(value));
          }
        } catch (NumberFormatException e) {
          // Keep as string, remove quotes if present
          params.put(key, value.replaceAll("\"", ""));
        }
      }
    }

    return params;
  }

  /**
   * Converts the loose {@code Map<String,Object>} message format used by older call sites into
   * the strongly-typed {@link ChatMessage} list the new SPI takes.
   */
  private List<ChatMessage> convertMessages(List<Map<String, Object>> messages) {
    List<ChatMessage> chatMessages = new ArrayList<>(messages.size());
    for (Map<String, Object> msg : messages) {
      String role = (String) msg.get("role");
      String content = (String) msg.get("content");
      if (content == null) {
        continue;
      }
      if (role == null) {
        chatMessages.add(ChatMessage.user(content));
        continue;
      }
      switch (role) {
        case "system":
          chatMessages.add(ChatMessage.system(content));
          break;
        case "user":
          chatMessages.add(ChatMessage.user(content));
          break;
        case "assistant":
        case "ai":
          chatMessages.add(ChatMessage.assistant(content));
          break;
        case "tool":
          String toolName = (String) msg.get("toolName");
          String toolCallId = (String) msg.get("toolCallId");
          chatMessages.add(ChatMessage.tool(toolCallId, toolName, content));
          break;
        default:
          LOG.warn("Unknown message role: {}, treating as user message", role);
          chatMessages.add(ChatMessage.user(content));
      }
    }
    return chatMessages;
  }

  /**
   * Creates a default LLM client with Ollama.
   */
  public static LLMClient createDefault(String modelName, double temperature) {
    return new LLMClientBuilder()
        .withModel(modelName)
        .withTemperature(temperature)
        .build();
  }

  /**
   * Creates a builder for custom configuration.
   */
  public static LLMClientBuilder builder() {
    return new LLMClientBuilder();
  }

  public String getModelName() { return modelName; }
  public double getTemperature() { return temperature; }
  public int getMaxTokens() { return maxTokens; }

  // ==================== Builder ====================

  public static class LLMClientBuilder {
    private String modelName = ConfigKeys.DEFAULT_OLLAMA_MODEL;
    private double temperature = 0.7;
    private int maxTokens = 4000;
    private String baseUrl = ConfigKeys.DEFAULT_OLLAMA_BASE_URL;
    private Duration timeout = Duration.ofSeconds(60);

    public LLMClientBuilder withModel(String modelName) {
      this.modelName = modelName;
      return this;
    }

    public LLMClientBuilder withTemperature(double temperature) {
      this.temperature = temperature;
      return this;
    }

    public LLMClientBuilder withMaxTokens(int maxTokens) {
      this.maxTokens = maxTokens;
      return this;
    }

    public LLMClientBuilder withBaseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    public LLMClientBuilder withTimeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public LLMClient build() {
      return new LLMClient(modelName, temperature, maxTokens, baseUrl, timeout, null);
    }

    /** Build with an explicit {@link ChatConnection}; bypasses the default LangChain4J path. */
    public LLMClient build(ChatConnection chatConnection) {
      return new LLMClient(modelName, temperature, maxTokens, baseUrl, timeout, chatConnection);
    }
  }
}
