package org.agentic.flink.execution;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.inference.Guardrail;
import org.agentic.flink.inference.GuardrailDecision;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * @author Agentic Flink Team
 */
public class LLMClient implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LLMClient.class);

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
          chatMessages = List.of(ChatMessage.user(d.getRewrittenPayload()));
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

      List<ToolCall> toolCalls = parseToolCalls(responseText);
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
   * Parses tool calls from LLM response text.
   *
   * <p>Supports multiple formats:
   * <ul>
   *   <li>TOOL_CALL: tool_name {"param": "value"}</li>
   *   <li>TOOL_CALL: tool_name(param=value)</li>
   *   <li>{"tool": "tool_name", "parameters": {...}}</li>
   * </ul>
   *
   * @param responseText The LLM response text
   * @return List of parsed tool calls
   */
  private List<ToolCall> parseToolCalls(String responseText) {
    List<ToolCall> toolCalls = new ArrayList<>();

    if (responseText == null || responseText.isEmpty()) {
      return toolCalls;
    }

    // Pattern 1: TOOL_CALL: tool_name {json}
    // Example: TOOL_CALL: calculator-add {"a": 5, "b": 3}
    java.util.regex.Pattern pattern1 = java.util.regex.Pattern.compile(
        "TOOL_CALL:\\s*([a-zA-Z0-9_-]+)\\s*\\{([^}]+)\\}");
    java.util.regex.Matcher matcher1 = pattern1.matcher(responseText);

    int callCount = 0;
    while (matcher1.find()) {
      String toolName = matcher1.group(1).trim();
      String jsonParams = "{" + matcher1.group(2) + "}";

      try {
        Map<String, Object> parameters = parseJsonParameters(jsonParams);
        String toolCallId = "call_" + (callCount++);
        toolCalls.add(new ToolCall(toolCallId, toolName, parameters));
        LOG.debug("Parsed tool call: {} with params: {}", toolName, parameters);
      } catch (Exception e) {
        LOG.warn("Failed to parse tool call parameters: {}", jsonParams, e);
      }
    }

    // Pattern 2: TOOL_CALL: tool_name(param=value, param2=value2)
    // Example: TOOL_CALL: calculator-add(a=5, b=3)
    if (toolCalls.isEmpty()) {
      java.util.regex.Pattern pattern2 = java.util.regex.Pattern.compile(
          "TOOL_CALL:\\s*([a-zA-Z0-9_-]+)\\s*\\(([^)]+)\\)");
      java.util.regex.Matcher matcher2 = pattern2.matcher(responseText);

      while (matcher2.find()) {
        String toolName = matcher2.group(1).trim();
        String paramsStr = matcher2.group(2);

        try {
          Map<String, Object> parameters = parseKeyValueParameters(paramsStr);
          String toolCallId = "call_" + (callCount++);
          toolCalls.add(new ToolCall(toolCallId, toolName, parameters));
          LOG.debug("Parsed tool call: {} with params: {}", toolName, parameters);
        } catch (Exception e) {
          LOG.warn("Failed to parse tool call parameters: {}", paramsStr, e);
        }
      }
    }

    return toolCalls;
  }

  /**
   * Parses JSON-formatted parameters.
   */
  private Map<String, Object> parseJsonParameters(String jsonStr) {
    Map<String, Object> params = new HashMap<>();

    // Simple JSON parser for basic types
    // Format: {"key": "value", "key2": 123}
    String content = jsonStr.replaceAll("[{}]", "").trim();
    if (content.isEmpty()) {
      return params;
    }

    String[] pairs = content.split(",");
    for (String pair : pairs) {
      String[] kv = pair.split(":", 2);
      if (kv.length == 2) {
        String key = kv[0].trim().replaceAll("\"", "");
        String value = kv[1].trim();

        // Remove quotes and parse value
        if (value.startsWith("\"") && value.endsWith("\"")) {
          params.put(key, value.substring(1, value.length() - 1));
        } else {
          // Try to parse as number
          try {
            if (value.contains(".")) {
              params.put(key, Double.parseDouble(value));
            } else {
              params.put(key, Integer.parseInt(value));
            }
          } catch (NumberFormatException e) {
            // Keep as string
            params.put(key, value);
          }
        }
      }
    }

    return params;
  }

  /**
   * Parses key=value parameter format.
   */
  private Map<String, Object> parseKeyValueParameters(String paramsStr) {
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
