package org.agentic.flink.llm.langchain4j;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.llm.ChatConnection;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.googleai.GoogleAiGeminiChatModel;
import dev.langchain4j.model.ollama.OllamaChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import java.time.Duration;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link ChatConnection} that delegates to LangChain4J.
 *
 * <p>Picks the underlying LangChain4J model implementation based on the {@code provider} field:
 * {@code "ollama"} or {@code "openai"}. Other providers can be added by implementing
 * {@link ChatConnection} directly or by registering a different SPI via {@link
 * java.util.ServiceLoader}.
 *
 * <p>The actual {@link dev.langchain4j.model.chat.ChatModel} is constructed lazily inside
 * each {@link org.agentic.flink.llm.ChatSetup} call — model name / temperature live in
 * the setup, not the connection, so a single connection can serve many setups with different
 * model parameters. We cache a per-setup-signature client to avoid re-building on every event.
 */
public final class LangChain4jChatConnection implements ChatConnection {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LangChain4jChatConnection.class);

  /** Provider tag for LangChain4J routing. */
  public enum Provider {
    OLLAMA,
    OPENAI,
    ANTHROPIC,
    GEMINI
  }

  private final Provider provider;
  private final String baseUrl;
  private final String apiKey;
  private final Duration timeout;

  /** No-arg constructor for {@link java.util.ServiceLoader}. Uses Ollama at the default URL. */
  public LangChain4jChatConnection() {
    this(Provider.OLLAMA, ConfigKeys.DEFAULT_OLLAMA_BASE_URL, null, Duration.ofSeconds(60));
  }

  public LangChain4jChatConnection(
      Provider provider, String baseUrl, String apiKey, Duration timeout) {
    this.provider = provider;
    this.baseUrl = baseUrl;
    this.apiKey = apiKey;
    this.timeout = timeout == null ? Duration.ofSeconds(60) : timeout;
  }

  public static LangChain4jChatConnection ollama(String baseUrl) {
    return new LangChain4jChatConnection(Provider.OLLAMA, baseUrl, null, Duration.ofSeconds(60));
  }

  public static LangChain4jChatConnection openai(String apiKey) {
    return new LangChain4jChatConnection(Provider.OPENAI, null, apiKey, Duration.ofSeconds(60));
  }

  /**
   * Claude via the Anthropic API. The model name (e.g. {@code claude-sonnet-4-6}) is supplied
   * per call through {@link org.agentic.flink.llm.ChatSetup}.
   */
  public static LangChain4jChatConnection anthropic(String apiKey) {
    return new LangChain4jChatConnection(
        Provider.ANTHROPIC, null, apiKey, Duration.ofSeconds(120));
  }

  /**
   * Google Gemini via the Google AI API key ({@code GOOGLE_API_KEY}). The model name (e.g. {@code
   * gemini-3.5-flash}, required by the A2A banking demo) is supplied per call through {@link
   * org.agentic.flink.llm.ChatSetup}. Generous default timeout to stay under the harness's per-turn
   * limit while tolerating Vertex rate-limiting.
   */
  public static LangChain4jChatConnection gemini(String apiKey) {
    return new LangChain4jChatConnection(Provider.GEMINI, null, apiKey, Duration.ofSeconds(120));
  }

  public Provider getProvider() {
    return provider;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public Duration getTimeout() {
    return timeout;
  }

  @Override
  public LangChain4jChatClient bind(RuntimeContext runtimeContext) {
    LOG.info(
        "Binding LangChain4jChatConnection: provider={}, baseUrl={}, timeout={}",
        provider, baseUrl, timeout);
    return new ChatClientImpl(this);
  }

  @Override
  public String providerName() {
    return "langchain4j:" + provider.name().toLowerCase();
  }

  /**
   * Builds a LangChain4J {@link ChatModel} for the given setup signature.
   *
   * <p>Public on this concrete class as part of the LangChain4J-specific surface: users who
   * downcast a {@link ChatConnection} to this type may want to construct a model directly without
   * going through {@link #bind} / {@code chat}.
   */
  public ChatModel buildModel(String modelName, double temperature, int maxTokens) {
    switch (provider) {
      case OLLAMA:
        return OllamaChatModel.builder()
            .baseUrl(baseUrl)
            .modelName(modelName)
            .temperature(temperature)
            .timeout(timeout)
            .build();
      case OPENAI:
        OpenAiChatModel.OpenAiChatModelBuilder b =
            OpenAiChatModel.builder().apiKey(apiKey).modelName(modelName).timeout(timeout);
        // OpenAI reasoning models (gpt-5*, o1/o3/o4*) reject `max_tokens` (they require
        // `max_completion_tokens`, which langchain4j 0.35.0 can't send) and only allow the default
        // temperature. Omit both for those models so the request is accepted; send them otherwise.
        if (!org.agentic.flink.llm.OpenAiModels.isReasoning(modelName)) {
          b.temperature(temperature).maxTokens(maxTokens);
        }
        if (baseUrl != null && !baseUrl.isEmpty()) {
          b.baseUrl(baseUrl);
        }
        return b.build();
      case ANTHROPIC:
        return AnthropicChatModel.builder()
            .apiKey(apiKey)
            .modelName(modelName)
            .temperature(temperature)
            .maxTokens(maxTokens)
            .timeout(timeout)
            .build();
      case GEMINI:
        return GoogleAiGeminiChatModel.builder()
            .apiKey(apiKey)
            .modelName(modelName)
            .temperature(temperature)
            .maxOutputTokens(maxTokens)
            .timeout(timeout)
            .build();
      default:
        throw new IllegalStateException("Unknown provider: " + provider);
    }
  }


  /** Convert framework {@link org.agentic.flink.llm.ChatMessage} to LangChain4J form. */
  static dev.langchain4j.data.message.ChatMessage toLangChainMessage(
      org.agentic.flink.llm.ChatMessage m) {
    switch (m.getRole()) {
      case SYSTEM:
        return SystemMessage.from(m.getContent());
      case USER:
        return UserMessage.from(m.getContent());
      case ASSISTANT:
        return AiMessage.from(m.getContent());
      case TOOL:
        // No first-class tool-result role here; fold it into an assistant turn tagged with the
        // tool name so the model can see the result.
        return AiMessage.from(
            "[tool=" + (m.getToolName() == null ? "?" : m.getToolName()) + "] " + m.getContent());
      default:
        return UserMessage.from(m.getContent());
    }
  }

  /** Extract a {@link org.agentic.flink.llm.ChatResponse} from a LangChain4J response. */
  static org.agentic.flink.llm.ChatResponse fromLangChainResponse(
      dev.langchain4j.model.chat.response.ChatResponse response, String modelName) {
    String text = response.aiMessage() != null ? response.aiMessage().text() : "";
    Long tokens =
        response.tokenUsage() == null ? null : (long) response.tokenUsage().totalTokenCount();
    org.agentic.flink.llm.ChatResponse.FinishReason fr =
        org.agentic.flink.llm.ChatResponse.FinishReason.STOP;
    if (response.finishReason() != null) {
      switch (response.finishReason()) {
        case STOP:
          fr = org.agentic.flink.llm.ChatResponse.FinishReason.STOP;
          break;
        case LENGTH:
          fr = org.agentic.flink.llm.ChatResponse.FinishReason.LENGTH;
          break;
        case TOOL_EXECUTION:
          fr = org.agentic.flink.llm.ChatResponse.FinishReason.TOOL_CALLS;
          break;
        case CONTENT_FILTER:
          fr = org.agentic.flink.llm.ChatResponse.FinishReason.CONTENT_FILTER;
          break;
        default:
          fr = org.agentic.flink.llm.ChatResponse.FinishReason.UNKNOWN;
      }
    }
    return new org.agentic.flink.llm.ChatResponse(
        text, modelName, java.util.Collections.emptyList(), tokens, fr);
  }
}
