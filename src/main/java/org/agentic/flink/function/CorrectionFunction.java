package org.agentic.flink.function;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.langchain.PromptTemplateManager;
import org.agentic.flink.serde.ValidationResult;
import org.agentic.flink.langchain.client.LangChainAsyncClient;
import org.agentic.flink.langchain.model.AiModel;
import org.agentic.flink.langchain.model.language.LangChainLanguageModel;
import org.agentic.flink.langchain.model.language.OllamaLanguageModel;
import org.agentic.flink.langchain.model.language.OpenAiLanguageModel;
import org.agentic.flink.langchain.config.LLMConfig;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.input.Prompt;
import dev.langchain4j.model.output.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorrectionFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(CorrectionFunction.class);
  public static final String UID = CorrectionFunction.class.getSimpleName();

  private transient LangChainAsyncClient langChainAsyncClient;
  private transient PromptTemplateManager promptManager;
  private final String customTemplateId;
  private final int maxCorrectionAttempts;

  /**
   * Creates a CorrectionFunction with default template and max attempts.
   *
   * @param maxCorrectionAttempts Maximum number of correction attempts
   */
  public CorrectionFunction(int maxCorrectionAttempts) {
    this(null, maxCorrectionAttempts);
  }

  /**
   * Creates a CorrectionFunction with custom template.
   *
   * @param customTemplateId Custom template ID (null to use default "correction" template)
   * @param maxCorrectionAttempts Maximum number of correction attempts
   */
  public CorrectionFunction(String customTemplateId, int maxCorrectionAttempts) {
    this.customTemplateId = customTemplateId;
    this.maxCorrectionAttempts = maxCorrectionAttempts;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    this.langChainAsyncClient =
        new LangChainAsyncClient(
            List.of(
                LangChainLanguageModel.DEFAULT_MODEL,
                new OllamaLanguageModel(),
                new OpenAiLanguageModel()));
    this.promptManager = PromptTemplateManager.getInstance();
  }

  @Override
  public void asyncInvoke(AgentEvent event, ResultFuture<AgentEvent> resultFuture) {

    LOG.info("Attempting correction for flow: {}", event.getFlowId());

    // Check if we've exceeded max attempts
    Integer attemptCount = event.getData("correctionAttempts", Integer.class);
    if (attemptCount == null) {
      attemptCount = 0;
    }

    if (attemptCount >= maxCorrectionAttempts) {
      LOG.warn(
          "Max correction attempts reached for flow: {}, escalating to supervisor",
          event.getFlowId());
      AgentEvent supervisorEvent = createSupervisorEscalationEvent(event);
      resultFuture.complete(Collections.singleton(supervisorEvent));
      return;
    }

    // Extract validation result and original result
    ValidationResult validation = event.getData("validationResult", ValidationResult.class);
    Object originalResult = event.getData("originalResult");

    // Build correction prompt using PromptTemplateManager
    String templateId = customTemplateId != null ? customTemplateId : "correction";
    Map<String, Object> variables = new HashMap<>();
    variables.put("result", originalResult != null ? originalResult.toString() : "");
    variables.put("errors", validation != null ? String.join(", ", validation.getErrors()) : "Unknown errors");

    Prompt correctionPrompt = promptManager.renderTemplate(templateId, variables);

    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new UserMessage(correctionPrompt.text()));

    // Create LLM config
    LLMConfig llmConfig = createLLMConfig(event);

    // Execute correction async
    CompletableFuture<Response<AiMessage>> asyncResponse =
        langChainAsyncClient.generate(messages, llmConfig);

    processCorrectionResponse(event, attemptCount + 1, resultFuture, asyncResponse);
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.warn("Correction timed out for flow: {}", input.getFlowId());

    // On timeout, escalate to supervisor
    AgentEvent supervisorEvent = createSupervisorEscalationEvent(input);
    resultFuture.complete(Collections.singleton(supervisorEvent));
  }

  private LLMConfig createLLMConfig(AgentEvent event) {
    LLMConfig config = new LLMConfig();
    config.setUserId(Long.valueOf(event.getUserId().hashCode()));
    config.setAiModel(AiModel.OLLAMA);
    config.setSystemMessage(
        "You are a correction assistant. Fix errors in tool execution results based on validation feedback.");
    return config;
  }

  private void processCorrectionResponse(
      AgentEvent event,
      int attemptCount,
      ResultFuture<AgentEvent> resultFuture,
      CompletableFuture<Response<AiMessage>> asyncResponse) {

    asyncResponse.whenComplete(
        (result, throwable) -> {
          if (throwable != null) {
            LOG.error("Correction failed for flow: {}", event.getFlowId(), throwable);
            AgentEvent supervisorEvent = createSupervisorEscalationEvent(event);
            resultFuture.complete(Collections.singleton(supervisorEvent));
            return;
          }

          String correctedResult = result.content().text();
          LOG.info(
              "Correction attempt {} completed for flow: {}", attemptCount, event.getFlowId());

          AgentEvent correctionEvent = new AgentEvent();
          correctionEvent.setFlowId(event.getFlowId());
          correctionEvent.setUserId(event.getUserId());
          correctionEvent.setAgentId(event.getAgentId());
          correctionEvent.setEventType(AgentEventType.CORRECTION_COMPLETED);
          correctionEvent.setTimestamp(System.currentTimeMillis());
          correctionEvent.setCurrentStage(event.getCurrentStage());
          correctionEvent.setIterationNumber(event.getIterationNumber());
          correctionEvent.putData("correctedResult", correctedResult);
          correctionEvent.putData("correctionAttempts", attemptCount);
          correctionEvent.putData("result", correctedResult); // Replace original result

          resultFuture.complete(Collections.singleton(correctionEvent));
        });
  }

  private AgentEvent createSupervisorEscalationEvent(AgentEvent event) {
    AgentEvent supervisorEvent = new AgentEvent();
    supervisorEvent.setFlowId(event.getFlowId());
    supervisorEvent.setUserId(event.getUserId());
    supervisorEvent.setAgentId(event.getAgentId());
    supervisorEvent.setEventType(AgentEventType.SUPERVISOR_REVIEW_REQUESTED);
    supervisorEvent.setTimestamp(System.currentTimeMillis());
    supervisorEvent.setCurrentStage(event.getCurrentStage());
    supervisorEvent.setIterationNumber(event.getIterationNumber());
    supervisorEvent.putData("reason", "Max correction attempts exceeded");
    supervisorEvent.putData("originalResult", event.getData("originalResult"));
    supervisorEvent.putData("validationResult", event.getData("validationResult"));
    return supervisorEvent;
  }
}
