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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationFunction.class);
  public static final String UID = ValidationFunction.class.getSimpleName();

  private transient LangChainAsyncClient langChainAsyncClient;
  private transient PromptTemplateManager promptManager;
  private final String customTemplateId;

  /**
   * Creates a ValidationFunction using the default validation template.
   */
  public ValidationFunction() {
    this(null);
  }

  /**
   * Creates a ValidationFunction with a custom template ID.
   *
   * @param customTemplateId Custom template ID (null to use default "validation" template)
   */
  public ValidationFunction(String customTemplateId) {
    this.customTemplateId = customTemplateId;
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

    LOG.info("Validating result for flow: {}", event.getFlowId());

    // Extract tool result from event
    Object toolResult = event.getData("result");
    if (toolResult == null) {
      // No result to validate, pass through
      LOG.warn("No result found to validate for flow: {}", event.getFlowId());
      AgentEvent validationEvent = createValidationEvent(event, true, 1.0, null);
      resultFuture.complete(Collections.singleton(validationEvent));
      return;
    }

    // Build validation prompt using PromptTemplateManager
    String templateId = customTemplateId != null ? customTemplateId : "validation";
    Prompt validationPrompt = promptManager.renderTemplate(templateId, "result", toolResult);

    List<ChatMessage> messages = new ArrayList<>();
    messages.add(new UserMessage(validationPrompt.text()));

    // Create LLM config
    LLMConfig llmConfig = createLLMConfig(event);

    // Execute validation async
    CompletableFuture<Response<AiMessage>> asyncResponse =
        langChainAsyncClient.generate(messages, llmConfig);

    processValidationResponse(event, resultFuture, asyncResponse);
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.warn("Validation timed out for flow: {}", input.getFlowId());

    // On timeout, mark as invalid and require correction
    AgentEvent validationEvent = createValidationEvent(input, false, 0.0, "Validation timeout");
    resultFuture.complete(Collections.singleton(validationEvent));
  }

  private LLMConfig createLLMConfig(AgentEvent event) {
    LLMConfig config = new LLMConfig();
    config.setUserId(Long.valueOf(event.getUserId().hashCode()));
    config.setAiModel(AiModel.OLLAMA);
    config.setSystemMessage(
        "You are a validation assistant. Evaluate tool execution results for correctness and completeness.");
    return config;
  }

  private void processValidationResponse(
      AgentEvent event,
      ResultFuture<AgentEvent> resultFuture,
      CompletableFuture<Response<AiMessage>> asyncResponse) {

    asyncResponse.whenComplete(
        (result, throwable) -> {
          if (throwable != null) {
            LOG.error("Validation failed for flow: {}", event.getFlowId(), throwable);
            AgentEvent validationEvent =
                createValidationEvent(event, false, 0.0, "Validation execution error");
            resultFuture.complete(Collections.singleton(validationEvent));
            return;
          }

          // Parse validation result
          String validationResponse = result.content().text();
          boolean isValid = validationResponse.toUpperCase().contains("VALID");
          double score = isValid ? 1.0 : 0.0;

          LOG.info(
              "Validation completed for flow: {}, isValid: {}", event.getFlowId(), isValid);

          AgentEvent validationEvent =
              createValidationEvent(
                  event, isValid, score, isValid ? null : validationResponse);
          resultFuture.complete(Collections.singleton(validationEvent));
        });
  }

  private AgentEvent createValidationEvent(
      AgentEvent originalEvent, boolean isValid, double score, String errorMessage) {

    ValidationResult validation =
        new ValidationResult(
            originalEvent.getFlowId(), originalEvent.getUserId(), originalEvent.getAgentId());
    validation.setValid(isValid);
    validation.setValidationScore(score);
    if (!isValid && errorMessage != null) {
      validation.addError(errorMessage);
    }

    AgentEvent event = new AgentEvent();
    event.setFlowId(originalEvent.getFlowId());
    event.setUserId(originalEvent.getUserId());
    event.setAgentId(originalEvent.getAgentId());
    event.setEventType(
        isValid ? AgentEventType.VALIDATION_PASSED : AgentEventType.VALIDATION_FAILED);
    event.setTimestamp(System.currentTimeMillis());
    event.setCurrentStage(originalEvent.getCurrentStage());
    event.setIterationNumber(originalEvent.getIterationNumber());
    event.putData("validationResult", validation);
    event.putData("originalResult", originalEvent.getData("result"));

    return event;
  }
}
