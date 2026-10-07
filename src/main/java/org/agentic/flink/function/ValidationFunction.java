package org.agentic.flink.function;

import dev.langchain4j.model.input.Prompt;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.agentic.flink.annotation.Internal;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.inference.ValidationVerdict;
import org.agentic.flink.langchain.PromptTemplateManager;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.serde.ValidationResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class ValidationFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationFunction.class);
  public static final String UID = ValidationFunction.class.getSimpleName();

  private static final String SYSTEM_MESSAGE =
      "You are a validation assistant. Evaluate tool execution results for correctness "
          + "and completeness.";

  private transient ChatClient chatClient;
  private transient PromptTemplateManager promptManager;
  private transient ChatSetup chatSetup;
  private final String customTemplateId;
  private final ChatConnection chatConnection;

  /** Creates a ValidationFunction using the default validation template. */
  public ValidationFunction() {
    this(null);
  }

  /**
   * Creates a ValidationFunction with a custom template ID.
   *
   * @param customTemplateId Custom template ID (null to use default "validation" template)
   */
  public ValidationFunction(String customTemplateId) {
    this(customTemplateId, null);
  }

  /**
   * Creates a ValidationFunction bound to an explicit chat provider.
   *
   * @param customTemplateId Custom template ID (null to use default "validation" template)
   * @param chatConnection provider to validate with; {@code null} discovers one via ServiceLoader
   */
  public ValidationFunction(String customTemplateId, ChatConnection chatConnection) {
    this.customTemplateId = customTemplateId;
    this.chatConnection = chatConnection;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    ChatConnection connection = chatConnection;
    if (connection == null) {
      connection = ChatConnections.require("ValidationFunction");
    }
    this.chatClient = connection.bind(getRuntimeContext());
    this.promptManager = PromptTemplateManager.getInstance();
    // Validation should be deterministic.
    this.chatSetup =
        ChatSetup.builder().withModel(ConfigKeys.DEFAULT_OLLAMA_MODEL).withTemperature(0.1).build();
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
    messages.add(ChatMessage.system(SYSTEM_MESSAGE));
    messages.add(ChatMessage.user(validationPrompt.text()));

    // Execute validation async
    CompletableFuture<ChatResponse> asyncResponse = chatClient.chatAsync(messages, chatSetup);

    processValidationResponse(event, resultFuture, asyncResponse);
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.warn("Validation timed out for flow: {}", input.getFlowId());

    // On timeout, mark as invalid and require correction
    AgentEvent validationEvent = createValidationEvent(input, false, 0.0, "Validation timeout");
    resultFuture.complete(Collections.singleton(validationEvent));
  }

  private void processValidationResponse(
      AgentEvent event,
      ResultFuture<AgentEvent> resultFuture,
      CompletableFuture<ChatResponse> asyncResponse) {

    asyncResponse.whenComplete(
        (result, throwable) -> {
          if (throwable != null) {
            LOG.error("Validation failed for flow: {}", event.getFlowId(), throwable);
            AgentEvent validationEvent =
                createValidationEvent(event, false, 0.0, "Validation execution error");
            resultFuture.complete(Collections.singleton(validationEvent));
            return;
          }

          String validationResponse = result == null ? null : result.getText();
          ValidationVerdict verdict = ValidationVerdict.parse(validationResponse);
          boolean isValid = verdict.isValid();

          LOG.info("Validation completed for flow: {}, verdict: {}", event.getFlowId(), verdict);

          AgentEvent validationEvent =
              createValidationEvent(
                  event,
                  isValid,
                  isValid ? verdict.getScore() : 0.0,
                  isValid ? null : errorMessage(verdict));
          resultFuture.complete(Collections.singleton(validationEvent));
        });
  }

  private static String errorMessage(ValidationVerdict verdict) {
    if (verdict.getOutcome() == ValidationVerdict.Outcome.UNDETERMINED) {
      return "Validator returned no VALID/INVALID verdict: " + verdict.getRawResponse();
    }
    return verdict.getRawResponse();
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
