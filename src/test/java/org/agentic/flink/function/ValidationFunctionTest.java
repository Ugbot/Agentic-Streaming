package org.agentic.flink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.inference.ValidationVerdict;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.serde.ValidationResult;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@link ValidationFunction} must read the judge's verdict through the shared {@link
 * ValidationVerdict} parser: any wording of INVALID / NOT VALID fails the result, any wording of
 * VALID passes it, and a response without a verdict fails rather than passes.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ValidationFunctionTest {

  /** Returns one fixed judge response for every chat call. */
  static final class FixedVerdictConnection implements ChatConnection {
    private static final long serialVersionUID = 1L;
    private final String text;

    FixedVerdictConnection(String text) {
      this.text = text;
    }

    @Override
    public ChatClient bind(RuntimeContext runtimeContext) {
      return new ChatClient() {
        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          return new ChatResponse(text, "judge", List.of(), 0L, ChatResponse.FinishReason.STOP);
        }

        @Override
        public String providerName() {
          return "fixed-verdict";
        }
      };
    }
  }

  /** Captures the single completed event; the chat runs on the common pool, so callers await. */
  static final class CapturingResultFuture implements ResultFuture<AgentEvent> {
    final List<AgentEvent> events = new CopyOnWriteArrayList<>();
    final CountDownLatch done = new CountDownLatch(1);
    volatile Throwable error;

    @Override
    public void complete(Collection<AgentEvent> result) {
      events.addAll(result);
      done.countDown();
    }

    @Override
    public void completeExceptionally(Throwable e) {
      error = e;
      done.countDown();
    }

    @Override
    public void complete(CollectionSupplier<AgentEvent> supplier) {
      try {
        complete(supplier.get());
      } catch (Throwable t) {
        completeExceptionally(t);
      }
    }
  }

  private static final String[] INVALID_TEMPLATES = {
    "%s: the result is wrong because %s",
    "Verdict: %s\nScore: 0.%d\nReason: %s",
    "After checking, the output is %s. %s",
    "%s",
    "I would say the tool result is %s given %s",
  };

  private static final String[] VALID_TEMPLATES = {
    "%s: the result is correct, %s",
    "Verdict: %s\nScore: 0.%d\nReason: %s",
    "After checking, the output is %s. %s",
    "%s",
    "The tool result looks %s to me since %s",
  };

  private static String randomCase(String word) {
    StringBuilder sb = new StringBuilder();
    for (char c : word.toCharArray()) {
      sb.append(
          ThreadLocalRandom.current().nextBoolean()
              ? Character.toUpperCase(c)
              : Character.toLowerCase(c));
    }
    return sb.toString();
  }

  private static String fill(String template, String verdict) {
    String reason = "reason-" + UUID.randomUUID();
    int digit = ThreadLocalRandom.current().nextInt(1, 10);
    if (template.contains("%d")) {
      return String.format(template, verdict, digit, reason);
    }
    if (template.chars().filter(c -> c == '%').count() == 2) {
      return String.format(template, verdict, reason);
    }
    return String.format(template, verdict);
  }

  private static String randomInvalidWording() {
    String[] words = {"INVALID", "NOT VALID", "not  valid", "Not-Valid", "NOT_VALID"};
    String word = randomCase(words[ThreadLocalRandom.current().nextInt(words.length)]);
    return fill(
        INVALID_TEMPLATES[ThreadLocalRandom.current().nextInt(INVALID_TEMPLATES.length)], word);
  }

  private static String randomValidWording() {
    String word = randomCase("VALID");
    return fill(VALID_TEMPLATES[ThreadLocalRandom.current().nextInt(VALID_TEMPLATES.length)], word);
  }

  private static AgentEvent eventWithResult(Object result) {
    AgentEvent e =
        new AgentEvent(
            "flow-" + UUID.randomUUID(),
            "user-" + UUID.randomUUID(),
            "agent-" + UUID.randomUUID(),
            AgentEventType.TOOL_CALL_COMPLETED);
    if (result != null) {
      e.putData("result", result);
    }
    return e;
  }

  private static ValidationFunction opened(String judgeResponse) throws Exception {
    ValidationFunction fn = new ValidationFunction(null, new FixedVerdictConnection(judgeResponse));
    ValidationFunction copy = InstantiationUtil.clone(fn);
    copy.setRuntimeContext(new MockStreamingRuntimeContext(1, 0));
    copy.open(null);
    return copy;
  }

  private static AgentEvent validate(String judgeResponse, AgentEvent input) throws Exception {
    ValidationFunction fn = opened(judgeResponse);
    CapturingResultFuture out = new CapturingResultFuture();
    fn.asyncInvoke(input, out);
    assertTrue(out.done.await(10, TimeUnit.SECONDS), "validation completed");
    if (out.error != null) {
      throw new AssertionError(out.error);
    }
    assertEquals(1, out.events.size(), "exactly one validation event");
    return out.events.get(0);
  }

  private static ValidationResult result(AgentEvent e) {
    ValidationResult r = e.getData("validationResult", ValidationResult.class);
    assertNotNull(r);
    return r;
  }

  @Test
  @DisplayName(
      "every INVALID / NOT VALID wording produces VALIDATION_FAILED with the judge text as error")
  void invalidWordingsFail() throws Exception {
    for (int i = 0; i < 40; i++) {
      String judge = randomInvalidWording();
      AgentEvent in = eventWithResult("result-" + UUID.randomUUID());
      AgentEvent out = validate(judge, in);
      assertEquals(AgentEventType.VALIDATION_FAILED, out.getEventType(), judge);
      ValidationResult r = result(out);
      assertFalse(r.isValid(), judge);
      assertEquals(0.0, r.getValidationScore(), 1e-9, judge);
      assertTrue(r.hasErrors(), judge);
      assertTrue(r.getErrors().get(0).contains(judge), judge);
      assertEquals(in.getFlowId(), out.getFlowId());
      assertEquals(in.getData("result"), out.getData("originalResult"));
    }
  }

  @Test
  @DisplayName("every VALID wording produces VALIDATION_PASSED with no errors")
  void validWordingsPass() throws Exception {
    for (int i = 0; i < 40; i++) {
      String judge = randomValidWording();
      AgentEvent out = validate(judge, eventWithResult("result-" + UUID.randomUUID()));
      assertEquals(AgentEventType.VALIDATION_PASSED, out.getEventType(), judge);
      ValidationResult r = result(out);
      assertTrue(r.isValid(), judge);
      assertFalse(r.hasErrors(), judge);
      assertEquals(ValidationVerdict.parse(judge).getScore(), r.getValidationScore(), 1e-9, judge);
    }
  }

  @Test
  @DisplayName("a judge response without a verdict fails validation instead of passing it")
  void noVerdictFails() throws Exception {
    String judge = "I looked at it: " + UUID.randomUUID();
    AgentEvent out = validate(judge, eventWithResult("result-" + UUID.randomUUID()));
    assertEquals(AgentEventType.VALIDATION_FAILED, out.getEventType());
    ValidationResult r = result(out);
    assertFalse(r.isValid());
    assertTrue(r.getErrors().get(0).contains("no VALID/INVALID verdict"), r.getErrors().toString());
  }

  @Test
  @DisplayName("an event with no result passes through as VALIDATION_PASSED without a chat call")
  void missingResultPassesThrough() throws Exception {
    AgentEvent out = validate("INVALID would be ignored", eventWithResult(null));
    assertEquals(AgentEventType.VALIDATION_PASSED, out.getEventType());
    assertTrue(result(out).isValid());
  }

  @Test
  @DisplayName("timeout marks the result invalid")
  void timeoutFails() throws Exception {
    ValidationFunction fn = opened("VALID");
    CapturingResultFuture out = new CapturingResultFuture();
    fn.timeout(eventWithResult("r"), out);
    assertEquals(1, out.events.size());
    assertEquals(AgentEventType.VALIDATION_FAILED, out.events.get(0).getEventType());
    assertEquals("Validation timeout", result(out.events.get(0)).getErrors().get(0));
  }
}
