package org.agentic.flink.feedback;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatRole;
import org.agentic.flink.llm.ChatSetup;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class LlmCriticQualityCheckTest {

  private static final ChatSetup SETUP = ChatSetup.builder().withModel("critic-model").build();

  /** A critic that always answers with the given reply and records every prompt it received. */
  private static final class ScriptedCritic implements ChatConnection {
    private static final long serialVersionUID = 1L;
    private final String reply;
    private transient List<List<ChatMessage>> prompts;
    private transient AtomicInteger binds;

    ScriptedCritic(String reply) {
      this.reply = reply;
    }

    List<List<ChatMessage>> prompts() {
      if (prompts == null) prompts = new ArrayList<>();
      return prompts;
    }

    int binds() {
      if (binds == null) binds = new AtomicInteger();
      return binds.get();
    }

    @Override
    public ChatClient bind(RuntimeContext runtimeContext) {
      if (binds == null) binds = new AtomicInteger();
      binds.incrementAndGet();
      return new ChatClient() {
        @Override
        public ChatResponse chat(List<ChatMessage> messages, ChatSetup setup) {
          prompts().add(messages);
          return new ChatResponse(reply, "scripted", List.of(), 0L, ChatResponse.FinishReason.STOP);
        }

        @Override
        public String providerName() {
          return "scripted";
        }
      };
    }
  }

  private static double randomScore(double lo, double hi) {
    double v = ThreadLocalRandom.current().nextDouble(lo, hi);
    return Math.round(v * 100.0) / 100.0;
  }

  @Test
  void passesWhenTheCriticScoreMeetsTheThreshold() {
    double threshold = randomScore(0.3, 0.6);
    double score = randomScore(threshold, 1.0);
    ScriptedCritic critic =
        new ScriptedCritic(String.format(Locale.ROOT, "SCORE: %.2f%nlooks fine", score));
    LlmCriticQualityCheck check = new LlmCriticQualityCheck(critic, SETUP, threshold, null);

    CheckResult result = check.check("task-" + UUID.randomUUID(), "answer-" + UUID.randomUUID());

    assertTrue(result.passed);
    assertEquals(score, result.score, 1e-9);
    assertNull(result.critique);
  }

  @Test
  void failsBelowTheThresholdAndFeedsTheCritiqueBack() {
    double threshold = randomScore(0.5, 0.9);
    double score = randomScore(0.0, threshold - 0.05);
    String critique = "critique-" + UUID.randomUUID();
    ScriptedCritic critic =
        new ScriptedCritic(String.format(Locale.ROOT, "score=%.2f%n%s", score, critique));
    LlmCriticQualityCheck check = new LlmCriticQualityCheck(critic, SETUP, threshold, "be concise");

    CheckResult result = check.check("task", "answer");

    assertFalse(result.passed);
    assertEquals(score, result.score, 1e-9);
    assertEquals(critique, result.critique);
  }

  @Test
  void unparseableReplyScoresZeroSoTheLoopKeepsRefining() {
    String reply = "no numeric verdict " + UUID.randomUUID();
    LlmCriticQualityCheck check =
        new LlmCriticQualityCheck(new ScriptedCritic(reply), SETUP, randomScore(0.01, 1.0), null);

    CheckResult result = check.check("task", "answer");

    assertFalse(result.passed);
    assertEquals(0.0, result.score, 0.0);
    assertEquals(reply, result.critique);
    assertEquals(1.0, LlmCriticQualityCheck.parseScore("SCORE: 7"), 0.0);
    assertEquals(0.0, LlmCriticQualityCheck.parseScore("SCORE: -3"), 0.0);
  }

  @Test
  void promptCarriesTaskOutputAndRubricAndTheCriticBindsOnce() {
    String task = "task-" + UUID.randomUUID();
    String output = "output-" + UUID.randomUUID();
    String rubric = "rubric-" + UUID.randomUUID();
    ScriptedCritic critic = new ScriptedCritic("SCORE: 1.0");
    LlmCriticQualityCheck check = new LlmCriticQualityCheck(critic, SETUP, 0.5, rubric);

    int calls = ThreadLocalRandom.current().nextInt(2, 5);
    for (int i = 0; i < calls; i++) {
      check.check(task, output);
    }

    assertEquals(1, critic.binds(), "critic client is bound lazily and reused");
    assertEquals(calls, critic.prompts().size());
    List<ChatMessage> prompt = critic.prompts().get(0);
    assertEquals(ChatRole.SYSTEM, prompt.get(0).getRole());
    assertTrue(prompt.get(0).getContent().contains(rubric));
    assertEquals(ChatRole.USER, prompt.get(1).getRole());
    assertTrue(prompt.get(1).getContent().contains(task));
    assertTrue(prompt.get(1).getContent().contains(output));
  }

  @Test
  void isSerializableWithTheBoundCriticLeftBehind() throws Exception {
    LlmCriticQualityCheck check =
        new LlmCriticQualityCheck(new ScriptedCritic("SCORE: 0.9"), SETUP, 0.5, null);
    check.check("warm", "up");

    LlmCriticQualityCheck copy =
        InstantiationUtil.deserializeObject(
            InstantiationUtil.serializeObject(check), getClass().getClassLoader());

    assertTrue(copy.check("task", "answer").passed);
  }
}
