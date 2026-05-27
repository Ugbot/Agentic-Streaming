package org.agentic.flink.feedback;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;

/**
 * {@link QualityCheck} that asks a second LLM ("critic") to score the output 0–1 and explain what's
 * wrong. The critic is told to begin its reply with {@code SCORE: <0.0-1.0>} followed by a one-line
 * critique. Passes when the parsed score meets the threshold; the critique is fed back to the
 * generator. The critic {@link ChatClient} binds lazily and is {@code transient}.
 */
public final class LlmCriticQualityCheck implements QualityCheck {
  private static final long serialVersionUID = 1L;

  private static final Pattern SCORE = Pattern.compile("SCORE\\s*[:=]?\\s*(\\d*\\.?\\d+)");

  private final ChatConnection connection;
  private final ChatSetup setup;
  private final double threshold;
  private final String rubric;
  private transient ChatClient critic;

  public LlmCriticQualityCheck(ChatConnection connection, ChatSetup setup, double threshold, String rubric) {
    this.connection = connection;
    this.setup = setup;
    this.threshold = threshold;
    this.rubric = rubric;
  }

  /** Claude critic from an Anthropic key, default model + rubric. */
  public static LlmCriticQualityCheck claude(String apiKey, double threshold, String rubric) {
    ChatSetup s =
        ChatSetup.builder()
            .withModel(ConfigKeys.DEFAULT_ANTHROPIC_MODEL)
            .withTemperature(0.0)
            .withMaxResponseTokens(256)
            .build();
    return new LlmCriticQualityCheck(LangChain4jChatConnection.anthropic(apiKey), s, threshold, rubric);
  }

  private ChatClient critic() {
    if (critic == null) {
      try {
        critic = connection.bind(null);
      } catch (Exception e) {
        throw new RuntimeException("Failed to bind critic: " + e.getMessage(), e);
      }
    }
    return critic;
  }

  @Override
  public CheckResult check(String task, String output) {
    String system =
        "You are a strict reviewer. Score the candidate answer from 0.0 (poor) to 1.0 (excellent) "
            + "against the task"
            + (rubric == null || rubric.isBlank() ? "" : " and this rubric: " + rubric)
            + ". Reply on the first line with exactly 'SCORE: <number>' then a one-line critique of "
            + "what to improve.";
    String user = "Task:\n" + task + "\n\nCandidate answer:\n" + output;
    ChatResponse resp =
        critic().chat(List.of(ChatMessage.system(system), ChatMessage.user(user)), setup);
    String text = resp.getText() == null ? "" : resp.getText().trim();
    double score = parseScore(text);
    boolean passed = score >= threshold;
    if (passed) return CheckResult.pass(score);
    return CheckResult.fail(score, critiqueOf(text));
  }

  static double parseScore(String text) {
    Matcher m = SCORE.matcher(text.toUpperCase(Locale.ROOT));
    if (m.find()) {
      try {
        return Math.max(0.0, Math.min(1.0, Double.parseDouble(m.group(1))));
      } catch (NumberFormatException ignore) {
        // fall through
      }
    }
    return 0.0; // unparseable → treat as failing so the loop keeps trying
  }

  private static String critiqueOf(String text) {
    // Everything after the first newline (or the whole thing if single line).
    int nl = text.indexOf('\n');
    return nl >= 0 ? text.substring(nl + 1).trim() : text;
  }
}
