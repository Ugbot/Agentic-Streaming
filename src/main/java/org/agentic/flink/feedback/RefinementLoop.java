package org.agentic.flink.feedback;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate → check → feed back → retry. A generator produces an answer, a {@link QualityCheck}
 * judges it, and if it falls short the critique is appended to the conversation and the generator
 * tries again — up to {@code maxAttempts}. Returns the accepted answer, or the best-scoring attempt
 * if the budget runs out. Mirrors {@link org.agentic.flink.cascade.EscalationPipeline}'s house
 * style (in-JVM, builder, single {@code refine} method, typed result).
 *
 * <p>This is the in-JVM, single-conversation form of the framework's {@code ValidationFunction} +
 * {@code CorrectionFunction} feedback loop.
 */
public final class RefinementLoop {

  private static final Logger LOG = LoggerFactory.getLogger(RefinementLoop.class);

  private final ChatClient generator;
  private final ChatSetup genSetup;
  private final String systemPrompt;
  private final QualityCheck check;
  private final int maxAttempts;

  private RefinementLoop(Builder b) {
    this.genSetup = b.genSetup;
    this.systemPrompt = b.systemPrompt;
    this.check = b.check;
    this.maxAttempts = b.maxAttempts;
    if (b.chatConnection == null) {
      throw new IllegalStateException(
          "RefinementLoop needs a generator ChatConnection (withChatConnection/withClaude)");
    }
    try {
      this.generator = b.chatConnection.bind(null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build RefinementLoop: " + e.getMessage(), e);
    }
  }

  /** Generate an answer for the task, refining against the check up to the attempt budget. */
  public RefinementResult refine(String task) {
    List<ChatMessage> conv = new ArrayList<>();
    conv.add(ChatMessage.system(systemPrompt));
    conv.add(ChatMessage.user(task));

    List<RefinementResult.Attempt> trace = new ArrayList<>();
    RefinementResult.Attempt best = null;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      String output = generator.chat(conv, genSetup).getText();
      output = output == null ? "" : output.trim();
      CheckResult cr = check.check(task, output);
      RefinementResult.Attempt a =
          new RefinementResult.Attempt(attempt, output, cr.score, cr.critique);
      trace.add(a);
      if (best == null || cr.score > best.score) {
        best = a;
      }
      if (cr.passed) {
        return new RefinementResult(output, true, attempt, trace);
      }
      if (attempt == maxAttempts) {
        break;
      }
      // Feed the prior answer + critique back for the next attempt.
      conv.add(ChatMessage.assistant(output));
      conv.add(
          ChatMessage.user(
              String.format(Locale.ROOT,
                  "That attempt scored %.2f. Issues: %s. Please revise to address these and "
                      + "improve the answer.",
                  cr.score, cr.critique)));
    }
    LOG.info("RefinementLoop exhausted {} attempts without passing; returning best-so-far", maxAttempts);
    return new RefinementResult(best.output, false, maxAttempts, trace);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private ChatConnection chatConnection;
    private ChatSetup genSetup =
        ChatSetup.builder()
            .withModel(ConfigKeys.DEFAULT_ANTHROPIC_MODEL)
            .withTemperature(0.3)
            .withMaxResponseTokens(512)
            .build();
    private String systemPrompt =
        "You produce the best possible answer to the user's task. When given feedback, revise to "
            + "address it.";
    private QualityCheck check = new KeywordQualityCheck(); // no-op gate by default
    private int maxAttempts = 3;

    public Builder withChatConnection(ChatConnection conn, ChatSetup setup) {
      this.chatConnection = conn;
      if (setup != null) this.genSetup = setup;
      return this;
    }

    public Builder withClaude(String apiKey) {
      this.chatConnection =
          org.agentic.flink.llm.langchain4j.LangChain4jChatConnection.anthropic(apiKey);
      return this;
    }

    public Builder withCheck(QualityCheck check) {
      this.check = check;
      return this;
    }

    public Builder withMaxAttempts(int maxAttempts) {
      if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder withSystemPrompt(String systemPrompt) {
      this.systemPrompt = systemPrompt;
      return this;
    }

    public RefinementLoop build() {
      return new RefinementLoop(this);
    }
  }
}
