package org.agentic.flink.cascade;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.inference.Classifier;
import org.agentic.flink.inference.ClassificationResult;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.LexiconInferenceConnection;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Three-tier escalation cascade: a cheap deterministic <b>filter</b> screens everything, only
 * matches are <b>confirmed</b> by an ML classifier, and only ML-confirmed cases escalate to an
 * <b>LLM</b> that decides the action. Each tier can terminate early, so the expensive stages only
 * run on the small fraction of traffic that survives the cheaper ones — "this looks suspicious,
 * confirm suspicious, act accordingly".
 *
 * <ul>
 *   <li><b>FILTER</b> — substring pre-screen (no model). No trigger term present → {@code CLEAN}.
 *   <li><b>ML</b> — {@link Classifier} score; below {@code mlThreshold} → {@code CLEAN} (the
 *       filter was a false positive). At/above threshold → escalate.
 *   <li><b>LLM</b> — a {@link ChatConnection} (Claude by default) returns the final verdict
 *       ({@code ALLOW} / {@code REVIEW} / {@code BLOCK}) with a rationale. If no chat model is
 *       configured the cascade stops at ML with verdict {@code REVIEW} (human-in-the-loop).
 * </ul>
 *
 * <p>In-JVM and synchronous so it is easy to drive from a notebook or wrap in a Flink operator.
 */
public final class EscalationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(EscalationPipeline.class);

  /** Which tier produced the final decision. */
  public enum Tier {
    FILTER,
    ML,
    LLM
  }

  private final Predicate<String> filter;
  private final Classifier classifier;
  private final InferenceSetup inferenceSetup;
  private final double mlThreshold;
  private final ChatClient chat; // nullable
  private final ChatSetup chatSetup;

  private EscalationPipeline(Builder b) {
    this.filter = b.filter;
    this.inferenceSetup = b.inferenceSetup;
    this.mlThreshold = b.mlThreshold;
    this.chatSetup = b.chatSetup;
    try {
      this.classifier = b.inferenceConnection.bind(null).asClassifier();
      this.chat = b.chatConnection == null ? null : b.chatConnection.bind(null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build EscalationPipeline: " + e.getMessage(), e);
    }
  }

  /** Run one item through the cascade. */
  public Decision evaluate(String input) {
    // Tier 1 — cheap filter.
    if (!filter.test(input == null ? "" : input)) {
      return new Decision(input, Tier.FILTER, "CLEAN", false, null, 0.0, null,
          "No trigger terms present; not screened further");
    }

    // Tier 2 — ML confirmation.
    ClassificationResult ml = classifier.classify(input, inferenceSetup);
    if (ml.getScore() < mlThreshold) {
      return new Decision(input, Tier.ML, "CLEAN", false, ml.getLabel(), ml.getScore(), null,
          String.format(Locale.ROOT,
              "Filter matched but ML score %.2f < threshold %.2f", ml.getScore(), mlThreshold));
    }

    // Tier 3 — LLM action. If no chat model, stop at ML and ask for human review.
    if (chat == null) {
      return new Decision(input, Tier.ML, "REVIEW", true, ml.getLabel(), ml.getScore(), null,
          String.format(Locale.ROOT,
              "ML confirmed suspicious (%.2f) — no LLM configured, routing to human review",
              ml.getScore()));
    }

    String system =
        "You are a security triage analyst. A cheap filter and an ML classifier have already "
            + "flagged the item below as potentially suspicious. Decide the action. Begin your "
            + "reply with exactly one of ALLOW, REVIEW, or BLOCK, then a one-sentence rationale.";
    String user =
        String.format(Locale.ROOT,
            "ML label: %s (score %.2f)\nItem:\n%s", ml.getLabel(), ml.getScore(), input);
    List<ChatMessage> messages = new ArrayList<>();
    messages.add(ChatMessage.system(system));
    messages.add(ChatMessage.user(user));
    ChatResponse resp = chat.chat(messages, chatSetup);
    String text = resp.getText() == null ? "" : resp.getText().trim();
    String verdict = parseVerdict(text);
    return new Decision(input, Tier.LLM, verdict, !"ALLOW".equals(verdict),
        ml.getLabel(), ml.getScore(), text, "LLM adjudicated the ML-confirmed case");
  }

  private static String parseVerdict(String text) {
    String upper = text.toUpperCase(Locale.ROOT);
    // Take whichever verdict token appears first.
    int block = upper.indexOf("BLOCK");
    int review = upper.indexOf("REVIEW");
    int allow = upper.indexOf("ALLOW");
    String best = "REVIEW";
    int bestPos = Integer.MAX_VALUE;
    if (block >= 0 && block < bestPos) { bestPos = block; best = "BLOCK"; }
    if (review >= 0 && review < bestPos) { bestPos = review; best = "REVIEW"; }
    if (allow >= 0 && allow < bestPos) { bestPos = allow; best = "ALLOW"; }
    return best;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Predicate<String> filter = defaultFilter();
    private InferenceConnection inferenceConnection = new LexiconInferenceConnection();
    private InferenceSetup inferenceSetup =
        InferenceSetup.builder().withModelName("lexicon").withModelUri("lexicon://default").build();
    private double mlThreshold = 0.5;
    private ChatConnection chatConnection;
    private ChatSetup chatSetup =
        ChatSetup.builder()
            .withModel(ConfigKeys.DEFAULT_ANTHROPIC_MODEL)
            .withTemperature(0.0)
            .withMaxResponseTokens(256)
            .build();

    /** Default cheap filter: input contains any default lexicon trigger term. */
    private static Predicate<String> defaultFilter() {
      List<String> terms = LexiconInferenceConnection.defaultTerms();
      return input -> {
        if (input == null) return false;
        String t = input.toLowerCase(Locale.ROOT);
        for (String term : terms) {
          if (t.contains(term)) return true;
        }
        return false;
      };
    }

    public Builder withFilter(Predicate<String> filter) {
      this.filter = filter;
      return this;
    }

    public Builder withClassifier(InferenceConnection conn, InferenceSetup setup) {
      this.inferenceConnection = conn;
      if (setup != null) this.inferenceSetup = setup;
      return this;
    }

    public Builder withMlThreshold(double threshold) {
      this.mlThreshold = threshold;
      return this;
    }

    public Builder withChatConnection(ChatConnection conn, ChatSetup setup) {
      this.chatConnection = conn;
      if (setup != null) this.chatSetup = setup;
      return this;
    }

    public Builder withClaude(String apiKey) {
      this.chatConnection =
          org.agentic.flink.llm.langchain4j.LangChain4jChatConnection.anthropic(apiKey);
      return this;
    }

    public EscalationPipeline build() {
      return new EscalationPipeline(this);
    }
  }

  /** The outcome of running one item through the cascade. */
  public static final class Decision implements Serializable {
    public final String input;
    public final Tier decidedBy;
    public final String verdict; // CLEAN / ALLOW / REVIEW / BLOCK
    public final boolean suspicious;
    public final String mlLabel; // null if not reached
    public final double mlScore;
    public final String llmRationale; // null if LLM not reached
    public final String reason;

    public Decision(String input, Tier decidedBy, String verdict, boolean suspicious,
        String mlLabel, double mlScore, String llmRationale, String reason) {
      this.input = input;
      this.decidedBy = decidedBy;
      this.verdict = verdict;
      this.suspicious = suspicious;
      this.mlLabel = mlLabel;
      this.mlScore = mlScore;
      this.llmRationale = llmRationale;
      this.reason = reason;
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT, "[%s] %s (mlScore=%.2f) — %s",
          decidedBy, verdict, mlScore, reason);
    }
  }
}
