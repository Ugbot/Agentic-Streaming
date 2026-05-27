package org.agentic.flink.screening;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.inference.ClassificationResult;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatConnection;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multiphase, layered screening cascade. Several cheap rule detectors (band-pass, repeated screen,
 * velocity) run first and their weighted signals sum into a combined risk; only items above a
 * review threshold pay for the ML classifier; only those still risky escalate to an LLM that
 * decides the action. Mirrors {@link org.agentic.flink.cascade.EscalationPipeline} but adds
 * stateful detectors ("the same payment three times in a row") and signal layering.
 *
 * <p>Tiers and exits:
 * <ul>
 *   <li><b>RULES</b> — no rule fired → {@code ALLOW}. Combined risk ≥ {@code blockThreshold} →
 *       {@code BLOCK} outright.</li>
 *   <li><b>ML</b> — a {@link org.agentic.flink.inference.Classifier} adds a score; if the combined
 *       risk is still below {@code reviewThreshold} → {@code ALLOW} (the rules were a false
 *       positive).</li>
 *   <li><b>LLM</b> — risk in the review band escalates to the chat model for an
 *       {@code ALLOW}/{@code REVIEW}/{@code BLOCK} verdict. With no chat configured, it routes to
 *       human {@code REVIEW}.</li>
 * </ul>
 *
 * <p>Holds bounded per-key history so the stateful detectors work in-process (notebook-friendly).
 * It is <b>not thread-safe</b>; in a Flink job keep history in keyed state instead (see
 * {@code PaymentScreeningExample}).
 */
public final class ScreeningPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ScreeningPipeline.class);

  private final List<Detector> ruleDetectors;
  private final ClassifierDetector classifierDetector; // nullable
  private final ChatClient chat; // nullable
  private final ChatSetup chatSetup;
  private final double reviewThreshold;
  private final double blockThreshold;

  private final History history;

  private ScreeningPipeline(Builder b) {
    this.ruleDetectors = List.copyOf(b.ruleDetectors);
    this.classifierDetector =
        b.classifierConnection == null
            ? null
            : new ClassifierDetector(b.classifierConnection, b.classifierSetup);
    this.chatSetup = b.chatSetup;
    this.reviewThreshold = b.reviewThreshold;
    this.blockThreshold = b.blockThreshold;
    this.history = new History(b.maxKeys, b.perKeyDepth);
    try {
      this.chat = b.chatConnection == null ? null : b.chatConnection.bind(null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to build ScreeningPipeline: " + e.getMessage(), e);
    }
  }

  /** Screen one item through the cascade, updating per-key history first. */
  public ScreeningResult screen(ScreenItem item) {
    history.record(item);
    ScreenContext ctx = history.contextAt(item.ts());

    // Tier 1 — rule detectors, layered into a combined risk.
    List<Signal> fired = new ArrayList<>();
    double risk = 0.0;
    for (Detector d : ruleDetectors) {
      Signal s = d.inspect(item, ctx);
      if (s != null) {
        fired.add(s);
        risk += s.weight();
      }
    }
    if (fired.isEmpty()) {
      return new ScreeningResult(item, fired, 0.0, ScreeningResult.Tier.RULES, "ALLOW",
          null, 0.0, null, "No rule fired");
    }

    // Tier 2 — ML classifier (only for items the rules flagged).
    String mlLabel = null;
    double mlScore = 0.0;
    if (classifierDetector != null) {
      ClassificationResult cr = classifierDetector.classify(item);
      mlLabel = cr.getLabel();
      mlScore = cr.getScore();
      fired.add(new Signal("classifier", Phase.CLASSIFIER, mlScore,
          String.format(Locale.ROOT, "classified '%s' (%.2f)", mlLabel, mlScore)));
      risk += mlScore;
    }
    ScreeningResult.Tier escalatedTier =
        classifierDetector != null ? ScreeningResult.Tier.ML : ScreeningResult.Tier.RULES;

    // Hard auto-block on overwhelming risk — no LLM needed.
    if (risk >= blockThreshold) {
      return new ScreeningResult(item, fired, risk, escalatedTier, "BLOCK",
          mlLabel, mlScore, null,
          String.format(Locale.ROOT, "Combined risk %.2f >= block threshold %.2f", risk, blockThreshold));
    }
    // Below review threshold — rules were a false positive.
    if (risk < reviewThreshold) {
      return new ScreeningResult(item, fired, risk, escalatedTier, "ALLOW",
          mlLabel, mlScore, null,
          String.format(Locale.ROOT, "Combined risk %.2f < review threshold %.2f", risk, reviewThreshold));
    }

    // Tier 3 — LLM adjudication for the review band.
    if (chat == null) {
      return new ScreeningResult(item, fired, risk, escalatedTier, "REVIEW",
          mlLabel, mlScore, null, "Risk in review band; no LLM configured — human review");
    }
    String system =
        "You are a risk analyst. Layered detectors flagged the item below. Decide the action. "
            + "Begin your reply with exactly one of ALLOW, REVIEW, or BLOCK, then a one-sentence rationale.";
    StringBuilder signalText = new StringBuilder();
    for (Signal s : fired) {
      signalText.append("- ").append(s.phase()).append(": ").append(s.reason()).append('\n');
    }
    String user =
        String.format(Locale.ROOT,
            "Combined risk: %.2f\nSignals:\n%sItem: key=%s value=%.2f label=%s attrs=%s",
            risk, signalText, item.key(), item.value(), item.label(), item.attrs());
    List<ChatMessage> messages = new ArrayList<>();
    messages.add(ChatMessage.system(system));
    messages.add(ChatMessage.user(user));
    ChatResponse resp = chat.chat(messages, chatSetup);
    String text = resp.getText() == null ? "" : resp.getText().trim();
    return new ScreeningResult(item, fired, risk, ScreeningResult.Tier.LLM, parseVerdict(text),
        mlLabel, mlScore, text, "LLM adjudicated the flagged item");
  }

  static String parseVerdict(String text) {
    String upper = text.toUpperCase(Locale.ROOT);
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
    private final List<Detector> ruleDetectors = new ArrayList<>();
    private InferenceConnection classifierConnection;
    private InferenceSetup classifierSetup =
        InferenceSetup.builder().withModelName("lexicon").withModelUri("lexicon://default").build();
    private ChatConnection chatConnection;
    private ChatSetup chatSetup =
        ChatSetup.builder()
            .withModel(ConfigKeys.DEFAULT_ANTHROPIC_MODEL)
            .withTemperature(0.0)
            .withMaxResponseTokens(256)
            .build();
    private double reviewThreshold = 0.5;
    private double blockThreshold = 2.0;
    private int maxKeys = 10_000;
    private int perKeyDepth = 32;

    public Builder addDetector(Detector detector) {
      this.ruleDetectors.add(detector);
      return this;
    }

    public Builder withClassifier(InferenceConnection conn, InferenceSetup setup) {
      this.classifierConnection = conn;
      if (setup != null) this.classifierSetup = setup;
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

    public Builder withReviewThreshold(double t) {
      this.reviewThreshold = t;
      return this;
    }

    public Builder withBlockThreshold(double t) {
      this.blockThreshold = t;
      return this;
    }

    public Builder withHistory(int maxKeys, int perKeyDepth) {
      this.maxKeys = maxKeys;
      this.perKeyDepth = perKeyDepth;
      return this;
    }

    public ScreeningPipeline build() {
      return new ScreeningPipeline(this);
    }
  }

  /** Bounded per-key recent-item history. Access-ordered map caps distinct keys; deques cap depth. */
  private static final class History {
    private final int perKeyDepth;
    private final LinkedHashMap<String, Deque<ScreenItem>> byKey;

    History(int maxKeys, int perKeyDepth) {
      this.perKeyDepth = perKeyDepth;
      this.byKey =
          new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Deque<ScreenItem>> e) {
              return size() > maxKeys;
            }
          };
    }

    void record(ScreenItem item) {
      Deque<ScreenItem> dq = byKey.computeIfAbsent(item.key(), k -> new ArrayDeque<>());
      dq.addLast(item);
      while (dq.size() > perKeyDepth) dq.pollFirst();
    }

    ScreenContext contextAt(long now) {
      return new ScreenContext() {
        @Override
        public List<ScreenItem> recent(String key) {
          Deque<ScreenItem> dq = byKey.get(key);
          return dq == null ? Collections.emptyList() : new ArrayList<>(dq);
        }

        @Override
        public long now() {
          return now;
        }
      };
    }
  }
}
