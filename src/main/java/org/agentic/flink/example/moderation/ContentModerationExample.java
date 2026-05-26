package org.agentic.flink.example.moderation;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.inference.ClassificationResult;
import org.agentic.flink.inference.Classifier;
import org.agentic.flink.inference.InferenceClient;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.djl.DjlInferenceConnection;
import org.agentic.flink.listener.AgentEventListener;
import org.agentic.flink.listener.MetricsAgentEventListener;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Real-time content-moderation pipeline on Flink.
 *
 * <p>Each incoming post is run through a toxicity classifier. Safe posts are summarized by the
 * LLM and emitted on the main output. Toxic posts are emitted on a side output tagged with the
 * predicted label and the score, ready to be written to an audit store (Postgres in production,
 * stdout here).
 *
 * <p>The {@link org.agentic.flink.inference.InferenceConnection} ships in the Flink job
 * graph; the heavy DJL model and the {@link ChatClient} are both bound lazily in
 * {@link ModerationProcessFunction#open}. The listener fires on every classification, every
 * chat response, and every block — wire it up to your metrics pipeline for SLO dashboards.
 *
 * <p><b>Prerequisites:</b>
 * <pre>
 *   docker compose up -d ollama postgres
 *   docker compose exec ollama ollama pull qwen2.5:3b
 * </pre>
 *
 * <p><b>To run:</b>
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.moderation.ContentModerationExample"
 * </pre>
 *
 * <p>For a real Kafka source, swap {@code env.fromElements(...)} for a {@code KafkaSource} —
 * see {@code docs/examples/moderation.md} for the snippet and the compose addition.
 */
public class ContentModerationExample {

  /** A single user-generated post entering the pipeline. */
  public record Post(String id, String user, String text) {}

  /** Audit record for posts that didn't make it through. */
  public record BlockedPost(String id, String user, String label, double score, String snippet) {}

  static final OutputTag<BlockedPost> BLOCKED =
      new OutputTag<>("blocked") {};

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // demo determinism

    // Toy stream — replace with KafkaSource in production. The compose snippet in
    // docs/examples/moderation.md shows the swap.
    DataStream<Post> posts =
        env.fromElements(
            new Post("p-001", "alice", "Loving the new release, great work!"),
            new Post("p-002", "bob", "you are all such useless idiots, fix the bug already"),
            new Post("p-003", "carol", "Question: how do I export to CSV?"),
            new Post("p-004", "dave", "i hope your servers burn down forever"));

    DjlInferenceConnection toxicity =
        DjlInferenceConnection.classification(
            "djl://ai.djl.huggingface.pytorch/unitary/toxic-bert");
    InferenceSetup toxicitySetup =
        InferenceSetup.builder()
            .withModelName("toxic-bert")
            .withModelUri(toxicity.getDefaultModelUri())
            .build();

    LangChain4jChatConnection chat =
        LangChain4jChatConnection.ollama(ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    ChatSetup chatSetup =
        ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.3).build();

    SingleOutputStreamOperator<String> safeSummaries =
        posts.process(
                new ModerationProcessFunction(
                    toxicity, toxicitySetup, chat, chatSetup, Set.of("toxic", "severe_toxic", "obscene", "threat")))
            .name("moderate");

    safeSummaries.print().name("safe-output");
    safeSummaries
        .getSideOutput(BLOCKED)
        .map(b -> "BLOCKED " + b.id() + " by " + b.user() + " label=" + b.label() + " score=" + b.score())
        .name("audit-sink")
        .print();

    env.execute("content-moderation");
  }

  /**
   * Per-post moderation logic. Built as a {@code ProcessFunction} so each subtask owns its own
   * model handle — the {@code DjlInferenceConnection} spec is the only thing shipping in the
   * job graph.
   */
  static final class ModerationProcessFunction extends ProcessFunction<Post, String> {
    private static final long serialVersionUID = 1L;

    private final DjlInferenceConnection toxicityConnection;
    private final InferenceSetup toxicitySetup;
    private final LangChain4jChatConnection chatConnection;
    private final ChatSetup chatSetup;
    private final Set<String> blockedLabels;

    private transient Classifier classifier;
    private transient ChatClient chatClient;
    private transient MetricsAgentEventListener metrics;
    private transient AuditingListener audit;

    ModerationProcessFunction(
        DjlInferenceConnection toxicityConnection,
        InferenceSetup toxicitySetup,
        LangChain4jChatConnection chatConnection,
        ChatSetup chatSetup,
        Set<String> blockedLabels) {
      this.toxicityConnection = toxicityConnection;
      this.toxicitySetup = toxicitySetup;
      this.chatConnection = chatConnection;
      this.chatSetup = chatSetup;
      this.blockedLabels = blockedLabels == null ? Collections.emptySet() : Set.copyOf(blockedLabels);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      InferenceClient inferenceClient = toxicityConnection.bind(getRuntimeContext());
      classifier = inferenceClient.asClassifier();
      chatClient = chatConnection.bind(getRuntimeContext());
      metrics = new MetricsAgentEventListener();
      audit = new AuditingListener();
    }

    @Override
    public void processElement(Post post, Context ctx, Collector<String> out) {
      long started = System.nanoTime();
      ClassificationResult cls = classifier.classify(post.text(), toxicitySetup);
      long durationMs = (System.nanoTime() - started) / 1_000_000;
      metrics.onInference("moderator", "toxic-bert", "classifier", durationMs);
      audit.onInference("moderator", "toxic-bert", "classifier", durationMs);

      if (blockedLabels.contains(cls.getLabel())) {
        metrics.onGuardrailBlock("moderator", "toxic-bert", cls.getLabel());
        audit.onGuardrailBlock("moderator", "toxic-bert", cls.getLabel());
        ctx.output(
            BLOCKED,
            new BlockedPost(post.id(), post.user(), cls.getLabel(), cls.getScore(), snippet(post.text())));
        return;
      }

      // Safe → summarize with the LLM. SPI-only: nothing LangChain4J-specific here.
      ChatResponse resp =
          chatClient.chat(
              List.of(
                  ChatMessage.system(
                      "Summarize the user post in <=20 words. Stay neutral and factual."),
                  ChatMessage.user(post.text())),
              chatSetup);
      metrics.onChatResponse("moderator", chatSetup.getModelName(), resp.getText().length(), resp.getTokensUsed());

      out.collect(post.id() + " | " + post.user() + " | " + resp.getText().trim());
    }

    private static String snippet(String text) {
      return text.length() <= 64 ? text : text.substring(0, 60) + "...";
    }
  }

  /**
   * Listener that POSTs blocked-post audit records to a JSON-over-HTTP endpoint.
   *
   * <p>In production this would write to Postgres directly (we have a
   * {@code LongTermMemoryStore} for it) or push to Kafka. The HTTP target here keeps the demo
   * dependency-free.
   */
  static final class AuditingListener implements AgentEventListener {
    private static final long serialVersionUID = 1L;
    private final transient HttpClient http =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String endpoint =
        System.getenv().getOrDefault("AUDIT_ENDPOINT", "http://localhost:8081/audit");

    @Override
    public void onGuardrailBlock(String agentId, String modelName, String label) {
      String payload =
          "{\"agentId\":\""
              + agentId
              + "\",\"model\":\""
              + modelName
              + "\",\"label\":\""
              + label
              + "\"}";
      try {
        HttpRequest req =
            HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .timeout(Duration.ofSeconds(1))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
                .build();
        http.send(req, HttpResponse.BodyHandlers.discarding());
      } catch (IOException | InterruptedException ignored) {
        // Audit best-effort: don't fail the pipeline if the audit endpoint is down.
        if (Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public void onInference(String agentId, String modelName, String task, long durationMs) {
      // Could push timing data to the audit log too; left as a no-op in the demo.
      Arrays.hashCode(new Object[] {agentId, modelName, task, durationMs}); // suppress lint
    }
  }
}
