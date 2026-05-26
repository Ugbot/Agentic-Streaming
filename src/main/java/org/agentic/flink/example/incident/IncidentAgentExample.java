package org.agentic.flink.example.incident;

import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.embedding.EmbeddingClient;
import org.agentic.flink.inference.GenericInferenceModel;
import org.agentic.flink.inference.InferenceClient;
import org.agentic.flink.inference.InferenceConnection;
import org.agentic.flink.inference.InferenceSetup;
import org.agentic.flink.inference.Classifier;
import org.agentic.flink.inference.Scorer;
import org.agentic.flink.llm.ChatClient;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatResponse;
import org.agentic.flink.llm.ChatSetup;
import org.agentic.flink.llm.langchain4j.LangChain4jChatConnection;
import org.agentic.flink.tools.ToolExecutor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Anomaly + CEP incident agent.
 *
 * <p>Streaming metric samples flow through a per-host anomaly detector implemented as a
 * {@link GenericInferenceModel}. Outliers become {@code AnomalyEvent}s. Flink CEP watches for
 * three anomalies within a sliding window on the same host and emits an
 * {@code IncidentEvent}. The agent then runs only on confirmed incidents — runbook lookup +
 * ticket creation — so the expensive LLM call fires once per real incident, not per metric
 * sample.
 *
 * <p>The anomaly detector is deliberately written as a {@link GenericInferenceModel} rather
 * than a {@link Classifier}: this is the framework's "raw" inference escape hatch for models
 * whose input/output shape doesn't fit the typed surfaces. Here it's a sliding-window
 * z-score; in production it could be an autoencoder loaded through ONNX or DJL.
 *
 * <p><b>Prerequisites:</b>
 * <pre>
 *   docker compose up -d ollama
 *   docker compose exec ollama ollama pull qwen2.5:3b
 * </pre>
 *
 * <p><b>To run:</b>
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass="org.agentic.flink.example.incident.IncidentAgentExample"
 * </pre>
 */
public class IncidentAgentExample {

  public record MetricSample(String host, String metric, double value, long ts) {}

  public record AnomalyEvent(String host, String metric, double value, double zScore, long ts) {}

  public record IncidentEvent(String host, String metric, long firstTs, long lastTs, int hits) {}

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<MetricSample> metrics =
        env.fromElements(synthMetrics().toArray(new MetricSample[0]));

    // Per-host sliding-window z-score: GenericInferenceModel because the I/O shape (rolling
    // stats + decision) doesn't fit the typed surfaces.
    InferenceConnection detector = new ZScoreAnomalyConnection(20, 2.5);

    DataStream<AnomalyEvent> anomalies =
        metrics
            .keyBy(MetricSample::host)
            .process(new AnomalyDetectFn(detector))
            .name("anomaly-detect");

    // CEP: three anomalies on the same host within the window.
    Pattern<AnomalyEvent, ?> sequence =
        Pattern.<AnomalyEvent>begin("first")
            .where(SimpleCondition.of(a -> true))
            .next("second")
            .where(SimpleCondition.of(a -> true))
            .next("third")
            .where(SimpleCondition.of(a -> true))
            .within(java.time.Duration.ofMinutes(5));

    PatternStream<AnomalyEvent> patterned = CEP.pattern(anomalies.keyBy(AnomalyEvent::host), sequence);

    DataStream<IncidentEvent> incidents =
        patterned.select(
            match -> {
              AnomalyEvent first = match.get("first").get(0);
              AnomalyEvent third = match.get("third").get(0);
              return new IncidentEvent(first.host(), first.metric(), first.ts(), third.ts(), 3);
            }).name("incident-detect");

    // Agent invocation — once per confirmed incident.
    LangChain4jChatConnection chat =
        LangChain4jChatConnection.ollama(ConfigKeys.DEFAULT_OLLAMA_BASE_URL);
    ChatSetup chatSetup =
        ChatSetup.builder().withModel("qwen2.5:3b").withTemperature(0.2).build();

    incidents
        .keyBy(IncidentEvent::host)
        .process(new IncidentAgentFn(chat, chatSetup))
        .print()
        .name("incidents");
    env.execute("incident-agent");
  }

  /** Synthetic stream: 18 normal samples then 3 spikes on the same host. */
  private static java.util.List<MetricSample> synthMetrics() {
    java.util.List<MetricSample> list = new java.util.ArrayList<>();
    long t = System.currentTimeMillis();
    for (int i = 0; i < 18; i++) {
      list.add(new MetricSample("host-a", "latency_ms", 100 + (i % 5), t + i * 30_000L));
    }
    // Three large spikes — should each trigger an anomaly.
    list.add(new MetricSample("host-a", "latency_ms", 920, t + 19 * 30_000L));
    list.add(new MetricSample("host-a", "latency_ms", 950, t + 20 * 30_000L));
    list.add(new MetricSample("host-a", "latency_ms", 970, t + 21 * 30_000L));
    return list;
  }

  /** Generic inference model: maintains a rolling window and emits a z-score per call. */
  static final class ZScoreAnomalyConnection implements InferenceConnection {
    private static final long serialVersionUID = 1L;
    private final int windowSize;
    private final double threshold;

    ZScoreAnomalyConnection(int windowSize, double threshold) {
      this.windowSize = windowSize;
      this.threshold = threshold;
    }

    @Override
    public InferenceClient bind(org.apache.flink.api.common.functions.RuntimeContext runtimeContext) {
      return new Client();
    }

    @Override
    public String providerName() {
      return "anomaly:z-score";
    }

    private final class Client implements InferenceClient {
      private final java.util.ArrayDeque<Double> window = new java.util.ArrayDeque<>();

      @Override
      public boolean supports(TaskKind kind) {
        return kind == TaskKind.GENERIC;
      }

      @Override
      public Classifier asClassifier() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Scorer asScorer() {
        throw new UnsupportedOperationException();
      }

      @Override
      public EmbeddingClient asEmbedder() {
        throw new UnsupportedOperationException();
      }

      @Override
      public GenericInferenceModel asGeneric() {
        return (input, setup) -> {
          double value = ((Number) input.get("value")).doubleValue();
          window.addLast(value);
          while (window.size() > windowSize) {
            window.removeFirst();
          }
          double mean = window.stream().mapToDouble(Double::doubleValue).average().orElse(0);
          double variance =
              window.stream().mapToDouble(d -> (d - mean) * (d - mean)).average().orElse(0);
          double std = Math.sqrt(variance);
          double z = std == 0 ? 0 : (value - mean) / std;
          return Map.of("zScore", z, "anomaly", Math.abs(z) >= threshold && window.size() >= 5);
        };
      }

      @Override
      public String providerName() {
        return "anomaly:z-score";
      }
    }
  }

  /** Per-host detector — feeds each sample to the connection and emits anomalies. */
  static final class AnomalyDetectFn extends KeyedProcessFunction<String, MetricSample, AnomalyEvent> {
    private static final long serialVersionUID = 1L;
    private final InferenceConnection conn;
    private transient GenericInferenceModel model;
    private transient InferenceSetup setup;

    AnomalyDetectFn(InferenceConnection conn) {
      this.conn = conn;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      model = conn.bind(getRuntimeContext()).asGeneric();
      setup = InferenceSetup.builder().withModelName("z-score").withModelUri("inproc").build();
    }

    @Override
    public void processElement(MetricSample s, Context ctx, Collector<AnomalyEvent> out) {
      Map<String, Object> res = model.infer(Map.of("value", s.value()), setup);
      Boolean anomaly = (Boolean) res.get("anomaly");
      if (Boolean.TRUE.equals(anomaly)) {
        out.collect(
            new AnomalyEvent(s.host(), s.metric(), s.value(), (double) res.get("zScore"), s.ts()));
      }
    }
  }

  /** On each confirmed incident: pull a runbook snippet (tool) and ask the LLM what to do. */
  static final class IncidentAgentFn extends KeyedProcessFunction<String, IncidentEvent, String> {
    private static final long serialVersionUID = 1L;
    private final LangChain4jChatConnection chatConn;
    private final ChatSetup chatSetup;

    private transient ChatClient chat;
    private transient ToolExecutor runbookTool;
    private transient ToolExecutor ticketTool;
    private transient ValueState<Integer> handledCount;

    IncidentAgentFn(LangChain4jChatConnection chatConn, ChatSetup chatSetup) {
      this.chatConn = chatConn;
      this.chatSetup = chatSetup;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      chat = chatConn.bind(getRuntimeContext());
      runbookTool = new RunbookLookupTool();
      ticketTool = new TicketCreationTool();
      handledCount =
          getRuntimeContext().getState(new ValueStateDescriptor<>("incidents", Integer.class));
    }

    @Override
    public void processElement(
        IncidentEvent inc, KeyedProcessFunction<String, IncidentEvent, String>.Context ctx,
        Collector<String> out) throws Exception {
      Integer prior = handledCount.value();
      int n = prior == null ? 1 : prior + 1;
      handledCount.update(n);

      Object runbook = runbookTool.execute(Map.of("metric", inc.metric())).get();
      ChatResponse plan =
          chat.chat(
              java.util.List.of(
                  ChatMessage.system(
                      "You are an on-call SRE. Propose a concise remediation plan (<=4 steps)"
                          + " given the runbook and incident details."),
                  ChatMessage.user(
                      "Incident: " + inc + "\nRunbook excerpt:\n" + runbook)),
              chatSetup);
      Object ticket =
          ticketTool
              .execute(
                  Map.of(
                      "host", inc.host(),
                      "metric", inc.metric(),
                      "plan", plan.getText()))
              .get();

      out.collect("incident#" + n + " ticket=" + ticket + " plan=" + plan.getText().trim());
    }
  }

  /** Tiny stub runbook lookup. In production this would hit a wiki / S3 / git repo. */
  static final class RunbookLookupTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      String metric = String.valueOf(parameters.getOrDefault("metric", "unknown"));
      return CompletableFuture.completedFuture(
          switch (metric) {
            case "latency_ms" ->
                "Runbook: check load balancer health, then JVM GC pauses, then DB pool sizes.";
            case "error_rate" ->
                "Runbook: inspect recent deploys, then dependency 5xx rates, then circuit breakers.";
            default -> "Runbook: open a ticket and tag #platform.";
          });
    }

    @Override
    public String getToolId() {
      return "runbook-lookup";
    }

    @Override
    public String getDescription() {
      return "Look up a runbook snippet for the given metric.";
    }
  }

  /** Stub ticket creator. Prints to stdout and returns a synthetic ticket id. */
  static final class TicketCreationTool implements ToolExecutor {
    private static final long serialVersionUID = 1L;
    private static final java.util.concurrent.atomic.AtomicInteger SEQ =
        new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public CompletableFuture<Object> execute(Map<String, Object> parameters) {
      String id = "INC-" + SEQ.incrementAndGet();
      System.out.println(
          "📨 created " + id + " host=" + parameters.get("host") + " metric=" + parameters.get("metric"));
      return CompletableFuture.completedFuture(id);
    }

    @Override
    public String getToolId() {
      return "ticket-create";
    }

    @Override
    public String getDescription() {
      return "Create an incident ticket and return its id.";
    }
  }
}
