package org.agentic.flink.session;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agentic.flink.channel.Channel;
import org.agentic.flink.channel.FlussChannel;
import org.agentic.flink.channel.FlussSink;
import org.agentic.flink.channel.ZeroMqChannel;
import org.agentic.flink.channel.ZeroMqSink;
import org.agentic.flink.control.ControlMessage;
import org.agentic.flink.control.ControlState;
import org.agentic.flink.example.markets.model.MarketRecords.EnrichedInventory;
import org.agentic.flink.example.markets.model.MarketRecords.Inventory;
import org.agentic.flink.example.markets.model.MarketRecords.MarketFeatures;
import org.agentic.flink.example.markets.model.MarketRecords.RankedQuote;
import org.agentic.flink.example.markets.model.MarketRecords.Security;
import org.agentic.flink.example.markets.source.CoinbaseTickerSource;
import org.agentic.flink.example.markets.stage.AgenticMarketAgentFn;
import org.agentic.flink.example.markets.stage.EnrichmentFn;
import org.agentic.flink.example.markets.stage.FeatureAggregatorFn;
import org.agentic.flink.example.markets.stage.TopNRankerFn;
import org.agentic.flink.operator.wiring.AgenticPipeline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry class submitted to the Flink session cluster. Dispatches on {@code --level} to build one
 * of the agentic levels (producer, 1-5) using arg-driven source/sink endpoints. Designed to be
 * driven by the notebook's Python session client: one jar uploaded once, many jobs submitted
 * with different program args.
 *
 * <p>Endpoints encode source/sink type in their scheme:
 *
 * <ul>
 *   <li>{@code coinbase://} — Coinbase WebSocket source (producer only).
 *   <li>{@code tcp://host:port} — ZeroMQ. Source is {@code PULL} binding to the endpoint by
 *       default; sink is {@code PUSH} connecting. Pairs cleanly with the framework default.
 *   <li>{@code fluss://database.table} — Fluss source/sink; bootstrap defaults to
 *       {@code localhost:9123}, overridable via {@code --fluss-bootstrap}.
 * </ul>
 *
 * <p>Examples (run inside the session cluster via {@code POST /jars/{id}/run}):
 *
 * <pre>
 *   --level producer  --out tcp://0.0.0.0:5560  --products BTC-USD,ETH-USD,SOL-USD
 *   --level 1         --in  tcp://localhost:5560  --out tcp://0.0.0.0:5561
 *   --level 5         --in  tcp://localhost:5564  --out fluss://agentic.alerts
 *                     --control tcp://localhost:5559 --anthropic-key sk-ant-...
 * </pre>
 */
public final class SessionJobLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(SessionJobLauncher.class);

  private SessionJobLauncher() {}

  public static void main(String[] args) throws Exception {
    Args a = Args.parse(args);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    String level = a.require("level");
    String jobName = a.opt("name").orElse("agentic-" + level);
    LOG.info("SessionJobLauncher level={} args={}", level, a);

    switch (level) {
      case "producer" -> buildProducer(env, a);
      case "1" -> buildLevel1(env, a);
      case "2" -> buildLevel2(env, a);
      case "3" -> buildLevel3(env, a);
      case "4" -> buildLevel4(env, a);
      case "5" -> buildLevel5(env, a);
      default -> throw new IllegalArgumentException("unknown level: " + level);
    }
    env.execute(jobName);
  }

  // ---- level builders ----

  /** Coinbase WS → JSON Inventory rows on the configured sink. */
  static void buildProducer(StreamExecutionEnvironment env, Args a) {
    List<String> products =
        List.of(a.opt("products").orElse("BTC-USD,ETH-USD,SOL-USD").split(","));
    DataStream<Inventory> src =
        env.addSource(new CoinbaseTickerSource(products), TypeInformation.of(Inventory.class))
            .name("coinbase-ws[" + String.join(",", products) + "]")
            .setParallelism(1);
    src.addSink(sinkFor(a.require("out"), a, Inventory.class))
        .name("sink[" + a.require("out") + "]")
        .setParallelism(1);
  }

  /** Identity passthrough — proves the chain is alive. */
  static void buildLevel1(StreamExecutionEnvironment env, Args a) throws Exception {
    DataStream<Inventory> in =
        sourceFor(a.require("in"), a, Inventory.class).open(env);
    in.addSink(sinkFor(a.require("out"), a, Inventory.class))
        .name("L1.passthrough.sink")
        .setParallelism(1);
  }

  /** Enrichment via a hardcoded synthetic Security broadcast. */
  static void buildLevel2(StreamExecutionEnvironment env, Args a) throws Exception {
    DataStream<Inventory> in = sourceFor(a.require("in"), a, Inventory.class).open(env);
    BroadcastStream<Security> securities =
        env.fromCollection(syntheticSecurities(), TypeInformation.of(Security.class))
            .broadcast(EnrichmentFn.SECURITIES);
    DataStream<EnrichedInventory> enriched =
        in.connect(securities).process(new EnrichmentFn()).name("L2.enrich");
    enriched
        .addSink(sinkFor(a.require("out"), a, EnrichedInventory.class))
        .name("L2.sink")
        .setParallelism(1);
  }

  /** Top-N ranking per (instrument, side). */
  static void buildLevel3(StreamExecutionEnvironment env, Args a) throws Exception {
    int topN = Integer.parseInt(a.opt("top-n").orElse("5"));
    DataStream<EnrichedInventory> in =
        sourceFor(a.require("in"), a, EnrichedInventory.class).open(env);
    DataStream<RankedQuote> ranked =
        in.keyBy(e -> e.inventory().instrumentId() + "|" + e.inventory().side())
            .process(new TopNRankerFn(topN))
            .name("L3.top-" + topN);
    ranked
        .addSink(sinkFor(a.require("out"), a, RankedQuote.class))
        .name("L3.sink")
        .setParallelism(1);
  }

  /** Windowed feature aggregation per instrumentId. */
  static void buildLevel4(StreamExecutionEnvironment env, Args a) throws Exception {
    long windowMs = Long.parseLong(a.opt("window-ms").orElse("5000"));
    int topN = Integer.parseInt(a.opt("top-n").orElse("5"));
    DataStream<RankedQuote> in = sourceFor(a.require("in"), a, RankedQuote.class).open(env);
    DataStream<MarketFeatures> features =
        in.keyBy(rq -> Long.toString(rq.quote().inventory().instrumentId()))
            .process(new FeatureAggregatorFn(windowMs, topN))
            .name("L4.feature-aggregator");
    features
        .addSink(sinkFor(a.require("out"), a, MarketFeatures.class))
        .name("L4.sink")
        .setParallelism(1);
  }

  /** Agentic screening: band-pass + z-score + Claude. Wires the control-plane broadcast. */
  static void buildLevel5(StreamExecutionEnvironment env, Args a) throws Exception {
    DataStream<MarketFeatures> in =
        sourceFor(a.require("in"), a, MarketFeatures.class).open(env);

    BroadcastStream<ControlMessage> control;
    if (a.opt("control").isPresent()) {
      control =
          AgenticPipeline.controlInput(
              env, sourceFor(a.require("control"), a, ControlMessage.class));
    } else {
      // Empty control source — operator still works, just never sees a flip.
      control =
          env.fromCollection(
                  List.<ControlMessage>of(), TypeInformation.of(ControlMessage.class))
              .broadcast(ControlState.DIRECTIVES);
    }

    AgenticMarketAgentFn fn =
        new AgenticMarketAgentFn(
            "L5.market-agent",
            a.opt("anthropic-key").orElse(System.getenv("ANTHROPIC_API_KEY")),
            Double.parseDouble(a.opt("spread-band-low").orElse("0.0")),
            Double.parseDouble(a.opt("spread-band-high").orElse("25.0")),
            Double.parseDouble(a.opt("z-threshold").orElse("2.5")),
            Integer.parseInt(a.opt("warmup").orElse("5")));

    SingleOutputStreamOperator<String> alerts =
        AgenticPipeline.wire(
            in.keyBy(f -> Long.toString(f.instrumentId())), control, fn);

    alerts
        .addSink(sinkFor(a.require("out"), a, String.class))
        .name("L5.sink")
        .setParallelism(1);

    // Optional live-observation tee: PUB out the same alert JSON so a notebook (or any
    // ZeroMQ SUB) can tail adjudicated alerts independently of the durable Fluss sink. Off
    // by default — pass --alerts-pub tcp://... to enable. Uses pubRaw so the already-JSON
    // String isn't double-encoded by the default Jackson serializer.
    if (a.opt("alerts-pub").isPresent()) {
      alerts
          .addSink(ZeroMqSink.pubRaw(a.require("alerts-pub"), ""))
          .name("L5.alerts-pub")
          .setParallelism(1);
    }

    // Tap the debug side-output if a sink was configured. Always use PUB on the debug path —
    // broadcast-friendly so multiple notebooks / observer tools can subscribe without socket
    // conflicts. Notebook side connects with SUB.
    if (a.opt("debug-sink").isPresent()) {
      String dbgEndpoint = a.require("debug-sink");
      org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction<
              org.agentic.flink.control.DebugEvent>
          dbgSink;
      if (dbgEndpoint.startsWith("tcp://")) {
        dbgSink = ZeroMqSink.pub(dbgEndpoint, "");
      } else {
        dbgSink = sinkFor(dbgEndpoint, a, org.agentic.flink.control.DebugEvent.class);
      }
      AgenticPipeline.debugStream(alerts).addSink(dbgSink).name("L5.debug-sink").setParallelism(1);
    }
  }

  // ---- source/sink resolution ----

  @SuppressWarnings("unchecked")
  static <T> Channel<T> sourceFor(String endpoint, Args a, Class<T> type) {
    if (endpoint.startsWith("tcp://")) {
      return ZeroMqChannel.pull(endpoint, type);
    }
    if (endpoint.startsWith("fluss://")) {
      String body = endpoint.substring("fluss://".length());
      String[] parts = body.split("\\.", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("fluss endpoint must be fluss://database.table");
      }
      return FlussChannel.of(
          a.opt("fluss-bootstrap").orElse("localhost:9123"), parts[0], parts[1], type);
    }
    throw new IllegalArgumentException("unknown source endpoint scheme: " + endpoint);
  }

  @SuppressWarnings("unchecked")
  static <T> RichSinkFunction<T> sinkFor(String endpoint, Args a, Class<T> type) {
    if (endpoint.startsWith("tcp://")) {
      return (RichSinkFunction<T>) ZeroMqSink.push(endpoint);
    }
    if (endpoint.startsWith("fluss://")) {
      String body = endpoint.substring("fluss://".length());
      String[] parts = body.split("\\.", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("fluss endpoint must be fluss://database.table");
      }
      return FlussSink.randomKey(
          a.opt("fluss-bootstrap").orElse("localhost:9123"), parts[0], parts[1]);
    }
    throw new IllegalArgumentException("unknown sink endpoint scheme: " + endpoint);
  }

  // ---- synthetic security broadcast for L2 enrichment ----

  static List<Security> syntheticSecurities() {
    List<Security> out = new ArrayList<>();
    String[] products = {"BTC-USD", "ETH-USD", "SOL-USD"};
    for (String p : products) {
      long id = ((long) Math.abs(p.hashCode()) % 1_000_000_000L) % 50_000L;
      out.add(
          new Security(
              id,
              p,
              p.replace("-", ""),
              p,
              "Coinbase",
              "Crypto",
              p.split("-")[0],
              0.0,
              "2099-12-31",
              "NR",
              "NR",
              "NR",
              "N"));
    }
    return out;
  }

  // ---- arg parser ----

  /** Minimal {@code --key value} arg parser. */
  static final class Args {
    private final Map<String, String> map;

    private Args(Map<String, String> map) {
      this.map = map;
    }

    static Args parse(String[] argv) {
      Map<String, String> m = new HashMap<>();
      for (int i = 0; i < argv.length; i++) {
        String arg = argv[i];
        if (!arg.startsWith("--")) {
          throw new IllegalArgumentException("unrecognized argument: " + arg);
        }
        String key = arg.substring(2);
        if (i + 1 >= argv.length || argv[i + 1].startsWith("--")) {
          m.put(key, "true");
        } else {
          m.put(key, argv[++i]);
        }
      }
      return new Args(m);
    }

    String require(String key) {
      String v = map.get(key);
      if (v == null) {
        throw new IllegalArgumentException("missing required --" + key);
      }
      return v;
    }

    java.util.Optional<String> opt(String key) {
      return java.util.Optional.ofNullable(map.get(key));
    }

    @Override
    public String toString() {
      return map.toString();
    }
  }
}
