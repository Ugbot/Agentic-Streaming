package org.agentic.flink.example.markets.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.channel.source.PollingSource;
import org.agentic.flink.example.markets.model.MarketRecords.Inventory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-JVM Coinbase Exchange WebSocket source — replaces the Python {@code coinbase_producer.py} +
 * Kafka path for the session-cluster flavour. Subscribes to the public {@code ticker} channel
 * (no API key required) and translates each ticker event into <b>two</b> {@link Inventory} rows
 * (one BID, one OFFER) so the rows slot straight into the existing market pipeline.
 *
 * <p>Uses the JDK's built-in {@link java.net.http.WebSocket} — no extra dependency. The async
 * listener pushes events onto a bounded queue; the {@link SourceFunction#run} loop drains the
 * queue with the Flink {@code SourceContext}'s checkpoint lock held.
 *
 * <p>Single parallelism by design — Coinbase rate-limits per connection and we want a stable
 * monotonic per-product update stream.
 */
public final class CoinbaseTickerSource implements PollingSource.PollFn<Inventory> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CoinbaseTickerSource.class);

  private static final URI WS_URI = URI.create("wss://ws-feed.exchange.coinbase.com");
  private static final int QUEUE_CAPACITY = 4096;
  private static final long SIZE_SCALE = 10_000L; // mirrors the Python producer

  private final List<String> products;

  private transient BlockingQueue<Inventory> queue;
  private transient WebSocket ws;

  public CoinbaseTickerSource(List<String> products) {
    if (products == null || products.isEmpty()) {
      throw new IllegalArgumentException("at least one product id required");
    }
    this.products = List.copyOf(products);
  }

  /** Convenience: ("BTC-USD", "ETH-USD", "SOL-USD"). */
  public static CoinbaseTickerSource defaults() {
    return new CoinbaseTickerSource(List.of("BTC-USD", "ETH-USD", "SOL-USD"));
  }

  /** Wrap as a native FLIP-27 source for {@code env.fromSource(...)}. */
  public static PollingSource<Inventory> source(List<String> products) {
    return new PollingSource<>(new CoinbaseTickerSource(products), QUEUE_CAPACITY);
  }

  @Override
  public void open(int subtaskIndex) throws Exception {
    queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    ObjectMapper mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Listener listener = new Listener(queue, mapper, products);
    ws =
        client
            .newWebSocketBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .buildAsync(WS_URI, listener)
            .get(20, TimeUnit.SECONDS);
    LOG.info("coinbase ws open products={}", products);
  }

  @Override
  public Inventory poll(long timeoutMs) throws InterruptedException {
    return queue.poll(Math.max(1, timeoutMs), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    if (ws != null) {
      try {
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye")
            .orTimeout(2, TimeUnit.SECONDS)
            .toCompletableFuture()
            .get();
      } catch (Exception ignored) {
        // best-effort
      }
    }
  }

  /**
   * Asynchronous WebSocket listener. Reassembles fragmented frames, parses each ticker into the
   * two Inventory rows (BID + OFFER), and pushes them onto the source's queue.
   */
  static final class Listener implements WebSocket.Listener {
    private final BlockingQueue<Inventory> queue;
    private final ObjectMapper mapper;
    private final List<String> products;
    private final StringBuilder buffer = new StringBuilder();

    Listener(BlockingQueue<Inventory> queue, ObjectMapper mapper, List<String> products) {
      this.queue = queue;
      this.mapper = mapper;
      this.products = products;
    }

    @Override
    public void onOpen(WebSocket ws) {
      try {
        String sub =
            mapper.writeValueAsString(
                java.util.Map.of(
                    "type", "subscribe",
                    "product_ids", products,
                    "channels", List.of("ticker", "heartbeat")));
        ws.sendText(sub, true);
      } catch (Exception e) {
        LOG.warn("coinbase ws subscribe failed: {}", e.getMessage());
      }
      ws.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
      buffer.append(data);
      if (last) {
        String msg = buffer.toString();
        buffer.setLength(0);
        try {
          handle(msg);
        } catch (Exception e) {
          LOG.warn("coinbase ws handle failed: {}", e.getMessage());
        }
      }
      ws.request(1);
      return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
      LOG.info("coinbase ws closed status={} reason={}", statusCode, reason);
      return null;
    }

    @Override
    public void onError(WebSocket ws, Throwable error) {
      LOG.warn("coinbase ws error: {}", error.getMessage());
    }

    private void handle(String json) throws Exception {
      JsonNode root = mapper.readTree(json);
      String type = root.path("type").asText("");
      if (!"ticker".equals(type)) {
        return;
      }
      String product = root.path("product_id").asText("");
      double bestBid = root.path("best_bid").asDouble(0.0);
      double bestAsk = root.path("best_ask").asDouble(0.0);
      double bidSize = root.path("best_bid_size").asDouble(0.0);
      double askSize = root.path("best_ask_size").asDouble(0.0);
      if (bestBid <= 0 || bestAsk <= 0) {
        return;
      }
      long ts;
      try {
        ts = Instant.parse(root.path("time").asText()).toEpochMilli();
      } catch (Exception e) {
        ts = System.currentTimeMillis();
      }
      long instrumentId = (long) (Math.abs(product.hashCode()) % 1_000_000_000L);

      Inventory bid =
          new Inventory(
              "COINBASE",
              instrumentId,
              "BID",
              bestBid,
              Math.max((long) (bidSize * SIZE_SCALE), 0L),
              0.0,
              1,
              1,
              "CRYPTO",
              product,
              "F",
              "UPDATE",
              ts);
      Inventory offer =
          new Inventory(
              "COINBASE",
              instrumentId,
              "OFFER",
              bestAsk,
              Math.max((long) (askSize * SIZE_SCALE), 0L),
              0.0,
              1,
              1,
              "CRYPTO",
              product,
              "F",
              "UPDATE",
              ts);
      // best-effort: drop on overflow to keep the WS callback non-blocking.
      if (!queue.offer(bid)) {
        LOG.warn("coinbase queue full, dropping bid");
      }
      if (!queue.offer(offer)) {
        LOG.warn("coinbase queue full, dropping offer");
      }
    }
  }

  // Avoid a hard import on java.util.ArrayList in the listener — silence the unused warning.
  @SuppressWarnings("unused")
  private static List<String> never() {
    return new ArrayList<>();
  }
}
