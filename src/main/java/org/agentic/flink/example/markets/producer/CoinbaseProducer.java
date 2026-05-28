package org.agentic.flink.example.markets.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.agentic.flink.example.markets.model.MarketRecords.Inventory;
import org.agentic.flink.example.markets.model.MarketRecords.Security;
import org.agentic.flink.example.markets.model.MarketRecords.Trade;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java twin of {@code examples-bin/markets/coinbase_producer.py}. Subscribes to the public
 * Coinbase Exchange WebSocket and translates each event into the same Inventory/Trade JSON shape
 * the Flink job already consumes, so the operator graph is identical for crypto and bonds.
 *
 * <p>Uses the built-in JDK 17 {@link WebSocket} client — no extra dependency. Channels:
 * {@code level2_batch} (mapped to inventory rows) and {@code matches} (mapped to trades). A small
 * static seed of {@link Security} rows is also published so the broadcast enrichment has data.
 *
 * <pre>
 *   java -cp target/agentic-flink-1.0.0-SNAPSHOT.jar \
 *     org.agentic.flink.example.markets.producer.CoinbaseProducer \
 *     --products BTC-USD,ETH-USD,SOL-USD
 * </pre>
 */
public final class CoinbaseProducer {

  private static final Logger LOG = LoggerFactory.getLogger(CoinbaseProducer.class);
  private static final URI WS_URI = URI.create("wss://ws-feed.exchange.coinbase.com");

  public static void main(String[] args) throws Exception {
    List<String> products =
        Arrays.stream(parseString(args, "--products", "BTC-USD,ETH-USD,SOL-USD").split(","))
            .map(String::trim).filter(s -> !s.isEmpty()).toList();

    try (MarketProducerSupport mp =
        new MarketProducerSupport(MarketProducerSupport.defaultBootstrap(), "coinbase-producer")) {
      // Seed the crypto "security master" so the Flink enrichment broadcast has rows.
      for (String p : products) {
        mp.send("coinbase-securities", securityFor(p));
      }

      CountDownLatch done = new CountDownLatch(1);
      HttpClient http = HttpClient.newHttpClient();
      Listener listener = new Listener(mp, done);
      WebSocket ws =
          http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(10))
              .buildAsync(WS_URI, listener).join();

      String sub = subscribeMessage(products);
      LOG.info("subscribing: {}", sub);
      ws.sendText(sub, true).join();
      System.out.printf(Locale.ROOT, "Coinbase bridge: streaming %s to coinbase-* topics%n", products);
      done.await();
    }
  }

  static Security securityFor(String product) {
    return new Security(
        Math.abs(product.hashCode()) % 1_000_000_000,
        product, product.replace("-", ""), product, "Coinbase",
        "Crypto", product.split("-")[0], 0.0, "2099-12-31",
        "NR", "NR", "NR", "N");
  }

  private static String subscribeMessage(List<String> products) {
    StringBuilder b = new StringBuilder();
    b.append("{\"type\":\"subscribe\",\"product_ids\":[");
    for (int i = 0; i < products.size(); i++) {
      if (i > 0) b.append(',');
      b.append('"').append(products.get(i)).append('"');
    }
    b.append("],\"channels\":[\"level2_batch\",\"matches\",\"heartbeat\"]}");
    return b.toString();
  }

  static long parseCoinbaseTs(String iso) {
    if (iso == null || iso.isBlank()) return System.currentTimeMillis();
    try {
      return OffsetDateTime.parse(iso).toInstant().toEpochMilli();
    } catch (Exception e) {
      return System.currentTimeMillis();
    }
  }

  static long productInstrumentId(String product) {
    return Math.abs(product.hashCode()) % 1_000_000_000L;
  }

  /** Translate one {@code level2} change ({@code ["buy"|"sell", price, size]}) to an Inventory row. */
  static Inventory l2ChangeToInventory(String product, String sideRaw, double price, double size, long ts) {
    return new Inventory(
        "COINBASE", productInstrumentId(product),
        "buy".equals(sideRaw) ? "BID" : "OFFER",
        price, (long) Math.max(0, size * 10_000),  // scale fractional crypto sizes to ints
        0.0, 1, 1, "CRYPTO", product, "F",
        size > 0 ? "UPDATE" : "DELETE", ts);
  }

  static Trade matchToTrade(JsonNode m) {
    double price = m.path("price").asDouble(0.0);
    double size = m.path("size").asDouble(0.0);
    String product = m.path("product_id").asText("");
    return new Trade(
        m.path("trade_id").asLong(0),
        product, parseCoinbaseTs(m.path("time").asText("")),
        "buy".equals(m.path("side").asText("")) ? "BUY" : "SELL",
        price, size, 0.0, 0.0, 0.0, 0.0,
        "CRYPTO", "CBMAKER", "CBTAKER", "COINBASE");
  }

  private static String parseString(String[] args, String flag, String def) {
    for (int i = 0; i + 1 < args.length; i++) {
      if (flag.equals(args[i])) return args[i + 1];
    }
    return def;
  }

  /** Accumulates WebSocket text fragments and parses each completed message as JSON. */
  static final class Listener implements WebSocket.Listener {
    private final MarketProducerSupport mp;
    private final CountDownLatch done;
    private final ObjectMapper mapper =
        new ObjectMapper().registerModule(new ParameterNamesModule());
    private final StringBuilder buffer = new StringBuilder();
    private long seen;

    Listener(MarketProducerSupport mp, CountDownLatch done) {
      this.mp = mp;
      this.done = done;
    }

    @Override
    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
      buffer.append(data);
      if (last) {
        String json = buffer.toString();
        buffer.setLength(0);
        try {
          handle(mapper.readTree(json));
        } catch (Exception e) {
          LOG.warn("malformed message ({}): {}", e.getMessage(), json);
        }
      }
      ws.request(1);
      return null;
    }

    @Override
    public void onError(WebSocket ws, Throwable err) {
      LOG.error("websocket error", err);
      done.countDown();
    }

    @Override
    public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
      LOG.warn("websocket closed: {} {}", statusCode, reason);
      done.countDown();
      return CompletableFuture.completedFuture(null);
    }

    private void handle(JsonNode msg) {
      String mtype = msg.path("type").asText("");
      switch (mtype) {
        case "snapshot" -> {
          String product = msg.path("product_id").asText("");
          long ts = parseCoinbaseTs(msg.path("time").asText(""));
          emitSnapshotSide(product, msg.path("bids"), "buy", ts);
          emitSnapshotSide(product, msg.path("asks"), "sell", ts);
        }
        case "l2update" -> {
          String product = msg.path("product_id").asText("");
          long ts = parseCoinbaseTs(msg.path("time").asText(""));
          for (JsonNode change : msg.path("changes")) {
            if (change.size() < 3) continue;
            mp.send("coinbase-inventory",
                l2ChangeToInventory(
                    product, change.get(0).asText(),
                    change.get(1).asDouble(), change.get(2).asDouble(), ts));
          }
        }
        case "match", "last_match" -> mp.send("coinbase-trades", matchToTrade(msg));
        default -> {
          // ignore heartbeats, subscriptions, etc.
        }
      }
      if (++seen % 500 == 0) LOG.info("handled {} coinbase messages", seen);
    }

    private void emitSnapshotSide(String product, JsonNode levels, String side, long ts) {
      int capped = Math.min(levels.size(), 25);
      List<Inventory> rows = new ArrayList<>(capped);
      for (int i = 0; i < capped; i++) {
        JsonNode lvl = levels.get(i);
        if (lvl.size() >= 2) {
          rows.add(
              l2ChangeToInventory(
                  product, side, lvl.get(0).asDouble(), lvl.get(1).asDouble(), ts));
        }
      }
      for (Inventory r : rows) mp.send("coinbase-inventory", r);
    }
  }
}
