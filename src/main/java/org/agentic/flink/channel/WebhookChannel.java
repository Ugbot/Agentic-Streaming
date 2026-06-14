package org.agentic.flink.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.channel.source.PollingSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight HTTP channel: spins up a JDK {@link HttpServer} bound to a host/port, accepts
 * {@code POST} requests of JSON bodies, deserializes each into a {@code T}, and emits.
 *
 * <p>Single-parallelism by design — multiple subtasks can't share a port. Good for development,
 * webhooks from upstream systems (GitHub events, Slack, …), and any case where you want
 * external producers to push without a message broker in the loop.
 */
public final class WebhookChannel<T> implements Channel<T> {
  private static final long serialVersionUID = 1L;

  private final String host;
  private final int port;
  private final String path;
  private final Class<T> type;
  private final TypeInformation<T> typeInfo;

  public WebhookChannel(String host, int port, String path, Class<T> type) {
    this(host, port, path, type, TypeInformation.of(type));
  }

  public WebhookChannel(
      String host, int port, String path, Class<T> type, TypeInformation<T> typeInfo) {
    this.host = Objects.requireNonNull(host, "host");
    this.port = port;
    this.path = path == null || path.isEmpty() ? "/" : path;
    this.type = Objects.requireNonNull(type, "type");
    this.typeInfo = Objects.requireNonNull(typeInfo, "typeInfo");
  }

  @Override
  public DataStream<T> open(StreamExecutionEnvironment env) {
    return env.fromSource(
            new PollingSource<>(new WebhookPollFn<>(host, port, path, type)),
            WatermarkStrategy.noWatermarks(),
            "webhook[" + host + ":" + port + path + "]",
            typeInfo)
        .setParallelism(1);
  }

  @Override
  public TypeInformation<T> elementType() {
    return typeInfo;
  }

  @Override
  public String providerName() {
    return "webhook";
  }

  /**
   * Native FLIP-27 {@link PollingSource.PollFn}: the HTTP handler pushes decoded payloads into a
   * queue (open starts the server); {@link #poll} drains the queue; close stops the server.
   */
  static final class WebhookPollFn<T> implements PollingSource.PollFn<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WebhookPollFn.class);

    private final String host;
    private final int port;
    private final String path;
    private final Class<T> type;

    private transient HttpServer server;
    private transient LinkedBlockingQueue<T> queue;

    WebhookPollFn(String host, int port, String path, Class<T> type) {
      this.host = host;
      this.port = port;
      this.path = path;
      this.type = type;
    }

    @Override
    public void open(int subtaskIndex) throws Exception {
      queue = new LinkedBlockingQueue<>();
      final ObjectMapper mapper = new ObjectMapper();
      server = HttpServer.create(new InetSocketAddress(host, port), 0);
      server.createContext(
          path,
          exchange -> {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
              exchange.sendResponseHeaders(405, -1);
              exchange.close();
              return;
            }
            try (InputStream is = exchange.getRequestBody()) {
              byte[] body = is.readAllBytes();
              queue.add(mapper.readValue(body, type));
              byte[] ok = "ok".getBytes(StandardCharsets.UTF_8);
              exchange.sendResponseHeaders(200, ok.length);
              exchange.getResponseBody().write(ok);
              exchange.close();
            } catch (Exception e) {
              LOG.warn("Webhook deserialize failed: {}", e.getMessage());
              exchange.sendResponseHeaders(400, -1);
              exchange.close();
            }
          });
      server.start();
      LOG.info("Webhook channel listening on http://{}:{}{}", host, port, path);
    }

    @Override
    public T poll(long timeoutMs) throws InterruptedException {
      return queue.poll(Math.max(1, timeoutMs), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
      if (server != null) {
        try {
          server.stop(0);
        } catch (Exception ignored) {
          // best-effort
        }
      }
    }
  }

  /** Convenience for handling IO-checked builders in a single line. */
  public static <T> WebhookChannel<T> on(int port, Class<T> type) {
    return new WebhookChannel<>("0.0.0.0", port, "/", type);
  }

  static IOException wrap(Exception e) {
    return e instanceof IOException ? (IOException) e : new IOException(e);
  }
}
