package org.agentic.flink.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
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
    return env.addSource(new Source<>(host, port, path, type), typeInfo)
        .name("webhook[" + host + ":" + port + path + "]")
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

  static final class Source<T> implements SourceFunction<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    private final String host;
    private final int port;
    private final String path;
    private final Class<T> type;

    private volatile boolean running = true;
    private transient HttpServer server;

    Source(String host, int port, String path, Class<T> type) {
      this.host = host;
      this.port = port;
      this.path = path;
      this.type = type;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
      ObjectMapper mapper = new ObjectMapper();
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
              T value = mapper.readValue(body, type);
              synchronized (ctx.getCheckpointLock()) {
                ctx.collect(value);
              }
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

      // Idle until cancel().
      while (running) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
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
