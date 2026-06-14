package org.agentic.flink.a2a.bridge;
import org.apache.flink.api.common.functions.OpenContext;

import java.util.List;
import org.agentic.flink.a2a.A2AJson;
import org.agentic.flink.channel.Channel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis {@link A2ABridge} — distributed-light transport (requires the optional Jedis dep), backed by
 * Redis <b>lists</b> with blocking pops ({@code RPUSH} + {@code BLPOP}), not pub/sub.
 *
 * <p>Requests are {@code RPUSH}ed onto the {@code requestChannel} list and {@code BLPOP}ped by the
 * Flink source; responses are {@code RPUSH}ed onto the {@code responseChannel} list and {@code
 * BLPOP}ped by the gateway connector (correlated by {@code taskId} in {@link
 * AbstractA2AGatewayConnector}). JSON via {@link A2AJson}.
 *
 * <p>Lists (not pub/sub) make the bridge <b>non-lossy</b>: a request published before the Flink
 * source is ready simply waits in the list until it pops it — no startup race — and queued
 * messages survive a consumer restart. Assumes a single gateway connector per response list (the
 * one-agent-process deployment); BLPOP would otherwise load-balance responses across connectors.
 */
public final class RedisA2ABridge implements A2ABridge {
  private static final long serialVersionUID = 1L;

  /** BLPOP timeout (seconds) — bounded so cancel()/close() is observed promptly. */
  private static final int BLPOP_TIMEOUT_SECONDS = 2;

  private final String host;
  private final int port;
  private final String requestChannel;
  private final String responseChannel;

  public RedisA2ABridge(String host, int port, String requestChannel, String responseChannel) {
    this.host = host;
    this.port = port;
    this.requestChannel = requestChannel;
    this.responseChannel = responseChannel;
  }

  @Override
  public String transport() {
    return "redis";
  }

  @Override
  public Channel<A2ARequest> requestChannel() {
    return new RedisRequestChannel(host, port, requestChannel);
  }

  @Override
  public org.apache.flink.api.connector.sink2.Sink<A2AResponse> responseSink() {
    return new org.agentic.flink.channel.sink.ForEachSink<>(
        new RedisResponseWriteFn(host, port, responseChannel));
  }

  @Override
  public A2AGatewayConnector openGateway() {
    return new RedisConnector(host, port, requestChannel, responseChannel);
  }

  // ==================== Flink request source ====================

  static final class RedisRequestChannel implements Channel<A2ARequest> {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final int port;
    private final String channel;

    RedisRequestChannel(String host, int port, String channel) {
      this.host = host;
      this.port = port;
      this.channel = channel;
    }

    @Override
    public DataStream<A2ARequest> open(StreamExecutionEnvironment env) {
      return env.fromSource(
              new org.agentic.flink.channel.source.PollingSource<>(
                  new RedisRequestPollFn(host, port, channel)),
              org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
              "a2a-bridge-redis-requests",
              elementType())
          .setParallelism(1);
    }

    @Override
    public TypeInformation<A2ARequest> elementType() {
      return A2AJsonTypeInfo.of(A2ARequest.class);
    }

    @Override
    public String providerName() {
      return "a2a-redis";
    }
  }

  static final class RedisRequestPollFn
      implements org.agentic.flink.channel.source.PollingSource.PollFn<A2ARequest> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisRequestPollFn.class);
    private final String host;
    private final int port;
    private final String channel;
    private transient JedisPool pool;

    RedisRequestPollFn(String host, int port, String channel) {
      this.host = host;
      this.port = port;
      this.channel = channel;
    }

    @Override
    public void open(int subtaskIndex) {
      pool = new JedisPool(host, port);
    }

    @Override
    public A2ARequest poll(long timeoutMs) {
      // BLPOP blocks up to BLPOP_TIMEOUT_SECONDS then returns null — so requests queued before we
      // started are still delivered (no lost messages), and shutdown is seen within ~2s.
      try (Jedis jedis = pool.getResource()) {
        List<String> res = jedis.blpop(BLPOP_TIMEOUT_SECONDS, channel);
        if (res == null || res.size() != 2) {
          return null;
        }
        try {
          return A2AJson.mapper().readValue(res.get(1), A2ARequest.class);
        } catch (Exception e) {
          LOG.warn("Dropping malformed A2A request: {}", e.getMessage());
          return null;
        }
      } catch (RuntimeException e) {
        LOG.warn("Redis request BLPOP error, will retry: {}", e.getMessage());
        sleepQuietly();
        return null;
      }
    }

    @Override
    public void close() {
      if (pool != null) {
        pool.close();
      }
    }
  }

  // ==================== Flink response sink ====================

  static final class RedisResponseWriteFn
      implements org.agentic.flink.channel.sink.ForEachSink.WriteFn<A2AResponse> {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final int port;
    private final String channel;
    private transient JedisPool pool;

    RedisResponseWriteFn(String host, int port, String channel) {
      this.host = host;
      this.port = port;
      this.channel = channel;
    }

    @Override
    public void open(int subtaskIndex) {
      pool = new JedisPool(host, port);
    }

    @Override
    public void write(A2AResponse value) throws Exception {
      try (Jedis jedis = pool.getResource()) {
        jedis.rpush(channel, A2AJson.mapper().writeValueAsString(value));
      }
    }

    @Override
    public void close() {
      if (pool != null) {
        pool.close();
      }
    }
  }

  // ==================== Gateway connector ====================

  static final class RedisConnector extends AbstractA2AGatewayConnector {
    private static final Logger LOG = LoggerFactory.getLogger(RedisConnector.class);

    private final JedisPool pool;
    private final String requestChannel;
    private final String responseChannel;
    private final Thread pollThread;
    private volatile boolean running = true;

    RedisConnector(String host, int port, String requestChannel, String responseChannel) {
      this.pool = new JedisPool(host, port);
      this.requestChannel = requestChannel;
      this.responseChannel = responseChannel;
      this.pollThread =
          new Thread(this::pollResponses, "a2a-redis-gateway-poll");
      this.pollThread.setDaemon(true);
      this.pollThread.start();
    }

    private void pollResponses() {
      while (running) {
        try (Jedis jedis = pool.getResource()) {
          while (running) {
            List<String> res = jedis.blpop(BLPOP_TIMEOUT_SECONDS, responseChannel);
            if (res == null || res.size() != 2) {
              continue;
            }
            try {
              deliver(A2AJson.mapper().readValue(res.get(1), A2AResponse.class));
            } catch (Exception e) {
              LOG.warn("Dropping malformed A2A response: {}", e.getMessage());
            }
          }
        } catch (RuntimeException e) {
          if (running) {
            LOG.warn("Redis response BLPOP loop error, reconnecting: {}", e.getMessage());
            sleepQuietly();
          }
        }
      }
    }

    @Override
    public void publishRequest(A2ARequest request) throws Exception {
      try (Jedis jedis = pool.getResource()) {
        jedis.rpush(requestChannel, A2AJson.mapper().writeValueAsString(request));
      }
    }

    @Override
    public void close() {
      running = false;
      pool.close();
      clearListeners();
    }
  }

  private static void sleepQuietly() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
