package org.agentic.flink.a2a.bridge;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.a2a.A2AJson;
import org.agentic.flink.channel.Channel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

/**
 * Redis pub/sub {@link A2ABridge} — distributed-light transport (requires the optional Jedis dep).
 *
 * <p>Requests are published to the {@code requestChannel} Redis channel and consumed by the Flink
 * source; responses are published to the {@code responseChannel} and consumed by the gateway. JSON
 * via {@link A2AJson}. Unlike ZeroMQ this needs a Redis server but no socket bind/connect topology.
 */
public final class RedisA2ABridge implements A2ABridge {
  private static final long serialVersionUID = 1L;

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
  public org.apache.flink.streaming.api.functions.sink.SinkFunction<A2AResponse> responseSink() {
    return new RedisResponseSink(host, port, responseChannel);
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
      return env.addSource(new RedisRequestSource(host, port, channel), elementType())
          .name("a2a-bridge-redis-requests");
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

  static final class RedisRequestSource implements SourceFunction<A2ARequest> {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final int port;
    private final String channel;
    private volatile boolean running = true;
    private transient JedisPubSub pubSub;

    RedisRequestSource(String host, int port, String channel) {
      this.host = host;
      this.port = port;
      this.channel = channel;
    }

    @Override
    public void run(SourceContext<A2ARequest> ctx) {
      LinkedBlockingQueue<A2ARequest> queue = new LinkedBlockingQueue<>();
      pubSub =
          new JedisPubSub() {
            @Override
            public void onMessage(String ch, String message) {
              try {
                queue.offer(A2AJson.mapper().readValue(message, A2ARequest.class));
              } catch (Exception e) {
                // skip malformed
              }
            }
          };
      JedisPool pool = new JedisPool(host, port);
      Thread sub =
          new Thread(
              () -> {
                try (Jedis jedis = pool.getResource()) {
                  jedis.subscribe(pubSub, channel);
                } catch (RuntimeException ignored) {
                  // unsubscribe / shutdown
                }
              },
              "a2a-redis-sub");
      sub.setDaemon(true);
      sub.start();
      try {
        while (running) {
          A2ARequest req = queue.poll(200, TimeUnit.MILLISECONDS);
          if (req != null) {
            synchronized (ctx.getCheckpointLock()) {
              ctx.collect(req);
            }
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        if (pubSub != null && pubSub.isSubscribed()) {
          pubSub.unsubscribe();
        }
        pool.close();
      }
    }

    @Override
    public void cancel() {
      running = false;
      if (pubSub != null && pubSub.isSubscribed()) {
        pubSub.unsubscribe();
      }
    }
  }

  // ==================== Flink response sink ====================

  static final class RedisResponseSink extends RichSinkFunction<A2AResponse> {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final int port;
    private final String channel;
    private transient JedisPool pool;

    RedisResponseSink(String host, int port, String channel) {
      this.host = host;
      this.port = port;
      this.channel = channel;
    }

    @Override
    public void open(Configuration parameters) {
      pool = new JedisPool(host, port);
    }

    @Override
    public void invoke(A2AResponse value, Context context) throws Exception {
      try (Jedis jedis = pool.getResource()) {
        jedis.publish(channel, A2AJson.mapper().writeValueAsString(value));
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
    private final JedisPubSub pubSub;
    private final Thread subThread;

    RedisConnector(String host, int port, String requestChannel, String responseChannel) {
      this.pool = new JedisPool(host, port);
      this.requestChannel = requestChannel;
      this.pubSub =
          new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
              try {
                deliver(A2AJson.mapper().readValue(message, A2AResponse.class));
              } catch (Exception e) {
                LOG.warn("Dropping malformed A2A response: {}", e.getMessage());
              }
            }
          };
      this.subThread =
          new Thread(
              () -> {
                try (Jedis jedis = pool.getResource()) {
                  jedis.subscribe(pubSub, responseChannel);
                } catch (RuntimeException ignored) {
                  // shutdown
                }
              },
              "a2a-redis-gateway-sub");
      this.subThread.setDaemon(true);
      this.subThread.start();
    }

    @Override
    public void publishRequest(A2ARequest request) throws Exception {
      try (Jedis jedis = pool.getResource()) {
        jedis.publish(requestChannel, A2AJson.mapper().writeValueAsString(request));
      }
    }

    @Override
    public void close() {
      if (pubSub.isSubscribed()) {
        pubSub.unsubscribe();
      }
      pool.close();
      clearListeners();
    }
  }
}
