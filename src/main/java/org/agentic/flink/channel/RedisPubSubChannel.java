package org.agentic.flink.channel;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.channel.source.PollingSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Channel that subscribes to a Redis pub/sub channel and emits each JSON-encoded {@link
 * KeyedContextItem}.
 *
 * <p>Wire format mirrors {@link KafkaContextChannel}: a JSON object with {@code flowId} and
 * {@code item}. Requires Jedis on the runtime classpath — the dependency is marked optional in
 * the project's pom, so users who don't need Redis don't pull it transitively.
 *
 * <p>This source is single-parallelism by design. A subscriber per task would receive duplicate
 * messages from Redis.
 *
 * <p>Migrated from {@code RedisPubSubFeed}; behaviour unchanged.
 */
public final class RedisPubSubChannel implements Channel<KeyedContextItem> {
  private static final long serialVersionUID = 1L;

  private final String host;
  private final int port;
  private final String channelName;

  public RedisPubSubChannel(String host, int port, String channelName) {
    this.host = host;
    this.port = port;
    this.channelName = channelName;
  }

  @Override
  public DataStream<KeyedContextItem> open(StreamExecutionEnvironment env) {
    return env.fromSource(
            new PollingSource<>(new RedisPubSubPollFn(host, port, channelName)),
            WatermarkStrategy.noWatermarks(),
            "redis-pubsub-channel[" + channelName + "]",
            elementType())
        .setParallelism(1);
  }

  @Override
  public TypeInformation<KeyedContextItem> elementType() {
    return TypeInformation.of(new TypeHint<KeyedContextItem>() {});
  }

  @Override
  public String providerName() {
    return "redis-pubsub";
  }

  /**
   * Native FLIP-27 {@link PollingSource.PollFn} for Redis pub/sub. Jedis {@code subscribe} is a
   * blocking, push-callback API, so it runs on its own daemon thread feeding a queue; {@link #poll}
   * drains the queue. Closed on the poll thread, which unsubscribes + stops the subscriber thread.
   */
  static final class RedisPubSubPollFn implements PollingSource.PollFn<KeyedContextItem> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisPubSubPollFn.class);

    private final String host;
    private final int port;
    private final String channelName;

    private transient LinkedBlockingQueue<KeyedContextItem> queue;
    private transient volatile boolean running;
    private transient volatile JedisPubSub subscription;
    private transient Thread subThread;

    RedisPubSubPollFn(String host, int port, String channelName) {
      this.host = host;
      this.port = port;
      this.channelName = channelName;
    }

    @Override
    public void open(int subtaskIndex) {
      queue = new LinkedBlockingQueue<>();
      running = true;
      final ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.registerModule(new ParameterNamesModule());
      mapper.setVisibility(
          mapper
              .getSerializationConfig()
              .getDefaultVisibilityChecker()
              .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
              .withGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
              .withSetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)
              .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));

      subThread =
          new Thread(
              () -> {
                while (running) {
                  try (Jedis jedis = new Jedis(host, port)) {
                    subscription =
                        new JedisPubSub() {
                          @Override
                          public void onMessage(String ch, String message) {
                            try {
                              queue.add(mapper.readValue(message, KeyedContextItem.class));
                            } catch (Exception e) {
                              LOG.warn("Failed to deserialize Redis pub/sub message: {}", e.getMessage());
                            }
                          }
                        };
                    jedis.subscribe(subscription, channelName);
                  } catch (Exception e) {
                    if (running) {
                      LOG.warn("RedisPubSubChannel subscription dropped, reconnecting in 1s: {}", e.getMessage());
                      try {
                        Thread.sleep(1000);
                      } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                      }
                    }
                  }
                }
              },
              "redis-pubsub-" + channelName);
      subThread.setDaemon(true);
      subThread.start();
    }

    @Override
    public KeyedContextItem poll(long timeoutMs) throws InterruptedException {
      return queue.poll(Math.max(1, timeoutMs), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
      running = false;
      JedisPubSub sub = subscription;
      if (sub != null && sub.isSubscribed()) {
        try {
          sub.unsubscribe();
        } catch (Exception ignored) {
          // best-effort
        }
      }
      if (subThread != null) {
        subThread.interrupt();
      }
    }
  }
}
