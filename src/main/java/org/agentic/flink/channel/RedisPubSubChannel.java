package org.agentic.flink.channel;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
    return env.addSource(new Source(host, port, channelName))
        .name("redis-pubsub-channel[" + channelName + "]")
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

  static final class Source implements SourceFunction<KeyedContextItem> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    private final String host;
    private final int port;
    private final String channelName;

    private volatile boolean running = true;
    private transient JedisPubSub subscription;

    Source(String host, int port, String channelName) {
      this.host = host;
      this.port = port;
      this.channelName = channelName;
    }

    @Override
    public void run(SourceContext<KeyedContextItem> ctx) {
      ObjectMapper mapper = new ObjectMapper();
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

      while (running) {
        try (Jedis jedis = new Jedis(host, port)) {
          subscription =
              new JedisPubSub() {
                @Override
                public void onMessage(String ch, String message) {
                  try {
                    KeyedContextItem out = mapper.readValue(message, KeyedContextItem.class);
                    synchronized (ctx.getCheckpointLock()) {
                      ctx.collect(out);
                    }
                  } catch (Exception e) {
                    LOG.warn("Failed to deserialize Redis pub/sub message: {}", e.getMessage());
                  }
                }
              };
          jedis.subscribe(subscription, channelName);
        } catch (Exception e) {
          if (running) {
            LOG.warn(
                "RedisPubSubChannel subscription dropped, reconnecting in 1s: {}", e.getMessage());
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
      if (subscription != null && subscription.isSubscribed()) {
        try {
          subscription.unsubscribe();
        } catch (Exception ignored) {
          // best-effort
        }
      }
    }
  }
}
