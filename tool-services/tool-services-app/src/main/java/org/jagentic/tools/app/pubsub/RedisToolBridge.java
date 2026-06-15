package org.jagentic.tools.app.pubsub;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.arc.properties.IfBuildProperty;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import org.jagentic.core.ToolRegistry;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Async Redis pub-sub transport: subscribe to a request channel, run the tool, and publish the
 * result to a reply channel. Request/result are JSON strings ({@code {id, tool, args}} -&gt;
 * {@code {id, ok, result|error}}), correlated by {@code id}.
 *
 * <p><b>Off by default.</b> Gated by the build property {@code tools.redis.enabled=true}
 * ({@code enableIfMissing=false}) so the default build/boot has no Redis subscriber and never
 * touches a Redis server. To enable: build/run with {@code -Dtools.redis.enabled=true}; the
 * Redis URL, request channel, and reply channel are read from config (defaults below).
 *
 * <p>The blocking {@code subscribe(...)} loop runs on a dedicated daemon thread (a separate Jedis
 * connection is used to publish replies, since the subscribed connection cannot issue commands).
 */
@ApplicationScoped
@IfBuildProperty(name = "tools.redis.enabled", stringValue = "true", enableIfMissing = false)
public class RedisToolBridge {

  private static final Logger LOG = Logger.getLogger(RedisToolBridge.class);

  @Inject
  ToolRegistry registry;

  @Inject
  ObjectMapper mapper;

  @ConfigProperty(name = "tools.redis.url", defaultValue = "redis://localhost:6379")
  String redisUrl;

  @ConfigProperty(name = "tools.redis.request-channel", defaultValue = "tool-requests")
  String requestChannel;

  @ConfigProperty(name = "tools.redis.reply-channel", defaultValue = "tool-results")
  String replyChannel;

  private ToolPubSub pubsub;
  private volatile Thread subscriberThread;
  private volatile JedisPubSub subscriber;
  private volatile Jedis subscribeConnection;

  void onStart(@Observes StartupEvent ev) {
    this.pubsub = new ToolPubSub(registry, mapper);

    // A dedicated Jedis connection for publishing replies (the subscribed connection is blocked
    // in subscribe() and cannot issue other commands).
    final Jedis publishConnection = new Jedis(redisUrl);

    this.subscriber = new JedisPubSub() {
      @Override
      public void onMessage(String channel, String message) {
        try {
          String reply = pubsub.handle(message);
          publishConnection.publish(replyChannel, reply);
        } catch (Exception e) {
          LOG.errorf(e, "Redis tool bridge failed handling a request on channel %s", channel);
        }
      }
    };

    Thread t = new Thread(() -> {
      // subscribe() blocks until unsubscribed; reconnect would be a deployment concern, kept simple.
      try (Jedis conn = new Jedis(redisUrl)) {
        this.subscribeConnection = conn;
        LOG.infof("Redis tool bridge subscribing on '%s', replying on '%s' (%s)",
            requestChannel, replyChannel, redisUrl);
        conn.subscribe(subscriber, requestChannel);
      } catch (Exception e) {
        LOG.errorf(e, "Redis tool bridge subscriber stopped");
      } finally {
        try {
          publishConnection.close();
        } catch (Exception ignore) {
          // best-effort close
        }
      }
    }, "redis-tool-bridge");
    t.setDaemon(true);
    this.subscriberThread = t;
    t.start();
  }

  void onStop(@Observes ShutdownEvent ev) {
    try {
      if (subscriber != null && subscriber.isSubscribed()) {
        subscriber.unsubscribe();
      }
    } catch (Exception ignore) {
      // best-effort
    }
    if (subscribeConnection != null) {
      try {
        subscribeConnection.close();
      } catch (Exception ignore) {
        // best-effort
      }
    }
    if (subscriberThread != null) {
      subscriberThread.interrupt();
    }
  }
}
