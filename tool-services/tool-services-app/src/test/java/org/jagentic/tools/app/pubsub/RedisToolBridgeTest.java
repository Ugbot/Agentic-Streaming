package org.jagentic.tools.app.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.inject.Inject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import org.jagentic.core.ToolRegistry;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Round-trip test for the Redis pub-sub contract against a live Redis. The production
 * {@link RedisToolBridge} is build-gated OFF by default, so this test stands up the same
 * {@link ToolPubSub} handler on a Jedis subscriber itself — proving the {@code {id,tool,args}}
 * -&gt; {@code {id,ok,result}} envelope works exactly as the bridge would run it.
 *
 * <p><b>Skips (never fails) when no Redis is reachable on localhost:6379.</b> Bring one up with
 * {@code podman run -d -p 6379:6379 docker.io/library/redis:7} to exercise it for real.
 */
@QuarkusTest
class RedisToolBridgeTest {

  private static final String HOST = "localhost";
  private static final int PORT = 6379;
  private static final String REQUESTS = "tool-requests-test";
  private static final String REPLIES = "tool-results-test";

  @Inject
  ToolRegistry registry;

  @Inject
  ObjectMapper mapper;

  private static boolean redisReachable() {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(HOST, PORT), 500);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Test
  void redisRoundTrip() throws Exception {
    assumeTrue(redisReachable(), "no Redis on " + HOST + ":" + PORT + " — skipping");

    ToolPubSub pubsub = new ToolPubSub(registry, mapper);
    String url = "redis://" + HOST + ":" + PORT;

    // Subscriber: mirror the bridge — on a request, run the tool, publish the reply.
    Jedis publisher = new Jedis(url);
    CountDownLatch subscribed = new CountDownLatch(1);
    JedisPubSub requestHandler = new JedisPubSub() {
      @Override
      public void onSubscribe(String channel, int subscribedChannels) {
        subscribed.countDown();
      }

      @Override
      public void onMessage(String channel, String message) {
        publisher.publish(REPLIES, pubsub.handle(message));
      }
    };
    Thread sub = new Thread(() -> {
      try (Jedis conn = new Jedis(url)) {
        conn.subscribe(requestHandler, REQUESTS);
      }
    }, "redis-test-subscriber");
    sub.setDaemon(true);
    sub.start();
    assertTrue(subscribed.await(5, TimeUnit.SECONDS), "subscriber did not come up");

    // Reply listener captures the correlated result.
    String id = UUID.randomUUID().toString();
    CountDownLatch replied = new CountDownLatch(1);
    AtomicReference<String> reply = new AtomicReference<>();
    CountDownLatch replyReady = new CountDownLatch(1);
    JedisPubSub replyHandler = new JedisPubSub() {
      @Override
      public void onSubscribe(String channel, int subscribedChannels) {
        replyReady.countDown();
      }

      @Override
      public void onMessage(String channel, String message) {
        reply.set(message);
        replied.countDown();
      }
    };
    Thread rsub = new Thread(() -> {
      try (Jedis conn = new Jedis(url)) {
        conn.subscribe(replyHandler, REPLIES);
      }
    }, "redis-test-reply-listener");
    rsub.setDaemon(true);
    rsub.start();
    assertTrue(replyReady.await(5, TimeUnit.SECONDS), "reply listener did not come up");

    // Publish a request: util_add(40, 2).
    try (Jedis client = new Jedis(url)) {
      String request = mapper.writeValueAsString(Map.of(
          "id", id,
          "tool", "util_add",
          "args", Map.of("a", 40, "b", 2)));
      client.publish(REQUESTS, request);
    }

    assertTrue(replied.await(5, TimeUnit.SECONDS), "no reply received from the Redis bridge");

    Map<String, Object> result =
        mapper.readValue(reply.get(), new TypeReference<Map<String, Object>>() {});
    assertEquals(id, result.get("id"), "reply must correlate by id");
    assertEquals(Boolean.TRUE, result.get("ok"), "tool call should succeed: " + reply.get());
    assertEquals(42.0, ((Number) result.get("result")).doubleValue(), 1e-9);

    requestHandler.unsubscribe();
    replyHandler.unsubscribe();
    publisher.close();
  }
}
