package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

/**
 * {@link RedisPubSubChannel} against a real Redis (Testcontainers on Podman). Publishes JSON
 * encoded {@link KeyedContextItem}s on a channel and reads them back through the poll function that
 * the FLIP-27 source drives.
 */
@Tag("integration")
class RedisPubSubChannelIT {

  private static GenericContainer<?> redis;

  @BeforeAll
  static void startRedis() {
    redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
    redis.start();
  }

  @AfterAll
  static void stopRedis() {
    if (redis != null) redis.stop();
  }

  private static boolean subscribed(String channelName) {
    try (Jedis jedis = new Jedis(redis.getHost(), redis.getMappedPort(6379))) {
      return jedis.pubsubNumSub(channelName).getOrDefault(channelName, 0L) > 0;
    }
  }

  @Test
  void deliversPublishedItemsInOrderAndIgnoresMalformedPayloads() throws Exception {
    String channelName = "ctx-" + UUID.randomUUID();
    RedisPubSubChannel channel =
        new RedisPubSubChannel(redis.getHost(), redis.getMappedPort(6379), channelName);
    assertEquals("redis-pubsub", channel.providerName());

    RedisPubSubChannel.RedisPubSubPollFn fn =
        new RedisPubSubChannel.RedisPubSubPollFn(
            redis.getHost(), redis.getMappedPort(6379), channelName);
    fn =
        InstantiationUtil.deserializeObject(
            InstantiationUtil.serializeObject(fn), getClass().getClassLoader());
    fn.open(0);
    try {
      long deadline = System.currentTimeMillis() + 30_000;
      while (!subscribed(channelName) && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertTrue(subscribed(channelName), "poll function subscribed to " + channelName);

      int n = ThreadLocalRandom.current().nextInt(2, 10);
      List<String> flowIds = new ArrayList<>();
      List<String> contents = new ArrayList<>();
      ObjectMapper mapper = new ObjectMapper();
      try (Jedis publisher = new Jedis(redis.getHost(), redis.getMappedPort(6379))) {
        for (int i = 0; i < n; i++) {
          String flowId = "flow-" + UUID.randomUUID();
          String content = "content-" + UUID.randomUUID();
          flowIds.add(flowId);
          contents.add(content);
          KeyedContextItem item =
              new KeyedContextItem(
                  flowId, new ContextItem(content, ContextPriority.SHOULD, MemoryType.SHORT_TERM));
          publisher.publish(channelName, mapper.writeValueAsString(item));
          if (i == n / 2) {
            publisher.publish(channelName, "not json " + UUID.randomUUID());
          }
        }
      }

      for (int i = 0; i < n; i++) {
        KeyedContextItem got = null;
        long until = System.currentTimeMillis() + 10_000;
        while (got == null && System.currentTimeMillis() < until) {
          got = fn.poll(200);
        }
        assertTrue(got != null, "item " + i + " arrived");
        assertEquals(flowIds.get(i), got.getFlowId());
        assertEquals(contents.get(i), got.getItem().getContent());
      }
      assertNull(fn.poll(200), "malformed payload was dropped, nothing else pending");
    } finally {
      fn.close();
    }
  }
}
