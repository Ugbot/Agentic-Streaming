package org.agentic.flink.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ToolInvocationChannelTest {

  public record UrlRequest(String url, String source) {}

  private String toolId;

  @BeforeEach
  void freshQueue() {
    toolId = "crawl-" + UUID.randomUUID();
  }

  @Test
  @DisplayName("in-JVM transport queues invocations and reports the correct transport label")
  void inJvmEmitsAndLabels() throws Exception {
    ToolInvocationChannel<UrlRequest> channel =
        ToolInvocationChannel.inJvm(
            toolId,
            UrlRequest.class,
            params -> new UrlRequest((String) params.get("url"), "agent"));

    Object res = channel.execute(Map.of("url", "https://example.com/a")).get();
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) res;
    assertEquals(Boolean.TRUE, result.get("queued"));
    assertEquals("in-jvm", result.get("transport"));
    assertEquals(toolId, channel.getToolId());
    assertEquals(ToolInvocationChannel.Transport.IN_JVM, channel.getTransport());
  }

  @Test
  @DisplayName("side-output transport falls back to the in-JVM queue when no context is set")
  void sideOutputFallsBackWithoutContext() throws Exception {
    ToolInvocationChannel<UrlRequest> channel =
        ToolInvocationChannel.sideOutput(
            toolId,
            UrlRequest.class,
            params -> new UrlRequest((String) params.get("url"), "agent"));
    ToolInvocationChannel.setCurrentContext(null);

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) channel.execute(Map.of("url", "https://example.com")).get();
    assertEquals(Boolean.TRUE, result.get("queued"));
    assertEquals("side-output:fallback", result.get("transport"));
  }

  @Test
  @DisplayName("side-output transport emits to the configured OutputTag when a context is set")
  void sideOutputEmitsWithContext() throws Exception {
    ToolInvocationChannel<UrlRequest> channel =
        ToolInvocationChannel.sideOutput(
            toolId,
            UrlRequest.class,
            params -> new UrlRequest((String) params.get("url"), "agent"));

    final java.util.concurrent.atomic.AtomicReference<UrlRequest> emitted =
        new java.util.concurrent.atomic.AtomicReference<>();
    final java.util.concurrent.atomic.AtomicReference<OutputTag<?>> seenTag =
        new java.util.concurrent.atomic.AtomicReference<>();

    ToolInvocationChannel.EmitContext<UrlRequest> emit =
        (tag, value) -> {
          seenTag.set(tag);
          emitted.set(value);
        };
    ToolInvocationChannel.setCurrentContext(emit);
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> result =
          (Map<String, Object>) channel.execute(Map.of("url", "https://example.com")).get();
      assertEquals(Boolean.TRUE, result.get("queued"));
      assertEquals("side-output", result.get("transport"));
      assertNotNull(emitted.get());
      assertEquals("https://example.com", emitted.get().url());
      assertNotNull(seenTag.get());
      assertEquals(toolId, seenTag.get().getId());
    } finally {
      ToolInvocationChannel.setCurrentContext(null);
    }
  }

  @Test
  @DisplayName("external transport publishes through the wrapped channel's consumer")
  void externalPublishesThroughConsumer() throws Exception {
    java.util.List<UrlRequest> sink = new java.util.ArrayList<>();
    Channel<UrlRequest> wrapped =
        new StaticSeedChannel<UrlRequest>(
            java.util.List.<UrlRequest>of(),
            org.apache.flink.api.common.typeinfo.TypeInformation.of(UrlRequest.class));
    ToolInvocationChannel<UrlRequest> channel =
        ToolInvocationChannel.via(
            toolId,
            UrlRequest.class,
            params -> new UrlRequest((String) params.get("url"), "agent"),
            wrapped,
            sink::add);

    channel.execute(Map.of("url", "https://example.com")).get();
    channel.execute(Map.of("url", "https://example.org")).get();
    assertEquals(2, sink.size());
    assertEquals("https://example.com", sink.get(0).url());
    assertEquals("https://example.org", sink.get(1).url());
  }

  @Test
  @DisplayName("OutputTag is consistent across calls")
  void outputTagStable() {
    ToolInvocationChannel<UrlRequest> channel =
        ToolInvocationChannel.sideOutput(
            toolId, UrlRequest.class, params -> new UrlRequest("x", "y"));
    OutputTag<UrlRequest> t1 = channel.outputTag();
    OutputTag<UrlRequest> t2 = channel.outputTag();
    assertEquals(t1, t2);
    assertEquals(toolId, t1.getId());
    assertTrue(channel.providerName().contains("tool-channel"));
  }
}
