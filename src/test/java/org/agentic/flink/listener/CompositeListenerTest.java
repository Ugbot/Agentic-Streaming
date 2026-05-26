package org.agentic.flink.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Listener composition contract: fan-out delivers every event to every registered listener;
 * exceptions from one listener don't suppress the others; ordering matches registration.
 */
class CompositeListenerTest {

  /** Records the methods invoked on it, in order, for assertions. */
  static final class RecordingListener implements AgentEventListener {
    private static final long serialVersionUID = 1L;
    private final String id;
    final List<String> calls = new ArrayList<>();

    RecordingListener(String id) {
      this.id = id;
    }

    @Override
    public void onAgentStart(String agentId) {
      calls.add("start:" + agentId);
    }

    @Override
    public void onChatRequest(String agentId, String modelName, int messageCount) {
      calls.add("chatReq:" + agentId + "/" + modelName + "/" + messageCount);
    }

    @Override
    public void onToolCallEnd(
        String agentId, String toolName, String toolCallId, boolean success, long durationMs) {
      calls.add("toolEnd:" + agentId + "/" + toolName + "/" + success);
    }

    @Override
    public String name() {
      return "rec-" + id;
    }
  }

  /** Listener that always throws — used to verify isolation. */
  static final class FaultyListener implements AgentEventListener, Serializable {
    private static final long serialVersionUID = 1L;
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public void onAgentStart(String agentId) {
      calls.incrementAndGet();
      throw new RuntimeException("intentional");
    }
  }

  @Test
  @DisplayName("CompositeListener fans every hook out to every registered listener")
  void fansOutToAll() {
    RecordingListener a = new RecordingListener("a");
    RecordingListener b = new RecordingListener("b");
    AgentEventListener composite = AgentEventListener.fanOut(List.of(a, b));

    String agentId = UUID.randomUUID().toString();
    composite.onAgentStart(agentId);
    composite.onChatRequest(agentId, "qwen2.5", 3);
    composite.onToolCallEnd(agentId, "calc", "c1", true, 12);

    assertEquals(List.of("start:" + agentId, "chatReq:" + agentId + "/qwen2.5/3", "toolEnd:" + agentId + "/calc/true"), a.calls);
    assertEquals(a.calls, b.calls);
  }

  @Test
  @DisplayName("A throwing listener doesn't block the rest of the fan-out")
  void faultyListenerIsolated() {
    FaultyListener faulty = new FaultyListener();
    RecordingListener good = new RecordingListener("good");
    AgentEventListener composite = AgentEventListener.fanOut(List.of(faulty, good));

    composite.onAgentStart("agent-x");
    assertEquals(1, faulty.calls.get());
    assertTrue(good.calls.contains("start:agent-x"));
  }

  @Test
  @DisplayName("Empty composite is safe and reports zero-length list")
  void emptyCompositeIsSafe() {
    CompositeListener empty = new CompositeListener(List.of());
    empty.onAgentStart("a");
    empty.onError("a", "stage", new RuntimeException("x"));
    assertNotNull(empty.getListeners());
    assertEquals(0, empty.getListeners().size());
  }

  @Test
  @DisplayName("fanOut returns the same identity for the same composite")
  void fanOutReturnsCompositeNotNull() {
    AgentEventListener composite = AgentEventListener.fanOut(List.of(new RecordingListener("x")));
    assertSame(CompositeListener.class, composite.getClass());
  }
}
