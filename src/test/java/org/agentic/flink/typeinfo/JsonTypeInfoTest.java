package org.agentic.flink.typeinfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.example.banking.safety.RoutingBudget;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.llm.ChatRole;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The Jackson-backed {@link JsonTypeInfo} serializer round-trips the framework's would-be-Kryo value
 * types through Flink's {@link TypeSerializer} contract — proving they no longer need Kryo, and that
 * mutable types are deep-copied (not aliased) on {@code copy}.
 */
class JsonTypeInfoTest {

  private static <T> T roundTrip(JsonTypeInfo<T> info, T value) throws Exception {
    TypeSerializer<T> ser = info.createSerializer(new ExecutionConfig());
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ser.serialize(value, new DataOutputViewStreamWrapper(bos));
    return ser.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(bos.toByteArray())));
  }

  @Test
  @DisplayName("immutable ChatMessage round-trips (role + tool identity preserved)")
  void chatMessageRoundTrips() throws Exception {
    String callId = UUID.randomUUID().toString();
    ChatMessage restored =
        roundTrip(JsonTypeInfo.of(ChatMessage.class), ChatMessage.tool(callId, "kb_search", "result"));
    assertEquals(ChatRole.TOOL, restored.getRole());
    assertEquals(callId, restored.getToolCallId());
    assertEquals("kb_search", restored.getToolName());
    assertEquals("result", restored.getContent());
  }

  @Test
  @DisplayName("mutable AgentEvent round-trips its Map<String,Object> data and deep-copies")
  void agentEventRoundTripsAndCopies() throws Exception {
    AgentEvent ev = new AgentEvent();
    ev.setFlowId("flow-1");
    ev.setEventType(AgentEventType.FLOW_STARTED);
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("k", "v");
    data.put("n", 7);
    ev.setData(data);

    JsonTypeInfo<AgentEvent> info = JsonTypeInfo.of(AgentEvent.class, true);
    AgentEvent restored = roundTrip(info, ev);
    assertEquals("flow-1", restored.getFlowId());
    assertEquals(AgentEventType.FLOW_STARTED, restored.getEventType());
    assertEquals("v", restored.getData().get("k"));
    assertEquals(7, ((Number) restored.getData().get("n")).intValue());

    // Mutable copy() must deep-copy so Flink object reuse can't alias live state.
    TypeSerializer<AgentEvent> ser = info.createSerializer(new ExecutionConfig());
    assertFalse(ser.isImmutableType());
    AgentEvent copy = ser.copy(ev);
    assertNotSame(ev, copy);
    copy.getData().put("k", "mutated");
    assertEquals("v", ev.getData().get("k"), "deep copy must not share the data map");
  }

  @Test
  @DisplayName("RoutingBudget round-trips caps + mutable counters + recentHashes (replaces byte[] hack)")
  void routingBudgetRoundTrips() throws Exception {
    RoutingBudget b = new RoutingBudget(3, 5, 1000L, 2);
    b.startTurn(0L);
    assertTrue(b.allowRoundTrip());
    assertTrue(b.allowRoundTrip()); // roundTrips = 2
    assertTrue(b.allowDispatch("h1")); // recentHashes now contains h1

    RoutingBudget restored = roundTrip(JsonTypeInfo.of(RoutingBudget.class, true), b);
    assertEquals(3, restored.maxRoundTrips());
    assertEquals(2, restored.roundTripsUsed(), "mutable counter must survive");
    assertFalse(restored.allowDispatch("h1"), "recentHashes (the ArrayDeque) must survive");
  }
}
