package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.agentic.flink.typeinfo.FlinkJson;
import org.agentic.flink.typeinfo.JsonTypeInfo;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;
import org.jagentic.core.ToolCall;
import org.jagentic.core.TurnError;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Exercises the Flink serializers the runtime registers for its stream and state types through
 * Flink's own {@code DataOutputSerializer}/{@code DataInputDeserializer} paths, including
 * serializer snapshots (what a savepoint stores) and Java serialization of the operator itself.
 */
class RuntimeSerializationTest {

  private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

  private static String rnd(String p) {
    return p + "-" + UUID.randomUUID();
  }

  private static Map<String, Object> randomPayload() {
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("n", RND.nextLong());
    p.put("d", RND.nextDouble());
    p.put("b", RND.nextBoolean());
    p.put("s", rnd("s"));
    p.put("nested", Map.of("list", List.of(1, "two", 3.5), "user", rnd("u")));
    return p;
  }

  private static Event randomEvent() {
    if (RND.nextBoolean()) {
      return Event.turn(rnd("c"), rnd("t"), rnd("u"), "text " + RND.nextInt());
    }
    return Event.resume(rnd("c"), rnd("t"), Map.of("kind", "approval", "approved", RND.nextBoolean(), "n", 3));
  }

  private static LogEvent randomLogEvent() {
    EventType[] types = EventType.values();
    return new LogEvent(rnd("c"), RND.nextLong(0, 1_000_000), rnd("t"), types[RND.nextInt(types.length)].wire(),
        randomPayload());
  }

  private static TurnResult randomResult() {
    String cid = rnd("c");
    String tid = rnd("t");
    List<ToolCall> calls = List.of(
        ToolCall.succeeded("lookup", 0, Map.of("user", rnd("u"), "amount", 12.5), Map.of("ok", true), 1),
        ToolCall.failed("broken", 1, Map.of("id", RND.nextInt()), "boom " + RND.nextInt(), 2));
    List<LogEvent> events = new ArrayList<>();
    for (int i = 0; i < 1 + RND.nextInt(5); i++) {
      LogEvent e = randomLogEvent();
      events.add(new LogEvent(cid, i, tid, e.type(), e.payload()));
    }
    Map<String, Object> state = new LinkedHashMap<>();
    state.put("turn_count", (long) RND.nextInt(50));
    state.put("phase", rnd("phase"));
    TurnStatus[] statuses = TurnStatus.values();
    TurnStatus status = statuses[RND.nextInt(statuses.length)];
    TurnError err = RND.nextBoolean() ? null : new TurnError(TurnError.ErrorClass.TOOL, rnd("msg"));
    return new TurnResult(cid, tid, status, RND.nextBoolean() ? "billing" : null, rnd("reply"), err, calls, events,
        state);
  }

  private static <T> T roundTrip(TypeInformation<T> info, T value) throws IOException {
    TypeSerializer<T> ser = info.createSerializer(new SerializerConfigImpl());
    DataOutputSerializer out = new DataOutputSerializer(256);
    ser.serialize(value, out);
    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    T back = ser.deserialize(in);
    assertEquals(0, in.available(), "serializer consumed exactly what it wrote");
    return back;
  }

  private static <T> void snapshotRoundTrip(TypeInformation<T> info) throws IOException {
    TypeSerializer<T> ser = info.createSerializer(new SerializerConfigImpl());
    TypeSerializerSnapshot<T> snapshot = ser.snapshotConfiguration();
    DataOutputSerializer out = new DataOutputSerializer(64);
    TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(out, snapshot);
    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    TypeSerializerSnapshot<T> restored = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(in,
        Thread.currentThread().getContextClassLoader());
    TypeSerializerSchemaCompatibility<T> compat = ser.snapshotConfiguration().resolveSchemaCompatibility(restored);
    assertTrue(compat.isCompatibleAsIs(), "restored snapshot is compatible with the current serializer: " + compat);
    assertNotNull(restored.restoreSerializer());
  }

  @RepeatedTest(5)
  void eventRoundTripsThroughFlinkSerializer() throws IOException {
    Event e = randomEvent();
    Event back = roundTrip(WorkflowTurnFunction.EVENT_TYPE, e);
    assertEquals(e, back);
    assertEquals(e.isResume(), back.isResume());
    if (e.isResume()) {
      assertEquals(e.signal().get("approved"), back.signal().get("approved"), "structured signal survives");
    }
  }

  @RepeatedTest(5)
  void logEventRoundTripsThroughFlinkSerializer() throws IOException {
    LogEvent e = randomLogEvent();
    LogEvent back = roundTrip(WorkflowTurnFunction.LOG_EVENT_TYPE, e);
    assertEquals(e.conversationId(), back.conversationId());
    assertEquals(e.sequence(), back.sequence());
    assertEquals(e.turnId(), back.turnId());
    assertEquals(e.type(), back.type());
    assertEquals(e.payload().get("s"), back.payload().get("s"));
    assertEquals(((Number) e.payload().get("n")).longValue(), ((Number) back.payload().get("n")).longValue());
    assertEquals(Map.of("list", List.of(1, "two", 3.5), "user", e.payload().get("nested") instanceof Map<?, ?> m
        ? m.get("user") : null), back.payload().get("nested"), "nested structured payload is preserved as maps");
  }

  @RepeatedTest(5)
  void turnResultRoundTripsThroughFlinkSerializerAsNormalizedDocument() throws IOException {
    TurnResult r = randomResult();
    TurnResult back = roundTrip(TurnResultTypeInfo.INSTANCE, r);
    assertEquals(FlinkJson.mapper().writeValueAsString(r.toMap()), FlinkJson.mapper().writeValueAsString(back.toMap()),
        "the normalized result document is the wire format (JSON numbers carry no int/long distinction)");
    assertEquals(((Number) r.state.get("turn_count")).longValue(), ((Number) back.state.get("turn_count")).longValue());
    assertEquals(r.calls.get(0).args(), back.calls.get(0).args(), "tool args stay structured maps");
    assertEquals(r.status, back.status);
    assertEquals(r.error == null, back.error == null);
    for (int i = 0; i < r.events.size(); i++) {
      assertEquals(r.turnId, back.events.get(i).turnId());
      assertEquals(r.events.get(i).sequence(), back.events.get(i).sequence());
    }
  }

  @Test
  void serializerSnapshotsRestoreCompatibly() throws IOException {
    snapshotRoundTrip(WorkflowTurnFunction.EVENT_TYPE);
    snapshotRoundTrip(WorkflowTurnFunction.LOG_EVENT_TYPE);
    snapshotRoundTrip(TurnResultTypeInfo.INSTANCE);
  }

  @Test
  void runtimeTypesAreNotGenericKryoTypes() {
    assertFalse(WorkflowTurnFunction.EVENT_TYPE instanceof GenericTypeInfo);
    assertFalse(WorkflowTurnFunction.LOG_EVENT_TYPE instanceof GenericTypeInfo);
    assertFalse(((TypeInformation<?>) TurnResultTypeInfo.INSTANCE) instanceof GenericTypeInfo);
    assertEquals(TurnResultTypeInfo.INSTANCE, new WorkflowTurnFunction(
        org.agentic.flink.runtime.testkit.Workflows.billing()).getProducedType());
    assertTrue(WorkflowTurnFunction.EVENT_TYPE instanceof JsonTypeInfo);
  }

  @Test
  void operatorIsJavaSerializableWithTransientRuntimeFields() throws Exception {
    Map<String, Object> wf = org.agentic.flink.runtime.testkit.Workflows.withFlink(
        org.agentic.flink.runtime.testkit.Workflows.billing(),
        Map.of("state_ttl_ms", 1000 + RND.nextInt(100_000), "resume_after_ms", RND.nextInt(10_000),
            "timer_domain", "event_time"));
    WorkflowTurnFunction fn = new WorkflowTurnFunction(wf);
    byte[] bytes = InstantiationUtil.serializeObject(fn);
    WorkflowTurnFunction back = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
    assertEquals(fn.options(), back.options());
    assertEquals(TurnResultTypeInfo.INSTANCE, back.getProducedType());
  }
}
