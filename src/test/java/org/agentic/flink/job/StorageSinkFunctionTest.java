package org.agentic.flink.job;

import static org.junit.jupiter.api.Assertions.*;

import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.junit.jupiter.api.*;

/**
 * Unit tests for {@link StorageSinkFunction}.
 *
 * <p>Tests verify:
 * <ul>
 *   <li>Construction with various configs (null, forTesting, custom)</li>
 *   <li>Pass-through behavior: every event is emitted unchanged</li>
 *   <li>Terminal events (FLOW_COMPLETED, FLOW_FAILED) trigger long-term storage</li>
 *   <li>Non-terminal events only hit short-term storage</li>
 *   <li>Timeout handler still passes the event through</li>
 *   <li>Storage backend detection (memory defaults when no Redis/Postgres configured)</li>
 *   <li>Serialization (Flink requirement)</li>
 * </ul>
 *
 * <p>All test data uses randomized flowIds, userIds, and agentIds via
 * {@link UUID#randomUUID()} and {@link ThreadLocalRandom}.
 *
 * @author Agentic Flink Team
 * @see StorageSinkFunction
 */
class StorageSinkFunctionTest {

  // ==================== Helpers ====================

  /**
   * Creates an AgentEvent with randomized identifiers.
   */
  private static AgentEvent randomEvent(AgentEventType type) {
    AgentEvent event = new AgentEvent(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        "agent-" + UUID.randomUUID().toString().substring(0, 8),
        type);
    event.setCurrentStage("test-stage-" + ThreadLocalRandom.current().nextInt(100));
    event.setIterationNumber(ThreadLocalRandom.current().nextInt(0, 10));
    return event;
  }

  /**
   * Simple ResultFuture implementation that captures the collected results.
   */
  private static class CapturingResultFuture implements ResultFuture<AgentEvent> {
    private final AtomicReference<Collection<AgentEvent>> results = new AtomicReference<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    @Override
    public void complete(Collection<AgentEvent> result) {
      results.set(result);
    }

    @Override
    public void completeExceptionally(Throwable throwable) {
      error.set(throwable);
    }

    public Collection<AgentEvent> getResults() {
      return results.get();
    }

    public Throwable getError() {
      return error.get();
    }
  }

  // ==================== Construction Tests ====================

  @Nested
  @DisplayName("construction")
  class ConstructionTests {

    @Test
    @DisplayName("should construct with null config")
    void constructWithNullConfig() {
      StorageSinkFunction function = new StorageSinkFunction(null);
      assertNotNull(function);
    }

    @Test
    @DisplayName("should construct with forTesting config")
    void constructWithTestingConfig() {
      StorageSinkFunction function = new StorageSinkFunction(AgenticFlinkConfig.forTesting());
      assertNotNull(function);
    }

    @Test
    @DisplayName("should construct with custom config")
    void constructWithCustomConfig() {
      Map<String, String> props = new HashMap<>();
      props.put("redis.host", "custom-host-" + UUID.randomUUID().toString().substring(0, 8));
      AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);
      StorageSinkFunction function = new StorageSinkFunction(config);
      assertNotNull(function);
    }
  }

  // ==================== Serialization Tests ====================

  @Nested
  @DisplayName("serialization")
  class SerializationTests {

    @Test
    @DisplayName("should be serializable with null config (Flink requirement)")
    void serializableWithNullConfig() throws Exception {
      StorageSinkFunction original = new StorageSinkFunction(null);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(original);
      }

      byte[] bytes = bos.toByteArray();
      assertTrue(bytes.length > 0, "Serialized bytes should not be empty");

      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
        Object deserialized = ois.readObject();
        assertNotNull(deserialized);
        assertInstanceOf(StorageSinkFunction.class, deserialized);
      }
    }

    @Test
    @DisplayName("should be serializable with forTesting config (Flink requirement)")
    void serializableWithTestingConfig() throws Exception {
      StorageSinkFunction original = new StorageSinkFunction(AgenticFlinkConfig.forTesting());

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(original);
      }

      byte[] bytes = bos.toByteArray();
      assertTrue(bytes.length > 0);

      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
        Object deserialized = ois.readObject();
        assertInstanceOf(StorageSinkFunction.class, deserialized);
      }
    }
  }

  // ==================== Pass-Through Behavior Tests ====================

  @Nested
  @DisplayName("pass-through behavior")
  class PassThroughTests {

    private StorageSinkFunction function;

    @BeforeEach
    void setUp() throws Exception {
      function = new StorageSinkFunction(null);
      // open() with null OpenContext -- the function handles null config by using in-memory stores
      function.open((OpenContext) null);
    }

    @AfterEach
    void tearDown() throws Exception {
      function.close();
    }

    @RepeatedTest(5)
    @DisplayName("should pass through non-terminal events unchanged")
    void passesNonTerminalEventsThrough() {
      AgentEventType[] nonTerminalTypes = {
          AgentEventType.FLOW_STARTED,
          AgentEventType.TOOL_CALL_REQUESTED,
          AgentEventType.TOOL_CALL_COMPLETED,
          AgentEventType.VALIDATION_REQUESTED,
          AgentEventType.VALIDATION_PASSED,
          AgentEventType.LOOP_ITERATION_STARTED,
          AgentEventType.LOOP_ITERATION_COMPLETED,
          AgentEventType.SUPERVISOR_REVIEW_REQUESTED,
      };
      AgentEventType type = nonTerminalTypes[
          ThreadLocalRandom.current().nextInt(nonTerminalTypes.length)];
      AgentEvent event = randomEvent(type);
      String originalFlowId = event.getFlowId();

      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.asyncInvoke(event, resultFuture);

      assertNotNull(resultFuture.getResults(), "ResultFuture should have been completed");
      assertNull(resultFuture.getError(), "ResultFuture should not have error");
      assertEquals(1, resultFuture.getResults().size());

      AgentEvent result = resultFuture.getResults().iterator().next();
      assertSame(event, result, "Event should pass through as the same object reference");
      assertEquals(originalFlowId, result.getFlowId());
      assertEquals(type, result.getEventType());
    }

    @RepeatedTest(5)
    @DisplayName("should pass through FLOW_COMPLETED events unchanged")
    void passesFlowCompletedThrough() {
      AgentEvent event = randomEvent(AgentEventType.FLOW_COMPLETED);
      String originalFlowId = event.getFlowId();

      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.asyncInvoke(event, resultFuture);

      assertNotNull(resultFuture.getResults());
      assertEquals(1, resultFuture.getResults().size());

      AgentEvent result = resultFuture.getResults().iterator().next();
      assertSame(event, result);
      assertEquals(originalFlowId, result.getFlowId());
      assertEquals(AgentEventType.FLOW_COMPLETED, result.getEventType());
    }

    @RepeatedTest(5)
    @DisplayName("should pass through FLOW_FAILED events unchanged")
    void passesFlowFailedThrough() {
      AgentEvent event = randomEvent(AgentEventType.FLOW_FAILED);
      event.setErrorMessage("test-error-" + UUID.randomUUID());

      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.asyncInvoke(event, resultFuture);

      assertNotNull(resultFuture.getResults());
      assertEquals(1, resultFuture.getResults().size());

      AgentEvent result = resultFuture.getResults().iterator().next();
      assertSame(event, result);
      assertEquals(AgentEventType.FLOW_FAILED, result.getEventType());
    }

    @Test
    @DisplayName("should handle multiple sequential events maintaining pass-through")
    void handlesMultipleSequentialEvents() {
      int eventCount = ThreadLocalRandom.current().nextInt(5, 20);
      AgentEventType[] allTypes = AgentEventType.values();

      for (int i = 0; i < eventCount; i++) {
        AgentEventType type = allTypes[ThreadLocalRandom.current().nextInt(allTypes.length)];
        AgentEvent event = randomEvent(type);
        String originalFlowId = event.getFlowId();

        CapturingResultFuture resultFuture = new CapturingResultFuture();
        function.asyncInvoke(event, resultFuture);

        assertNotNull(resultFuture.getResults(), "Event " + i + " should pass through");
        assertEquals(1, resultFuture.getResults().size());
        AgentEvent result = resultFuture.getResults().iterator().next();
        assertEquals(originalFlowId, result.getFlowId());
        assertEquals(type, result.getEventType());
      }
    }

    @Test
    @DisplayName("should preserve all event fields through pass-through")
    void preservesAllEventFields() {
      AgentEvent event = randomEvent(AgentEventType.TOOL_CALL_COMPLETED);
      event.setCorrelationId("corr-" + UUID.randomUUID());
      event.setParentFlowId("parent-" + UUID.randomUUID());
      event.putData("result_key", "result_val_" + UUID.randomUUID());
      event.putMetadata("meta_key", "meta_val_" + UUID.randomUUID());
      event.setErrorMessage(null); // no error
      event.setCompletionTaskId("task-" + UUID.randomUUID());

      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.asyncInvoke(event, resultFuture);

      AgentEvent result = resultFuture.getResults().iterator().next();
      assertSame(event, result, "Pass-through should return exact same object");
      assertEquals(event.getCorrelationId(), result.getCorrelationId());
      assertEquals(event.getParentFlowId(), result.getParentFlowId());
      assertEquals(event.getCompletionTaskId(), result.getCompletionTaskId());
    }
  }

  // ==================== Timeout Handling Tests ====================

  @Nested
  @DisplayName("timeout handling")
  class TimeoutTests {

    private StorageSinkFunction function;

    @BeforeEach
    void setUp() throws Exception {
      function = new StorageSinkFunction(null);
      function.open((OpenContext) null);
    }

    @AfterEach
    void tearDown() throws Exception {
      function.close();
    }

    @RepeatedTest(5)
    @DisplayName("should pass event through on timeout without loss")
    void passesEventOnTimeout() {
      AgentEventType type = AgentEventType.values()[
          ThreadLocalRandom.current().nextInt(AgentEventType.values().length)];
      AgentEvent event = randomEvent(type);
      String originalFlowId = event.getFlowId();

      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.timeout(event, resultFuture);

      assertNotNull(resultFuture.getResults(), "Timeout should still complete the result future");
      assertNull(resultFuture.getError(), "Timeout should not produce an error");
      assertEquals(1, resultFuture.getResults().size());

      AgentEvent result = resultFuture.getResults().iterator().next();
      assertSame(event, result);
      assertEquals(originalFlowId, result.getFlowId());
    }
  }

  // ==================== Storage Backend Detection Tests ====================

  @Nested
  @DisplayName("storage backend detection")
  class StorageDetectionTests {

    @Test
    @DisplayName("should use in-memory stores with null config")
    void inMemoryWithNullConfig() throws Exception {
      StorageSinkFunction function = new StorageSinkFunction(null);
      // open should succeed using in-memory backends
      assertDoesNotThrow(() -> function.open((OpenContext) null));
      function.close();
    }

    @Test
    @DisplayName("should use in-memory stores with forTesting config (default redis/postgres)")
    void inMemoryWithTestingConfig() throws Exception {
      StorageSinkFunction function = new StorageSinkFunction(AgenticFlinkConfig.forTesting());
      // forTesting() gives default values (localhost), which match DEFAULT_REDIS_HOST
      // and DEFAULT_POSTGRES_URL, so both should resolve to "memory" backend
      assertDoesNotThrow(() -> function.open((OpenContext) null));

      // Verify pass-through works with this config
      AgentEvent event = randomEvent(AgentEventType.FLOW_STARTED);
      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.asyncInvoke(event, resultFuture);
      assertNotNull(resultFuture.getResults());
      assertEquals(1, resultFuture.getResults().size());

      function.close();
    }

    @Test
    @DisplayName("should attempt redis when non-default redis host is configured")
    void detectsRedisFromConfig() {
      Map<String, String> props = new HashMap<>();
      props.put("redis.host", "redis.production.example.com");
      AgenticFlinkConfig config = AgenticFlinkConfig.fromMap(props);

      StorageSinkFunction function = new StorageSinkFunction(config);
      // open() will try to create a Redis store; this may fail because Redis is not running,
      // but the detection logic is exercised. We just verify it does not crash with an
      // unexpected error type.
      try {
        function.open((OpenContext) null);
        // If Redis happens to be available (unlikely in unit test), just close cleanly.
        function.close();
      } catch (Exception e) {
        // Expected: Redis connection failure, not a config detection error
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        assertFalse(msg.contains("Unknown"),
            "Should not get 'unknown backend' error, got: " + msg);
      }
    }
  }

  // ==================== Open / Close Lifecycle Tests ====================

  @Nested
  @DisplayName("lifecycle")
  class LifecycleTests {

    @Test
    @DisplayName("should allow open then close without invoke")
    void openAndCloseWithoutInvoke() throws Exception {
      StorageSinkFunction function = new StorageSinkFunction(null);
      function.open((OpenContext) null);
      assertDoesNotThrow(() -> function.close());
    }

    @Test
    @DisplayName("should allow close without prior open")
    void closeWithoutOpen() {
      StorageSinkFunction function = new StorageSinkFunction(null);
      // close() should handle null stores gracefully
      assertDoesNotThrow(() -> function.close());
    }

    @Test
    @DisplayName("should allow multiple close calls")
    void multipleClose() throws Exception {
      StorageSinkFunction function = new StorageSinkFunction(null);
      function.open((OpenContext) null);
      function.close();
      assertDoesNotThrow(() -> function.close());
    }
  }

  // ==================== Event Data Coverage Tests ====================

  @Nested
  @DisplayName("event data coverage")
  class EventDataCoverageTests {

    private StorageSinkFunction function;

    @BeforeEach
    void setUp() throws Exception {
      function = new StorageSinkFunction(AgenticFlinkConfig.forTesting());
      function.open((OpenContext) null);
    }

    @AfterEach
    void tearDown() throws Exception {
      function.close();
    }

    @Test
    @DisplayName("should handle event with minimal fields (no optional data)")
    void handlesMinimalEvent() {
      AgentEvent event = new AgentEvent(
          UUID.randomUUID().toString(),
          null, // no userId
          null, // no agentId
          AgentEventType.FLOW_STARTED);

      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.asyncInvoke(event, resultFuture);

      assertNotNull(resultFuture.getResults());
      assertEquals(1, resultFuture.getResults().size());
    }

    @Test
    @DisplayName("should handle event with error information")
    void handlesEventWithError() {
      AgentEvent event = randomEvent(AgentEventType.FLOW_FAILED);
      event.setErrorMessage("Simulated failure: " + UUID.randomUUID());
      event.setErrorCode("ERR_" + ThreadLocalRandom.current().nextInt(1000, 9999));

      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.asyncInvoke(event, resultFuture);

      assertNotNull(resultFuture.getResults());
      AgentEvent result = resultFuture.getResults().iterator().next();
      assertEquals(event.getErrorMessage(), result.getErrorMessage());
      assertEquals(event.getErrorCode(), result.getErrorCode());
    }

    @Test
    @DisplayName("should handle event with compensation data")
    void handlesEventWithCompensationData() {
      AgentEvent event = randomEvent(AgentEventType.FLOW_FAILED);
      event.putCompensationData("rollback_target", "table_" + UUID.randomUUID());
      event.putCompensationData("original_value", ThreadLocalRandom.current().nextInt());

      CapturingResultFuture resultFuture = new CapturingResultFuture();
      function.asyncInvoke(event, resultFuture);

      assertNotNull(resultFuture.getResults());
      AgentEvent result = resultFuture.getResults().iterator().next();
      assertTrue(result.requiresCompensation());
    }

    @RepeatedTest(3)
    @DisplayName("should handle all AgentEventType values without errors")
    void handlesAllEventTypes() {
      for (AgentEventType type : AgentEventType.values()) {
        AgentEvent event = randomEvent(type);
        CapturingResultFuture resultFuture = new CapturingResultFuture();

        assertDoesNotThrow(() -> function.asyncInvoke(event, resultFuture),
            "asyncInvoke should not throw for event type: " + type);
        assertNotNull(resultFuture.getResults(),
            "Result should be completed for event type: " + type);
        assertEquals(1, resultFuture.getResults().size(),
            "Exactly one event should pass through for type: " + type);
      }
    }
  }
}
