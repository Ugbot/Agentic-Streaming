package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.conformance.Crash;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The double payment, on a real operator with a real HTTP server as the tool: the checkpoint is
 * taken before the {@code pay} turn, the job dies, Flink restores, and the source redelivers the
 * turn. The log forgets the turn; the outside world does not. Crash points are before the call,
 * during it (the server has answered and nothing is recorded yet) and after it.
 */
class PayAfterRestoreTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  private HttpServer server;
  final List<String> keys = Collections.synchronizedList(new ArrayList<>());
  private final Crash.Probe probe = Crash.newProbe();

  @BeforeEach
  void startTheBank() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/pay",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          keys.add(String.valueOf(exchange.getRequestHeaders().getFirst("Idempotency-Key")));
          byte[] body = "{\"paid\":true}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void stopTheBank() {
    server.stop(0);
    probe.disarm();
  }

  @Test
  void aCompletedPaymentIsNotRepeatedAfterRestore() throws Exception {
    TurnResult redelivered = crashAndRedeliver("idempotent", "after");
    assertEquals(TurnStatus.COMPLETED, redelivered.status);
    assertEquals(1, keys.size(), "the bank saw " + keys);
  }

  @Test
  void aCrashBeforeTheCallPaysOnceOnRedelivery() throws Exception {
    TurnResult redelivered = crashAndRedeliver("idempotent", "before");
    assertEquals(TurnStatus.COMPLETED, redelivered.status);
    assertEquals(1, keys.size(), "the bank saw " + keys);
  }

  @Test
  void aCrashDuringAnIdempotentCallRepeatsItUnderTheSameKey() throws Exception {
    TurnResult redelivered = crashAndRedeliver("idempotent", "during");
    assertEquals(TurnStatus.COMPLETED, redelivered.status);
    assertEquals(2, keys.size(), "at-least-once: the call was made again: " + keys);
    assertEquals(1, keys.stream().distinct().count(), "one key, so the bank can dedupe: " + keys);
    assertFalse(keys.get(0).isBlank() || "null".equals(keys.get(0)), "a real key: " + keys);
  }

  @Test
  void aCrashDuringANonIdempotentCallIsNeverRepeated() throws Exception {
    TurnResult redelivered = crashAndRedeliver("non_idempotent", "during");
    assertEquals(
        TurnStatus.SUSPENDED, redelivered.status, "the outcome is unknown: " + redelivered);
    assertEquals(1, keys.size(), "the bank saw " + keys);
  }

  @Test
  void aCrashBeforeANonIdempotentCallLooksTheSameAndIsNotCalled() throws Exception {
    TurnResult redelivered = crashAndRedeliver("non_idempotent", "before");
    assertEquals(
        TurnStatus.SUSPENDED, redelivered.status, "the outcome is unknown: " + redelivered);
    assertEquals(0, keys.size(), "the bank saw " + keys);
  }

  @Test
  void aRecordedNonIdempotentResultIsReused() throws Exception {
    TurnResult redelivered = crashAndRedeliver("non_idempotent", "after");
    assertEquals(TurnStatus.COMPLETED, redelivered.status);
    assertEquals(1, keys.size(), "the bank saw " + keys);
  }

  /** {@code when} is a {@code crash_at} call point, as in the conformance fixtures. */
  private TurnResult crashAndRedeliver(String effect, String when) throws Exception {
    String journal =
        "{\"kind\": \"memory\", \"name\": \"%s\", \"x-process-local\": true}"
            .formatted(UUID.randomUUID());
    return crashAndRedeliver(effect, when, journal);
  }

  /**
   * @param journal the {@code stores.effects} block, as JSON
   */
  TurnResult crashAndRedeliver(String effect, String when, String journal) throws Exception {
    Map<String, Object> workflow = workflow(effect, journal);
    OperatorSubtaskState checkpoint;
    Event pay = Event.turn("c", "t1", "u", "pay");
    try (var job = job(workflow)) {
      job.open();
      job.processElement(new StreamRecord<>(Event.turn("c", "t0", "u", "hello"), 1));
      checkpoint = job.snapshot(1, 1);
      probe.arm(Map.of("call", "pay", "when", when));
      assertThrows(
          Crash.SimulatedCrash.class, () -> job.processElement(new StreamRecord<>(pay, 2)));
      probe.disarm();
    }
    try (var job = job(workflow)) {
      job.initializeState(checkpoint);
      job.open();
      job.processElement(new StreamRecord<>(pay, 2));
      List<TurnResult> results = results(job);
      TurnResult last = results.get(results.size() - 1);
      var surfaced = job.getSideOutput(WorkflowTurnFunction.TURN_FAILURES);
      assertEquals(
          last.status == TurnStatus.SUSPENDED,
          surfaced != null && !surfaced.isEmpty(),
          "a turn waiting for a person reaches the operator's stream: " + last);
      return last;
    }
  }

  private KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> job(
      Map<String, Object> workflow) throws Exception {
    FlinkRuntimeOptions options = FlinkRuntimeOptions.fromSpec(workflow).withProbe(probe);
    var job =
        new KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult>(
            new KeyedProcessOperator<>(new WorkflowTurnFunction(workflow, options)),
            Event::conversationId,
            Types.STRING);
    job.setup(
        TurnResultTypeInfo.INSTANCE.createSerializer(
            job.getExecutionConfig().getSerializerConfig()));
    return job;
  }

  private static List<TurnResult> results(
      KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> job) {
    List<TurnResult> out = new ArrayList<>();
    for (Object record : job.getOutput()) {
      if (record instanceof StreamRecord<?> r && r.getValue() instanceof TurnResult result) {
        out.add(result);
      }
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> workflow(String effect, String journal) throws Exception {
    return JSON.readValue(
        """
        {"spec_version": "agentic/v1",
         "agent": {"id": "a", "router": {"kind": "keyword", "default": "p", "rules": {}},
                   "paths": {"p": {"brain": "rule", "tool_triggers": {"pay": "pay"}}}},
         "tools": [{"id": "pay", "kind": "http", "effect": "%s",
                    "url": "http://127.0.0.1:%d/pay"}],
         "stores": {"effects": %s}}
        """
            .formatted(effect, server.getAddress().getPort(), journal),
        Map.class);
  }
}
