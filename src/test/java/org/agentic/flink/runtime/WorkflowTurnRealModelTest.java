package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.agentic.flink.runtime.testkit.Workflows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.InstantiationUtil;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.Test;

/**
 * A workflow document that declares a real provider runs on Flink: the factory ships with the
 * operator, the client is built in {@code open()}, and the call goes over HTTP. The provider here
 * is a local server speaking Ollama's wire format, so this proves the path, not a model.
 */
class WorkflowTurnRealModelTest {

  private static Map<String, Object> workflow(Map<String, Object> llm) {
    Map<String, Object> wf = new LinkedHashMap<>(Workflows.billing());
    @SuppressWarnings("unchecked")
    Map<String, Object> agent = new LinkedHashMap<>((Map<String, Object>) wf.get("agent"));
    agent.put(
        "paths", Map.of("general", Map.of("brain", "llm"), "billing", Map.of("brain", "llm")));
    wf.put("agent", agent);
    wf.put("llm", llm);
    return wf;
  }

  private static List<TurnResult> outputs(
      KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h) {
    List<TurnResult> out = new ArrayList<>();
    for (Object o : h.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((TurnResult) r.getValue());
      }
    }
    return out;
  }

  @Test
  void aDeclaredProviderRunsThroughTheShippedFactory() throws Exception {
    List<String> bodies = new CopyOnWriteArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/api/chat",
        e -> {
          bodies.add(new String(e.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] body =
              "{\"message\":{\"content\":\"{\\\"text\\\":\\\"it is 42\\\"}\"}}"
                  .getBytes(StandardCharsets.UTF_8);
          e.sendResponseHeaders(200, body.length);
          e.getResponseBody().write(body);
          e.close();
        });
    server.start();
    try {
      Map<String, Object> wf =
          workflow(
              Map.of(
                  "provider",
                  "ollama",
                  "model",
                  "some-model",
                  "temperature",
                  0.25,
                  "base_url",
                  "http://127.0.0.1:" + server.getAddress().getPort()));
      WorkflowTurnFunction fn =
          new WorkflowTurnFunction(
              wf, FlinkRuntimeOptions.fromSpec(wf), ChatClientFactories.fromWorkflow());
      WorkflowTurnFunction shipped = InstantiationUtil.clone(fn, getClass().getClassLoader());
      try (var h =
          new KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult>(
              new KeyedProcessOperator<>(shipped), Event::conversationId, Types.STRING)) {
        h.setup(
            TurnResultTypeInfo.INSTANCE.createSerializer(
                h.getExecutionConfig().getSerializerConfig()));
        h.open();
        h.processElement(
            new StreamRecord<>(
                Event.turn("c-" + UUID.randomUUID(), "t1", "u", "what is the answer?"), 1));
        TurnResult r = outputs(h).get(0);
        assertEquals(TurnStatus.COMPLETED, r.status, String.valueOf(r.error));
        assertTrue(r.reply.contains("it is 42"), r.reply);
      }
      assertEquals(1, bodies.size());
      assertTrue(bodies.get(0).contains("some-model"), bodies.get(0));
      assertTrue(
          bodies.get(0).contains("0.25"), "a declared temperature is sent: " + bodies.get(0));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void anUnknownProviderIsRefusedWhenTheJobIsBuilt() {
    Map<String, Object> wf = workflow(Map.of("provider", "no-such-provider"));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new WorkflowTurnFunction(
                    wf, FlinkRuntimeOptions.fromSpec(wf), ChatClientFactories.fromWorkflow()));
    assertTrue(e.getMessage().contains("no-such-provider"), e.getMessage());
  }

  @Test
  void aKeyInTheDocumentIsRefusedAndNotEchoed() {
    String secret = "sk-" + UUID.randomUUID();
    Map<String, Object> wf =
        workflow(Map.of("provider", "openai", "model", "m", "x-api-key", secret));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new WorkflowTurnFunction(
                    wf, FlinkRuntimeOptions.fromSpec(wf), ChatClientFactories.fromWorkflow()));
    assertTrue(e.getMessage().contains("x-api-key"), e.getMessage());
    assertFalse(e.getMessage().contains(secret), "the value must not reach a log");
  }

  @Test
  void theDefaultFactoryDoesNotEchoTheLlmBlock() {
    String marker = "marker-" + UUID.randomUUID();
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> ChatClientFactories.failFast().create(Map.of("provider", "x", "x-note", marker)));
    assertFalse(e.getMessage().contains(marker), e.getMessage());
  }
}
