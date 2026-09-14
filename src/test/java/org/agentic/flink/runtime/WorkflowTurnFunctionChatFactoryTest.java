package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.conformance.FlinkConformanceHarness;
import org.agentic.flink.runtime.testkit.Workflows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.junit.jupiter.api.Test;

/** F4: the ChatClientFactory is injected and serializable; llm brains fail at job build without one. */
class WorkflowTurnFunctionChatFactoryTest {

  @SuppressWarnings("unchecked")
  private static Map<String, Object> llmScriptedWorkflow() {
    Map<String, Object> fixture = FlinkConformanceHarness.load(
        FlinkConformanceHarness.fixturesDir().resolve("16-llm-brain-scripted.yaml"));
    return (Map<String, Object>) fixture.get("workflow");
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> harness(WorkflowTurnFunction fn)
      throws Exception {
    KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h = new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(fn), Event::conversationId, Types.STRING);
    h.setup(TurnResultTypeInfo.INSTANCE.createSerializer(h.getExecutionConfig().getSerializerConfig()));
    return h;
  }

  private static List<TurnResult> outputs(KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h) {
    List<TurnResult> out = new ArrayList<>();
    for (Object o : h.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((TurnResult) r.getValue());
      }
    }
    return out;
  }

  private static Object roundTrip(Object o) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(o);
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      return ois.readObject();
    }
  }

  @Test
  void llmBrainWithoutFactoryFailsAtConstructionNamingThePaths() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new WorkflowTurnFunction(llmScriptedWorkflow()));
    assertTrue(e.getMessage().contains("payments"), e.getMessage());
    assertTrue(e.getMessage().contains("ChatClientFactory"), e.getMessage());
  }

  @Test
  void ruleBrainWorkflowStillRunsWithTheDefaultFactory() throws Exception {
    String cid = "c-" + UUID.randomUUID();
    try (var h = harness(new WorkflowTurnFunction(Workflows.billing()))) {
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, "t-" + UUID.randomUUID(), "u", "what is my balance?")));
      List<TurnResult> out = outputs(h);
      assertEquals(1, out.size());
      assertEquals(TurnStatus.COMPLETED, out.get(0).status);
    }
  }

  @Test
  void llmBrainRunsWithStubFactoryAndFactorySurvivesSerialization() throws Exception {
    Map<String, Object> wf = llmScriptedWorkflow();
    WorkflowTurnFunction fn = (WorkflowTurnFunction) roundTrip(
        new WorkflowTurnFunction(wf, FlinkRuntimeOptions.fromSpec(wf), ChatClientFactories.stub()));
    assertSame(ChatClientFactories.stub(), roundTrip(ChatClientFactories.stub()));
    assertSame(ChatClientFactories.failFast(), roundTrip(ChatClientFactories.failFast()));

    String cid = "c-" + UUID.randomUUID();
    try (var h = harness(fn)) {
      h.open();
      h.processElement(new StreamRecord<>(Event.turn(cid, "t1", "u", "what is my balance?")));
      h.processElement(new StreamRecord<>(Event.turn(cid, "t2", "u", "hello there")));
      List<TurnResult> out = outputs(h);
      assertEquals(2, out.size());
      TurnResult llm = out.get(0);
      assertEquals(TurnStatus.COMPLETED, llm.status);
      assertTrue(llm.reply().endsWith("Your balance is 1234.56 USD."), llm.reply());
      assertTrue(llm.events.stream().anyMatch(ev -> "tool_called".equals(ev.type())));
      TurnResult rule = out.get(1);
      assertEquals(TurnStatus.COMPLETED, rule.status);
      assertTrue(rule.reply().startsWith("[general] "), rule.reply());
    }
  }
}
