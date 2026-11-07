package org.agentic.flink.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.agentic.flink.runtime.testkit.Workflows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.jagentic.core.Event;
import org.jagentic.core.Principal;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.privacy.FileKeyVault;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A deletion request honoured on the primary runtime, over state restored from a snapshot. */
class WorkflowTurnSealedTest {

  private static final String KEY = "agentic.key_vault_dir";

  @TempDir Path vaultDir;

  @BeforeEach
  void vault() {
    System.setProperty(KEY, vaultDir.toString());
  }

  @AfterEach
  void noVault() {
    System.clearProperty(KEY);
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> harness()
      throws Exception {
    KeyedOneInputStreamOperatorTestHarness<String, Event, TurnResult> h =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new WorkflowTurnFunction(Workflows.billing())),
            Event::conversationId,
            Types.STRING);
    h.setup(
        TurnResultTypeInfo.INSTANCE.createSerializer(h.getExecutionConfig().getSerializerConfig()));
    return h;
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
  void anErasedOwnerReadsAsErasedAfterARestore() throws Exception {
    String cid = "c-" + UUID.randomUUID();
    String tid = "t-" + UUID.randomUUID();
    String words = "my balance question " + UUID.randomUUID();
    Principal alice = Principal.of("alice", "acme");
    OperatorSubtaskState snapshot;
    try (var h = harness()) {
      h.open();
      h.processElement(
          new StreamRecord<>(Event.turn(cid, tid, "", "balance " + words).withPrincipal(alice), 1));
      TurnResult first = outputs(h).get(0);
      assertEquals(TurnStatus.COMPLETED, first.status);
      assertTrue(first.events.toString().contains(words), "this turn reads its own events");
      snapshot = h.snapshot(1L, 2L);
    }
    assertEquals(1, new FileKeyVault(vaultDir).erase("acme/alice"));

    try (var h = harness()) {
      h.setup();
      h.initializeState(snapshot);
      h.open();
      h.processElement(
          new StreamRecord<>(Event.turn(cid, tid, "", "balance " + words).withPrincipal(alice), 3));
      TurnResult again = outputs(h).get(0);
      assertEquals(TurnStatus.DUPLICATE, again.status);
      assertEquals("[erased]", again.reply);
      assertFalse(again.toString().contains(words));
    }
  }
}
