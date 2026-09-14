package org.agentic.flink.memory.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.agentic.flink.context.core.ContextItem;
import org.agentic.flink.context.core.ContextPriority;
import org.agentic.flink.context.core.MemoryType;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

/** F3: per-key HNSW graphs are rebuilt lazily for the active key, never in the unkeyed open(). */
class FlinkStateHnswVectorMemoryKeyedTest {

  private static final int DIM = 8;

  /** Input: (key, op, payload). Output: "key:op:result". */
  static final class Op extends KeyedProcessFunction<String, Tuple3<String, String, float[]>, String> {
    private static final long serialVersionUID = 1L;
    private final VectorMemorySpec spec;
    private transient FlinkStateHnswVectorMemory memory;

    Op(VectorMemorySpec spec) {
      this.spec = spec;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      memory = (FlinkStateHnswVectorMemory) spec.bind(getRuntimeContext());
    }

    @Override
    public void processElement(Tuple3<String, String, float[]> in, Context ctx, Collector<String> out)
        throws Exception {
      switch (in.f1) {
        case "put" -> {
          memory.put(in.f0 + "-" + UUID.randomUUID(), in.f2,
              new ContextItem(in.f0, ContextPriority.SHOULD, MemoryType.SHORT_TERM));
          out.collect(in.f0 + ":put:" + memory.size());
        }
        case "search" -> {
          List<ScoredItem> hits = memory.search(in.f2, 10);
          StringBuilder sb = new StringBuilder(in.f0).append(":search:").append(hits.size());
          for (ScoredItem hit : hits) {
            sb.append(':').append(hit.getItem().getContent());
          }
          out.collect(sb.toString());
        }
        case "rebuilds" -> out.collect(in.f0 + ":rebuilds:" + memory.rebuildCount());
        default -> throw new IllegalArgumentException(in.f1);
      }
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Tuple3<String, String, float[]>, String> harness()
      throws Exception {
    KeyedOneInputStreamOperatorTestHarness<String, Tuple3<String, String, float[]>, String> h =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new Op(FlinkStateHnswVectorMemory.spec(DIM))),
            t -> t.f0, Types.STRING);
    return h;
  }

  private static float[] vec() {
    float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      v[i] = ThreadLocalRandom.current().nextFloat() - 0.5f;
    }
    return v;
  }

  private static List<String> drain(
      KeyedOneInputStreamOperatorTestHarness<String, Tuple3<String, String, float[]>, String> h) {
    List<String> out = new ArrayList<>();
    for (Object o : h.getOutput()) {
      if (o instanceof StreamRecord<?> r) {
        out.add((String) r.getValue());
      }
    }
    h.getOutput().clear();
    return out;
  }

  private static void send(
      KeyedOneInputStreamOperatorTestHarness<String, Tuple3<String, String, float[]>, String> h,
      String key, String op, float[] v) throws Exception {
    h.processElement(new StreamRecord<>(Tuple3.of(key, op, v), 0L));
  }

  @Test
  void twoKeysStayIsolatedAcrossSnapshotAndRestore() throws Exception {
    String a = "conv-" + UUID.randomUUID();
    String b = "conv-" + UUID.randomUUID();
    int na = ThreadLocalRandom.current().nextInt(2, 6);
    int nb = ThreadLocalRandom.current().nextInt(2, 6);

    OperatorSubtaskState snapshot;
    try (var h = harness()) {
      h.open();
      send(h, a, "rebuilds", new float[0]);
      assertEquals(List.of(a + ":rebuilds:0"), drain(h), "nothing rebuilt in the unkeyed open()");

      for (int i = 0; i < na; i++) send(h, a, "put", vec());
      for (int i = 0; i < nb; i++) send(h, b, "put", vec());
      List<String> puts = drain(h);
      assertEquals(a + ":put:" + na, puts.get(na - 1));
      assertEquals(b + ":put:" + nb, puts.get(na + nb - 1), "key B does not see key A's vectors");
      send(h, a, "rebuilds", new float[0]);
      assertEquals(List.of(a + ":rebuilds:2"), drain(h), "one lazy rebuild per key touched");
      snapshot = h.snapshot(1L, 1L);
    }

    try (var h = harness()) {
      h.initializeState(snapshot);
      h.open();
      send(h, a, "search", vec());
      send(h, b, "search", vec());
      List<String> searches = drain(h);
      String[] ra = searches.get(0).split(":");
      String[] rb = searches.get(1).split(":");
      assertEquals(na, Integer.parseInt(ra[2]), "key A restored with only its own vectors");
      assertEquals(nb, Integer.parseInt(rb[2]), "key B restored with only its own vectors");
      for (int i = 3; i < ra.length; i++) assertEquals(a, ra[i]);
      for (int i = 3; i < rb.length; i++) assertEquals(b, rb[i]);

      send(h, a, "rebuilds", new float[0]);
      assertTrue(drain(h).get(0).endsWith(":rebuilds:2"), "one rebuild per touched key after restore");
    }
  }
}
