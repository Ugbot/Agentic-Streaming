package org.agentic.flink.memory.vector;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Binding a Flink-state vector memory from an unkeyed operator fails with a message naming the fix.
 */
final class FlinkStateVectorMemoryUnkeyedBindTest {

  static Stream<Arguments> specs() {
    int dim = 4 + ThreadLocalRandom.current().nextInt(60);
    return Stream.of(
        Arguments.of("brute-force", FlinkStateVectorMemory.spec(dim)),
        Arguments.of("hnsw", FlinkStateHnswVectorMemory.spec(dim)));
  }

  @ParameterizedTest(name = "[{0}] unkeyed open() reports the keyed-state requirement")
  @MethodSource("specs")
  void unkeyedBindFailsExplicitly(String flavour, VectorMemorySpec spec) throws Exception {
    try (OneInputStreamOperatorTestHarness<String, String> harness =
        new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new BindInOpen(spec)))) {
      Exception e = assertThrows(Exception.class, harness::open);
      Throwable cause = e;
      while (cause != null && !(cause instanceof IllegalStateException)) {
        cause = cause.getCause();
      }
      assertInstanceOf(IllegalStateException.class, cause, () -> "unexpected failure: " + e);
      assertTrue(cause.getMessage().contains("keyed operator"), cause.getMessage());
      assertTrue(cause.getMessage().contains("RetrievalPipeline"), cause.getMessage());
      assertTrue(cause.getMessage().contains(spec.providerName()), cause.getMessage());
    }
  }

  static final class BindInOpen extends ProcessFunction<String, String> {
    private static final long serialVersionUID = 1L;
    private final VectorMemorySpec spec;

    BindInOpen(VectorMemorySpec spec) {
      this.spec = spec;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      spec.bind(getRuntimeContext());
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
      out.collect(value);
    }
  }
}
