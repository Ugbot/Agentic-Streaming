package org.agentic.flink.python;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Round-trips a cloudpickled Python callable through PEMJA. Tagged {@code integration} because it
 * requires the optional PEMJA classpath entry plus a working {@code python3} with cloudpickle
 * installed; the default {@code mvn test} run excludes it.
 */
@Tag("integration")
class PythonExecutorTest {

  @Test
  void invokesCloudpickledLambda() {
    assumeTrue(PythonExecutor.isAvailable(), "PEMJA not on classpath; skipping");
    try (PythonExecutor px = new PythonExecutor()) {
      px.open();
      // Build the cloudpickle inside the embedded interpreter to avoid needing python3 outside.
      px.exec("import cloudpickle, base64");
      px.exec("_fn = lambda x: str(x).upper()");
      px.exec("_b64 = base64.b64encode(cloudpickle.dumps(_fn)).decode('ascii')");
      Object b64 = px.get("_b64");
      assertTrue(b64 instanceof String);
      Object out = px.invokeOnce((String) b64, List.of("hello"));
      assertEquals("HELLO", out);
    }
  }

  @Test
  void availabilityProbeIsCheap() {
    // Should never throw and should be cheap enough to call freely.
    boolean a = PythonExecutor.isAvailable();
    boolean b = PythonExecutor.isAvailable();
    assertEquals(a, b);
  }

  @Test
  void openThrowsWhenPemjaMissing() {
    assumeTrue(!PythonExecutor.isAvailable(), "PEMJA present; this case only fires without it");
    PythonExecutor px = new PythonExecutor();
    assertThrows(IllegalStateException.class, px::open);
  }
}
