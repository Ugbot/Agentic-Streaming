package org.jagentic.pekko.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.pipeline.Backends;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.core.pipeline.PipelineLoader;

/** Proves {@code backend: pekko} resolves through the core's ServiceLoader fallback (our
 * {@link PekkoBackendProvider}) and that the shared banking.yaml routes identically on the Pekko
 * actor runtime as it would on LocalRuntime. */
class PekkoBackendPipelineTest {

  // examples/pipelines/banking.yaml, relative to the agentic-pekko module dir
  private static final Path BANKING = Path.of("..", "examples", "pipelines", "banking.yaml");

  @Test
  void backendPekkoRunsTheSharedBankingYaml() {
    if (!Files.exists(BANKING)) {
      org.junit.jupiter.api.Assumptions.abort("banking.yaml not found at " + BANKING.toAbsolutePath());
    }
    PipelineLoader.PipelineSystem sys = PipelineLoader.load(BANKING, "pekko");
    try {
      assertEquals("pekko", sys.backendName);
      assertTrue(sys.runtime instanceof PekkoRuntime, "expected a PekkoRuntime via the SPI");
      assertEquals("payments", sys.submit(new Event("c1", "u", "what is my balance?")).path);
      assertEquals("cards", sys.submit(new Event("c2", "u", "tell me about crypto cash-back")).path);
      assertEquals("general", sys.submit(new Event("c3", "u", "hello there")).path);
    } finally {
      close(sys.runtime);
    }
  }

  @Test
  void backendsCreateResolvesPekkoViaServiceLoader() {
    GraphBuilder.Built built = GraphBuilder.build(Map.of(
        "agent", Map.of(
            "router", Map.of("default", "general", "rules", Map.of("payments", java.util.List.of("balance"))),
            "paths", Map.of(
                "payments", Map.of("brain", "rule", "tool_triggers", Map.of("balance", "get_balance")),
                "general", Map.of("brain", "rule"))),
        "tools", java.util.List.of(Map.of("id", "get_balance", "kind", "constant", "value", "1234.56"))),
        null);
    Runtime rt = Backends.create("pekko", built);
    try {
      assertTrue(rt instanceof PekkoRuntime);
      assertEquals("payments", rt.submit(new Event("c1", "u", "what is my balance?")).path);
    } finally {
      close(rt);
    }
  }

  private static void close(Runtime rt) {
    if (rt instanceof AutoCloseable c) {
      try {
        c.close();
      } catch (Exception ignore) {
        // best-effort
      }
    }
  }
}
