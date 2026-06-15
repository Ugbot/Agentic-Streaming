package org.jagentic.pekko;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;
import org.jagentic.core.pipeline.PipelineLoader;

/** Run ANY {@code pipeline.yaml} on the Pekko actor runtime — the declarative counterpart to
 * {@link Main} (which uses the compiled banking graph). The YAML's {@code backend:} is overridden
 * to {@code pekko}, so the jagentic-core {@link PipelineLoader} builds the graph and hosts each
 * conversation on an event-sourced, sharded entity via our {@code PekkoBackendProvider} (SPI).
 *
 * <pre>
 *   mvn -f agentic-pekko/pom.xml exec:java -Dexec.mainClass=org.jagentic.pekko.PipelineMain \
 *     -Dexec.args="../examples/pipelines/banking.yaml --text 'what is my balance?'"
 * </pre>
 *
 * With no args it runs the shared banking.yaml through a short multi-turn script. */
public final class PipelineMain {

  private PipelineMain() {}

  public static void main(String[] args) throws Exception {
    String yaml = "examples/pipelines/banking.yaml";
    String text = null;
    for (int i = 0; i < args.length; i++) {
      if ("--text".equals(args[i]) && i + 1 < args.length) {
        text = args[++i];
      } else if (!args[i].startsWith("--")) {
        yaml = args[i];
      }
    }

    Path path = resolve(yaml);
    // backend: pekko — resolved via the BackendProvider ServiceLoader; the entity owns its state.
    PipelineLoader.PipelineSystem sys = PipelineLoader.load(path, "pekko");
    try {
      System.out.printf("loaded %s on backend=%s%n", path, sys.backendName);
      List<Event> turns = text != null
          ? List.of(new Event("c1", "demo", text))
          : List.of(
              new Event("c1", "alice", "what card types do you offer?"),
              new Event("c2", "bob", "what is my balance?"),
              new Event("c1", "alice", "tell me about crypto cash-back"),
              new Event("c3", "carol", "hello there"));
      for (Event e : turns) {
        TurnResult r = sys.submit(e);
        System.out.printf("[%s] path=%-8s ok=%-5s reply=%s%n", e.conversationId(), r.path, r.ok, r.reply);
      }
    } finally {
      if (sys.runtime instanceof AutoCloseable c) {
        c.close();
      }
    }
  }

  /** Resolve a pipeline path whether {@code exec:java} runs from the repo root or the module dir:
   * try the path as given, then prefixed with {@code ../}, then with a leading {@code ../} stripped. */
  private static Path resolve(String yaml) {
    Path p = Path.of(yaml);
    if (Files.exists(p)) {
      return p;
    }
    Path parent = Path.of("..").resolve(yaml);
    if (Files.exists(parent)) {
      return parent;
    }
    if (yaml.startsWith("../")) {
      Path stripped = Path.of(yaml.substring(3));
      if (Files.exists(stripped)) {
        return stripped;
      }
    }
    return p; // let PipelineLoader surface a clear NoSuchFileException with the original path
  }
}
