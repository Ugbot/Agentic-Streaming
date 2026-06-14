package org.jagentic.core.pipeline;

import java.nio.file.Path;

import org.jagentic.core.Event;
import org.jagentic.core.TurnResult;

/**
 * CLI: build a pipeline.yaml on the chosen backend and run a turn through it.
 *
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass=org.jagentic.core.pipeline.PipelineCli \
 *       -Dexec.args="../../examples/pipelines/banking.yaml --text 'what is my balance?'"
 * </pre>
 */
public final class PipelineCli {

  private PipelineCli() {}

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("usage: PipelineCli <pipeline.yaml> [--backend x] [--text t] [--conv c] [--user u]");
      System.exit(2);
    }
    Path yaml = Path.of(args[0]);
    String backend = null, text = "what is my balance?", conv = "c1", user = "demo";
    for (int i = 1; i + 1 < args.length; i += 2) {
      switch (args[i]) {
        case "--backend" -> backend = args[i + 1];
        case "--text" -> text = args[i + 1];
        case "--conv" -> conv = args[i + 1];
        case "--user" -> user = args[i + 1];
        default -> { }
      }
    }
    PipelineLoader.PipelineSystem system = PipelineLoader.load(yaml, backend);
    TurnResult res = system.submit(new Event(conv, user, text));
    System.out.println("backend=" + system.backendName + " path=" + res.path + " ok=" + res.ok);
    System.out.println("reply: " + res.reply);
    if (!res.toolCalls.isEmpty()) {
      System.out.println("tools: " + res.toolCalls);
    }
  }
}
