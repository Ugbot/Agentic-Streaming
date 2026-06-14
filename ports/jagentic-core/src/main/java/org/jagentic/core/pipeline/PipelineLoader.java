package org.jagentic.core.pipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.TurnResult;
import org.jagentic.core.llm.ChatClient;
import org.jagentic.core.llm.ChatResult;
import org.jagentic.core.llm.OllamaChatClient;
import org.jagentic.core.llm.OpenAIChatClient;
import org.jagentic.core.llm.StubChatClient;

/**
 * Loads a {@code pipeline.yaml} (the same schema as the Python/Go loaders), builds the
 * agentic system with {@link GraphBuilder}, and wires it onto the chosen backend.
 */
public final class PipelineLoader {

  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  private PipelineLoader() {}

  /** A built, deployed system: a backend {@link Runtime} + the spec that produced it. */
  public static final class PipelineSystem {
    public final String backendName;
    public final Runtime runtime;
    public final GraphBuilder.Built built;

    PipelineSystem(String backendName, Runtime runtime, GraphBuilder.Built built) {
      this.backendName = backendName;
      this.runtime = runtime;
      this.built = built;
    }

    public TurnResult submit(Event event) {
      return runtime.submit(event);
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> loadYaml(Path path) {
    try {
      return YAML.readValue(Files.readString(path), Map.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static PipelineSystem buildSystem(Map<String, Object> spec, String backendOverride) {
    GraphBuilder.Built built = GraphBuilder.build(spec, PipelineLoader::chatClient);
    String backend = backendOverride != null ? backendOverride : (String) spec.getOrDefault("backend", "local");
    return new PipelineSystem(backend, Backends.create(backend, built), built);
  }

  public static PipelineSystem load(Path path, String backendOverride) {
    return buildSystem(loadYaml(path), backendOverride);
  }

  /** Build a ChatClient from the {@code llm:} section (ollama/openai/stub). */
  @SuppressWarnings("unchecked")
  static ChatClient chatClient(Map<String, Object> llm) {
    String provider = (String) (llm == null ? "ollama" : llm.getOrDefault("provider", "ollama"));
    switch (provider) {
      case "ollama":
        return new OllamaChatClient((String) llm.getOrDefault("model", "qwen2.5:3b"));
      case "openai":
        return new OpenAIChatClient((String) llm.getOrDefault("model", "gpt-5.4-mini"));
      case "stub":
        List<ChatResult> script = new ArrayList<>();
        for (Map<String, Object> step : (List<Map<String, Object>>) llm.getOrDefault("script", List.of())) {
          if (step.get("tool") != null) {
            script.add(ChatResult.toolCall((String) step.get("tool"),
                (Map<String, Object>) step.getOrDefault("args", Map.of())));
          } else {
            script.add(ChatResult.text((String) step.getOrDefault("text", "ok")));
          }
        }
        if (script.isEmpty()) {
          script.add(ChatResult.text("ok"));
        }
        return new StubChatClient(script);
      default:
        throw new IllegalArgumentException("unknown llm provider " + provider);
    }
  }
}
