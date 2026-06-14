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

import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.InMemoryLongTermStore;
import org.jagentic.core.LongTermStore;
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
    /** Durable long-term store (conversation resumption + per-user fact archive). */
    public final LongTermStore longTerm;
    /** Per-conversation transcript + attributes store wired into the backend runtime. */
    public final ConversationStore conversation;

    PipelineSystem(String backendName, Runtime runtime, GraphBuilder.Built built,
                   LongTermStore longTerm, ConversationStore conversation) {
      this.backendName = backendName;
      this.runtime = runtime;
      this.built = built;
      this.longTerm = longTerm;
      this.conversation = conversation;
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

  @SuppressWarnings("unchecked")
  public static PipelineSystem buildSystem(Map<String, Object> spec, String backendOverride) {
    GraphBuilder.Built built = GraphBuilder.build(spec, PipelineLoader::chatClient);
    String backend = backendOverride != null ? backendOverride : (String) spec.getOrDefault("backend", "local");

    Map<String, Object> stores = (Map<String, Object>) spec.getOrDefault("stores", Map.of());
    LongTermStore longTerm = buildLongTermStore((Map<String, Object>) stores.get("long_term"));
    ConversationStore conversation = buildConversationStore((Map<String, Object>) stores.get("conversation"));

    Runtime runtime = Backends.create(backend, built, conversation);
    return new PipelineSystem(backend, runtime, built, longTerm, conversation);
  }

  /** Build the long-term store by kind: memory (default) | postgres. A postgres failure
   * (no reachable server) degrades to an in-memory store rather than failing the build. */
  static LongTermStore buildLongTermStore(Map<String, Object> spec) {
    if (spec == null) {
      return new InMemoryLongTermStore();
    }
    String kind = String.valueOf(spec.getOrDefault("kind", "memory")).toLowerCase();
    switch (kind) {
      case "memory":
        return new InMemoryLongTermStore();
      case "postgres":
        try {
          return new org.jagentic.core.store.PostgresLongTermStore(
              GraphBuilder.resolveEnv(String.valueOf(spec.getOrDefault("url", spec.get("jdbc_url")))),
              GraphBuilder.resolveEnv(String.valueOf(spec.getOrDefault("user", "postgres"))),
              GraphBuilder.resolveEnv(String.valueOf(spec.getOrDefault("password", ""))),
              (String) spec.get("schema"));
        } catch (RuntimeException e) {
          return new InMemoryLongTermStore();
        }
      default:
        throw new IllegalArgumentException("unknown long_term store kind " + kind + "; choose memory|postgres");
    }
  }

  /** Build the conversation store by kind: memory (default) | redis. A redis failure
   * degrades to in-memory rather than failing the build. */
  static ConversationStore buildConversationStore(Map<String, Object> spec) {
    if (spec == null) {
      return new ConversationStore.InMemory();
    }
    String kind = String.valueOf(spec.getOrDefault("kind", "memory")).toLowerCase();
    switch (kind) {
      case "memory":
        return new ConversationStore.InMemory();
      case "redis":
        try {
          String url = GraphBuilder.resolveEnv(String.valueOf(spec.getOrDefault("url", "redis://localhost:6379")));
          int max = ((Number) spec.getOrDefault("max_messages", 200)).intValue();
          return new org.jagentic.core.store.RedisConversationStore(url, max);
        } catch (RuntimeException e) {
          return new ConversationStore.InMemory();
        }
      default:
        throw new IllegalArgumentException("unknown conversation store kind " + kind + "; choose memory|redis");
    }
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
