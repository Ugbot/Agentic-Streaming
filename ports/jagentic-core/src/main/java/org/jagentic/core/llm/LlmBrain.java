package org.jagentic.core.llm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jagentic.core.AgentContext;
import org.jagentic.core.Brain;
import org.jagentic.core.ChatMessage;
import org.jagentic.core.ContextItem;
import org.jagentic.core.ContextWindowManager;
import org.jagentic.core.Priority;

/**
 * A {@link Brain} that drives a bounded ReAct loop over a {@link ChatClient} (thought →
 * tool → observation → final) — the portable analogue of the Flink
 * {@code ReActProcessFunction}. The model-free {@code Banking.RuleBrain} stays the
 * default; {@code LlmBrain} is opt-in so the full LLM+tools workflow runs on any backend.
 */
public final class LlmBrain implements Brain {

  static final String REACT_SYSTEM =
      "You are a tool-using agent. On each step reply with ONE JSON object and nothing "
          + "else. To call a tool: {\"tool\": \"<name>\", \"args\": {<json args>}}. To give "
          + "the final answer: {\"text\": \"<answer>\"}. Available tools: ";

  private final ChatClient client;
  private final String name;
  private final String systemPrompt;
  private final Set<String> allowedTools; // null => all
  private final int maxIterations;
  private Map<String, Object> outputSchema; // optional structured-output contract
  private ContextWindowManager contextManager; // optional transcript compaction (MoSCoW)

  public LlmBrain(ChatClient client, String name) {
    this(client, name, "", null, 6);
  }

  public LlmBrain(ChatClient client, String name, String systemPrompt,
                  Collection<String> tools, int maxIterations) {
    this.client = client;
    this.name = name;
    this.systemPrompt = systemPrompt == null ? "" : systemPrompt;
    this.allowedTools = tools == null ? null : Set.copyOf(tools);
    this.maxIterations = Math.max(1, maxIterations);
  }

  /** Enforce a structured (JSON-schema-lite) final answer; returns this for chaining. */
  public LlmBrain withOutputSchema(Map<String, Object> schema) {
    this.outputSchema = schema;
    return this;
  }

  /** Compact the replayed transcript to a token budget (MoSCoW); returns this for chaining. */
  public LlmBrain withContextManager(ContextWindowManager contextManager) {
    this.contextManager = contextManager;
    return this;
  }

  @Override
  public String turn(String userText, AgentContext ctx) {
    List<Map<String, String>> specs = ctx.tools.specs().stream()
        .filter(s -> allowedTools == null || allowedTools.contains(s.get("name")))
        .toList();
    String toolList = specs.stream()
        .map(s -> s.get("name") + ": " + s.get("description"))
        .collect(Collectors.joining(", "));

    String sys = (systemPrompt + "\n" + REACT_SYSTEM + toolList).strip();
    if (outputSchema != null) {
      sys += "\n" + org.jagentic.core.structured.Structured.schemaInstruction(outputSchema);
    }
    List<Map<String, String>> messages = new ArrayList<>();
    messages.add(Map.of("role", "system", "content", sys));
    // The agent already appended the user turn; replay the persisted transcript, compacting
    // it to the token budget (recency MoSCoW) if a ContextWindowManager is set.
    List<ChatMessage> transcript = compact(ctx.store.history(ctx.conversationId));
    for (ChatMessage m : transcript) {
      messages.add(Map.of("role", m.role(), "content", m.content() == null ? "" : m.content()));
    }
    if (messages.isEmpty() || !userText.equals(messages.get(messages.size() - 1).get("content"))) {
      messages.add(Map.of("role", "user", "content", userText));
    }

    for (int i = 0; i < maxIterations; i++) {
      ChatResult r = client.chat(messages, specs);
      if (r.isToolCall()) {
        Object observation = ctx.callTool(r.tool(), r.args());
        messages.add(Map.of("role", "assistant", "content", "{\"tool\":\"" + r.tool() + "\"}"));
        messages.add(Map.of("role", "tool", "content", String.valueOf(observation)));
        continue;
      }
      return "[" + name + "] " + finalize(r.text(), ctx);
    }
    return "[" + name + "] (stopped after " + maxIterations + " steps)";
  }

  /**
   * Bound the replayed transcript with the {@link ContextWindowManager} if configured.
   * The most-recent turns are MUST-keep, the next four SHOULD, the rest COULD — so the
   * compactor drops from the tail of history first when over budget. Mirrors pyagentic's
   * {@code LlmBrain._compact}.
   */
  private List<ChatMessage> compact(List<ChatMessage> transcript) {
    if (contextManager == null || transcript.isEmpty()) {
      return transcript;
    }
    int n = transcript.size();
    List<ContextItem> items = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      ChatMessage m = transcript.get(i);
      int age = n - 1 - i; // 0 = most recent
      Priority prio = age < 2 ? Priority.MUST : (age < 6 ? Priority.SHOULD : Priority.COULD);
      items.add(new ContextItem(itemText(m), prio));
    }
    Set<String> keptTexts = contextManager.compact(items).stream()
        .map(ContextItem::text).collect(Collectors.toSet());
    List<ChatMessage> kept = new ArrayList<>();
    for (ChatMessage m : transcript) {
      if (keptTexts.contains(itemText(m))) {
        kept.add(m);
      }
    }
    return kept;
  }

  private static String itemText(ChatMessage m) {
    return m.role() + ": " + (m.content() == null ? "" : m.content());
  }

  private String finalize(String text, AgentContext ctx) {
    if (text == null) {
      return "(no answer)";
    }
    if (outputSchema != null) {
      var result = org.jagentic.core.structured.Structured.parse(text, outputSchema);
      if (!result.ok()) {
        for (org.jagentic.core.AgentListener l : ctx.listeners) {
          l.onError("output_schema", new IllegalArgumentException(String.join("; ", result.errors())), ctx);
        }
      } else if (result.value() != null) {
        try {
          return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(result.value());
        } catch (Exception ignore) {
          // fall through to raw text
        }
      }
    }
    return text;
  }
}
