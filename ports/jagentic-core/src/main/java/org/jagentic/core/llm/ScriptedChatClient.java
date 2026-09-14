package org.jagentic.core.llm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The spec's deterministic {@code llm.provider: stub}: a {@link ChatClient} that answers from
 * {@code llm.script}, replayed from the top on every turn (spec/v1/primitives.md, section 8).
 *
 * <p>The client is stateless. Like a real model it reads its position from the transcript
 * {@link LlmBrain} hands it: the number of {@code tool} observations appended after the last
 * {@code user} message is the number of script steps already taken this turn, so the next call
 * returns the step after them. A new turn starts with a fresh user message and therefore restarts
 * the script. Running past the end of the script (a script with no {@code text} step) is a
 * {@link ScriptExhausted} error rather than an invented answer.
 */
public final class ScriptedChatClient implements ChatClient {

  /** The stub provider was asked for a step the script does not have. */
  public static final class ScriptExhausted extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    ScriptExhausted(int steps) {
      super("llm.script ended after " + steps + " step(s) without a final text");
    }
  }

  public static final String PROVIDER = "stub";

  private final List<ChatResult> script;

  public ScriptedChatClient(List<ChatResult> script) {
    if (script == null || script.isEmpty()) {
      throw new IllegalArgumentException("llm.script must have at least one step for provider stub");
    }
    this.script = List.copyOf(script);
  }

  /** True when {@code llmSpec} selects this provider ({@code provider: stub}, the spec default). */
  public static boolean accepts(Map<String, Object> llmSpec) {
    return llmSpec != null && PROVIDER.equals(String.valueOf(llmSpec.getOrDefault("provider", PROVIDER)));
  }

  /**
   * Build the client from an {@code llm:} block. Each {@code {tool, args}} step becomes a structured
   * tool call carrying exactly those arguments (insertion order preserved); each {@code {text}} step
   * a final answer.
   */
  @SuppressWarnings("unchecked")
  public static ScriptedChatClient fromSpec(Map<String, Object> llmSpec) {
    if (!accepts(llmSpec)) {
      throw new IllegalArgumentException("ScriptedChatClient only implements llm.provider: stub, got "
          + (llmSpec == null ? null : llmSpec.get("provider")));
    }
    Object raw = llmSpec.get("script");
    if (!(raw instanceof List<?> steps) || steps.isEmpty()) {
      throw new IllegalArgumentException("llm.provider: stub requires a non-empty llm.script");
    }
    List<ChatResult> script = new ArrayList<>(steps.size());
    for (int i = 0; i < steps.size(); i++) {
      if (!(steps.get(i) instanceof Map<?, ?> step)) {
        throw new IllegalArgumentException("llm.script[" + i + "] must be a mapping");
      }
      Object tool = step.get("tool");
      if (tool != null) {
        Object args = step.get("args");
        Map<String, Object> copy = new LinkedHashMap<>();
        if (args instanceof Map<?, ?> m) {
          for (Map.Entry<?, ?> e : m.entrySet()) {
            copy.put(String.valueOf(e.getKey()), e.getValue());
          }
        } else if (args != null) {
          throw new IllegalArgumentException("llm.script[" + i + "].args must be a mapping");
        }
        script.add(ChatResult.toolCall(String.valueOf(tool), copy));
      } else if (step.containsKey("text")) {
        script.add(ChatResult.text(String.valueOf(step.get("text"))));
      } else {
        throw new IllegalArgumentException("llm.script[" + i + "] needs either tool or text");
      }
    }
    return new ScriptedChatClient(script);
  }

  /** The scripted steps, in order. */
  public List<ChatResult> script() {
    return script;
  }

  @Override
  public ChatResult chat(List<Map<String, String>> messages, List<Map<String, String>> tools) {
    int step = stepsTakenThisTurn(messages);
    if (step >= script.size()) {
      throw new ScriptExhausted(script.size());
    }
    return script.get(step);
  }

  /** Tool observations after the most recent user message: one per script step already taken. */
  static int stepsTakenThisTurn(List<Map<String, String>> messages) {
    int taken = 0;
    for (int i = messages.size() - 1; i >= 0; i--) {
      String role = messages.get(i).get("role");
      if ("user".equals(role)) {
        break;
      }
      if ("tool".equals(role)) {
        taken++;
      }
    }
    return taken;
  }
}
