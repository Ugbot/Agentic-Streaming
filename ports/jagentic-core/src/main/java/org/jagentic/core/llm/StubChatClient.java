package org.jagentic.core.llm;

import java.util.List;
import java.util.Map;

/**
 * Deterministic, scripted {@link ChatClient} for offline tests: returns the next
 * {@link ChatResult} from the script each call (repeating the last once exhausted).
 */
public final class StubChatClient implements ChatClient {

  private final List<ChatResult> script;
  private int i = 0;

  public StubChatClient(List<ChatResult> script) {
    if (script == null || script.isEmpty()) {
      throw new IllegalArgumentException("StubChatClient needs at least one scripted ChatResult");
    }
    this.script = List.copyOf(script);
  }

  @Override
  public ChatResult chat(List<Map<String, String>> messages, List<Map<String, String>> tools) {
    ChatResult r = script.get(Math.min(i, script.size() - 1));
    i++;
    return r;
  }
}
