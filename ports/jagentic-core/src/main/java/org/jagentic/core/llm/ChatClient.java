package org.jagentic.core.llm;

import java.util.List;
import java.util.Map;

/**
 * Turns (system prompt + transcript + available tools) into a {@link ChatResult} — the
 * portable analogue of the Flink {@code ChatConnection}. {@code messages} are
 * {@code {role, content}} maps; {@code tools} are {@code {name, description}} maps.
 */
@FunctionalInterface
public interface ChatClient {
  ChatResult chat(List<Map<String, String>> messages, List<Map<String, String>> tools);
}
