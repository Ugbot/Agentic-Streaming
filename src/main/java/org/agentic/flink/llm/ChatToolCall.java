package org.agentic.flink.llm;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * A tool invocation requested by the model, in vendor-neutral form.
 *
 * <p>The runtime resolves {@link #getName()} against the agent's {@code ToolRegistry} and feeds
 * the resulting {@code ContextItem} back as a {@link ChatRole#TOOL} message.
 */
public final class ChatToolCall implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String id;
  private final String name;
  private final Map<String, Object> arguments;

  public ChatToolCall(String id, String name, Map<String, Object> arguments) {
    this.id = id;
    this.name = name;
    this.arguments = arguments == null ? Collections.emptyMap() : Map.copyOf(arguments);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getArguments() {
    return arguments;
  }

  @Override
  public String toString() {
    return "ChatToolCall[id=" + id + ", name=" + name + ", args=" + arguments + "]";
  }
}
