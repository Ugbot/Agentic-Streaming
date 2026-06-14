package org.jagentic.core;

/** Lifecycle hooks the RoutedGraph fires per turn. All are {@code default} no-ops, so a
 * listener implements only what it cares about. Portable analogue of the Flink
 * {@code AgentEventListener}. */
public interface AgentListener {
  default void onTurnStart(Event event, AgentContext ctx) {}

  default void onRouted(String path, AgentContext ctx) {}

  default void onToolCallStart(String toolId, AgentContext ctx) {}

  default void onToolCallEnd(String toolId, Object result, AgentContext ctx) {}

  default void onGuardrailBlock(String reason, AgentContext ctx) {}

  default void onError(String stage, Throwable error, AgentContext ctx) {}

  default void onTurnEnd(TurnResult result, AgentContext ctx) {}
}
