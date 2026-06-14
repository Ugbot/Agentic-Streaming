package org.jagentic.core;

/** Lifecycle hooks the RoutedGraph fires per turn: start → routed → end. Portable
 * analogue of the Flink {@code AgentEventListener}. */
public interface AgentListener {
  default void onTurnStart(Event event, AgentContext ctx) {}

  default void onRouted(String path, AgentContext ctx) {}

  default void onTurnEnd(TurnResult result, AgentContext ctx) {}
}
