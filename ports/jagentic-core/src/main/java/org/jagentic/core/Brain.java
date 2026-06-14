package org.jagentic.core;

/** Produces a reply for a turn. A real brain runs an LLM ReAct loop; demos/tests
 * use a deterministic rule brain so the port runs with no model. */
@FunctionalInterface
public interface Brain {
  String turn(String userText, AgentContext ctx);
}
