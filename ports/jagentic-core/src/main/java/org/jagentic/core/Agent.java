package org.jagentic.core;

/** A named agent = id + system prompt + a brain. Stateless; all state lives in the
 * ConversationStore/KeyedStateStore reached through the context. */
public final class Agent {
  public final String agentId;
  public final String systemPrompt;
  public final Brain brain;

  public Agent(String agentId, String systemPrompt, Brain brain) {
    this.agentId = agentId;
    this.systemPrompt = systemPrompt;
    this.brain = brain;
  }

  public TurnResult turn(Event event, AgentContext ctx) {
    ctx.store.associateUser(event.conversationId(), event.userId());
    ctx.store.append(event.conversationId(), ChatMessage.user(event.text()));
    String reply = brain.turn(event.text(), ctx);
    ctx.store.append(event.conversationId(), ChatMessage.assistant(reply));
    return new TurnResult(event.conversationId(), reply, ctx.toolCalls);
  }
}
