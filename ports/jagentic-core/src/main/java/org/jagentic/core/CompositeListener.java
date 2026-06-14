package org.jagentic.core;

import java.util.List;

/** Fans every lifecycle hook out to several listeners. */
public final class CompositeListener implements AgentListener {
  private final List<AgentListener> listeners;

  public CompositeListener(AgentListener... listeners) {
    this.listeners = List.of(listeners);
  }

  @Override public void onTurnStart(Event e, AgentContext c) { for (AgentListener l : listeners) l.onTurnStart(e, c); }
  @Override public void onRouted(String p, AgentContext c) { for (AgentListener l : listeners) l.onRouted(p, c); }
  @Override public void onToolCallStart(String t, AgentContext c) { for (AgentListener l : listeners) l.onToolCallStart(t, c); }
  @Override public void onToolCallEnd(String t, Object r, AgentContext c) { for (AgentListener l : listeners) l.onToolCallEnd(t, r, c); }
  @Override public void onGuardrailBlock(String reason, AgentContext c) { for (AgentListener l : listeners) l.onGuardrailBlock(reason, c); }
  @Override public void onError(String stage, Throwable e, AgentContext c) { for (AgentListener l : listeners) l.onError(stage, e, c); }
  @Override public void onTurnEnd(TurnResult r, AgentContext c) { for (AgentListener l : listeners) l.onTurnEnd(r, c); }
}
