package org.jagentic.core;

import java.util.function.Consumer;

/** Logs each lifecycle event (stand-in for the reference LoggingAgentEventListener). */
public final class LoggingListener implements AgentListener {
  private final Consumer<String> sink;

  public LoggingListener() {
    this(System.out::println);
  }

  public LoggingListener(Consumer<String> sink) {
    this.sink = sink;
  }

  @Override
  public void onTurnStart(Event event, AgentContext ctx) {
    sink.accept("[turn-start] conv=" + event.conversationId() + " text=" + event.text());
  }

  @Override
  public void onRouted(String path, AgentContext ctx) {
    sink.accept("[routed] conv=" + ctx.conversationId + " path=" + path);
  }

  @Override
  public void onTurnEnd(TurnResult result, AgentContext ctx) {
    sink.accept("[turn-end] conv=" + result.conversationId + " path=" + result.path
        + " ok=" + result.ok + " tools=" + result.toolCalls);
  }
}
