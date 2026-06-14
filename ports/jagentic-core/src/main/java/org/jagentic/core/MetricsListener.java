package org.jagentic.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Counts turns, per-path dispatches, blocked turns, and tool calls. */
public final class MetricsListener implements AgentListener {
  public final AtomicLong turns = new AtomicLong();
  public final AtomicLong blocked = new AtomicLong();
  public final AtomicLong toolCalls = new AtomicLong();
  public final Map<String, AtomicInteger> paths = new ConcurrentHashMap<>();

  @Override
  public void onTurnStart(Event event, AgentContext ctx) {
    turns.incrementAndGet();
  }

  @Override
  public void onRouted(String path, AgentContext ctx) {
    paths.computeIfAbsent(path, k -> new AtomicInteger()).incrementAndGet();
  }

  @Override
  public void onTurnEnd(TurnResult result, AgentContext ctx) {
    if (!result.ok) {
      blocked.incrementAndGet();
    }
    toolCalls.addAndGet(result.toolCalls.size());
  }

  public int pathCount(String path) {
    AtomicInteger c = paths.get(path);
    return c == null ? 0 : c.get();
  }
}
