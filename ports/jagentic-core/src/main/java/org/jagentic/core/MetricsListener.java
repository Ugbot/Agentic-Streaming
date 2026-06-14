package org.jagentic.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Counts turns, per-path dispatches, guardrail blocks, tool calls, and errors via the
 * lifecycle hooks. */
public final class MetricsListener implements AgentListener {
  public final AtomicLong turns = new AtomicLong();
  public final AtomicLong blocked = new AtomicLong();
  public final AtomicLong toolCalls = new AtomicLong();
  public final AtomicLong errors = new AtomicLong();
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
  public void onToolCallEnd(String toolId, Object result, AgentContext ctx) {
    toolCalls.incrementAndGet();
  }

  @Override
  public void onGuardrailBlock(String reason, AgentContext ctx) {
    blocked.incrementAndGet();
  }

  @Override
  public void onError(String stage, Throwable error, AgentContext ctx) {
    errors.incrementAndGet();
  }

  public int pathCount(String path) {
    AtomicInteger c = paths.get(path);
    return c == null ? 0 : c.get();
  }
}
