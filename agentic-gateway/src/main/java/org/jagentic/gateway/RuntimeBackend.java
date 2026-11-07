package org.jagentic.gateway;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.Event;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;

/** The Java core's {@link LocalRuntime} in this process. */
public final class RuntimeBackend implements TurnBackend {

  private final LocalRuntime runtime;
  private final ConversationLog log;

  public RuntimeBackend(LocalRuntime runtime) {
    this.runtime = runtime;
    this.log = runtime.log();
  }

  @Override
  public CompletionStage<TurnResult> submit(Event event) {
    return CompletableFuture.supplyAsync(() -> runtime.submit(event));
  }

  @Override
  public CompletionStage<List<LogEvent>> conversation(String key) {
    return CompletableFuture.completedFuture(log.events(key));
  }
}
