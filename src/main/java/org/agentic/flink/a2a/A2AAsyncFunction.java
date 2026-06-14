package org.agentic.flink.a2a;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Non-blocking, <b>stateless</b> A2A delegation operator: calls a remote peer off the operator
 * thread via Flink Async I/O ({@link RichAsyncFunction}), so a slow peer never stalls the pipeline.
 *
 * <p>Flink's Async-I/O operator cannot access keyed state (on 1.x or 2.x — 2.x's async story is async
 * <em>state access</em> via the disaggregated backend, not async external I/O), so this function
 * holds no state. The remote {@code contextId} for conversation continuity rides on the event under
 * {@link A2AStepSupport#contextIdKey(A2AStep)}: a keyed pre-step stamps it on, this operator forwards
 * it and writes the peer's returned contextId back, and a keyed post-step persists it (see {@link
 * A2AStep#applyToStateful}). For fire-and-enrich steps with no cross-turn continuity, {@link
 * A2AStep#applyToAsync} uses this operator alone.
 *
 * <p>The blocking {@code sendAndAwait}/{@code stream} call runs on a bounded daemon pool built in
 * {@link #open(OpenContext)} — never on the async callback thread. The client is the resilient
 * decorator (retry + backoff + circuit breaker) from the step's factory. {@link #timeout} emits the
 * event annotated with a timeout error (or a {@code FLOW_FAILED} event when the step is
 * fail-on-error) instead of hanging.
 */
public final class A2AAsyncFunction extends RichAsyncFunction<AgentEvent, AgentEvent> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(A2AAsyncFunction.class);

  /** Concurrent in-flight remote calls per subtask; matches the operator's async capacity. */
  private final int poolSize;
  private final A2AStep step;

  private transient A2AClient client;
  private transient ExecutorService pool;

  public A2AAsyncFunction(A2AStep step, int poolSize) {
    this.step = java.util.Objects.requireNonNull(step, "step");
    this.poolSize = Math.max(1, poolSize);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    client = step.clientFactory().create(step.spec());
    ThreadPoolExecutor tp =
        new ThreadPoolExecutor(
            1,
            poolSize,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            r -> {
              Thread t = new Thread(r, "a2a-async-" + step.name());
              t.setDaemon(true);
              return t;
            });
    tp.allowCoreThreadTimeOut(true);
    pool = tp;
  }

  @Override
  public void asyncInvoke(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    // Operate on a copy: the input may be reused/recycled by the runtime once asyncInvoke returns.
    final AgentEvent event = input.withEventType(input.getEventType());
    final String text = A2AStepSupport.resolveInput(step, event);
    final String contextId =
        event.getData() == null ? null : asString(event.getData().get(A2AStepSupport.contextIdKey(step)));

    CompletableFuture.supplyAsync(
            () -> {
              A2ATask task = A2AStepSupport.call(client, step, text, contextId);
              A2AStepSupport.applyResult(step, event, task);
              boolean failed = task.getState() != A2ATaskState.COMPLETED && step.failOnError();
              if (failed) {
                return emitFailureEvent(
                    event, "A2A step '" + step.name() + "' ended in state " + task.getState().wire());
              }
              return event;
            },
            pool)
        .whenComplete(
            (result, err) -> {
              if (err != null) {
                Throwable cause = err instanceof java.util.concurrent.CompletionException ? err.getCause() : err;
                LOG.warn("A2A async step '{}' failed: {}", step.name(), cause.getMessage());
                if (step.failOnError()) {
                  resultFuture.complete(
                      Collections.singleton(emitFailureEvent(event, cause.getMessage())));
                } else {
                  event.putData(step.outputKey() + ".error", cause.getMessage());
                  resultFuture.complete(Collections.singleton(event));
                }
              } else {
                resultFuture.complete(Collections.singleton(result));
              }
            });
  }

  @Override
  public void timeout(AgentEvent input, ResultFuture<AgentEvent> resultFuture) {
    LOG.warn("A2A async step '{}' timed out", step.name());
    AgentEvent event = input.withEventType(input.getEventType());
    String msg = "A2A step '" + step.name() + "' timed out";
    if (step.failOnError()) {
      resultFuture.complete(Collections.singleton(emitFailureEvent(event, msg)));
    } else {
      event.putData(step.outputKey() + ".error", msg);
      event.putData(step.outputKey() + ".state", "timeout");
      resultFuture.complete(Collections.singleton(event));
    }
  }

  @Override
  public void close() throws Exception {
    if (pool != null) {
      pool.shutdownNow();
    }
    if (client != null) {
      client.close();
    }
    super.close();
  }

  private AgentEvent emitFailureEvent(AgentEvent event, String error) {
    AgentEvent failed = event.withEventType(AgentEventType.FLOW_FAILED);
    failed.setErrorMessage(error);
    return failed;
  }

  private static String asString(Object o) {
    return o == null ? null : o.toString();
  }
}
