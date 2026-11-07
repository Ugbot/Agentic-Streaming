package org.jagentic.core;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.jagentic.core.embedding.Embedder;
import org.jagentic.core.llm.ChatClient;

/**
 * Time limits on anything that leaves the process. The turn still blocks the calling thread; what
 * this adds is that it blocks for a known time. The call runs on a worker so that a limit holds
 * whatever the call is doing, including a stalled response body, which {@code HttpClient}'s own
 * request timeout does not cover. The worker is interrupted at the limit, and the effect may still
 * land on the far side: a timeout says the answer did not arrive, not that nothing happened.
 */
public final class Deadlines {

  /** {@code tools[].timeout_ms} when a document declares none. */
  public static final long DEFAULT_TOOL_TIMEOUT_MS = 10_000;
  /** {@code a2a[].timeout_ms} when a document declares none. */
  public static final long DEFAULT_PEER_TIMEOUT_MS = 30_000;
  /** {@code llm.timeout_ms} when a document declares none. */
  public static final long DEFAULT_MODEL_TIMEOUT_MS = 60_000;

  private static final ExecutorService WORKERS = Executors.newCachedThreadPool(task -> {
    Thread t = new Thread(task, "agentic-bounded-call");
    t.setDaemon(true);
    return t;
  });

  private Deadlines() {}

  /** The limit was reached. {@code message} names what timed out and for how long. */
  public static final class DeadlineExceeded extends OutcomeUnknown {
    private static final long serialVersionUID = 1L;

    public DeadlineExceeded(String message) {
      super(message);
    }
  }

  /** The {@code timeout_ms} of a spec section, or {@code fallback} when it declares none. */
  public static long timeoutOf(Map<String, Object> section, long fallback) {
    return section != null && section.get("timeout_ms") instanceof Number n ? n.longValue() : fallback;
  }

  /** {@code fn} limited to {@code timeoutMs} per call. */
  public static Function<Map<String, Object>, Object> bound(
      String what, long timeoutMs, Function<Map<String, Object>, Object> fn) {
    return bound(what, timeoutMs, fn, () -> { });
  }

  /** As above; {@code onTimeout} runs at the limit, for a call an interrupt cannot reach. */
  public static Function<Map<String, Object>, Object> bound(
      String what, long timeoutMs, Function<Map<String, Object>, Object> fn, Runnable onTimeout) {
    return params -> call(what, timeoutMs, () -> fn.apply(params), onTimeout);
  }

  /** {@code fn} limited to {@code timeoutMs} per call, given the call's identity. */
  public static java.util.function.BiFunction<Map<String, Object>, ToolRegistry.CallIdentity, Object> boundKeyed(
      String what, long timeoutMs,
      java.util.function.BiFunction<Map<String, Object>, ToolRegistry.CallIdentity, Object> fn) {
    return boundKeyed(what, timeoutMs, fn, () -> { });
  }

  /** As above; {@code onTimeout} runs at the limit, for a call an interrupt cannot reach. */
  public static java.util.function.BiFunction<Map<String, Object>, ToolRegistry.CallIdentity, Object> boundKeyed(
      String what, long timeoutMs,
      java.util.function.BiFunction<Map<String, Object>, ToolRegistry.CallIdentity, Object> fn,
      Runnable onTimeout) {
    return (params, identity) -> call(what, timeoutMs, () -> fn.apply(params, identity), onTimeout);
  }

  /** {@code client} limited to {@code timeoutMs} per model call; a timeout is transient. */
  public static ChatClient bound(String what, long timeoutMs, ChatClient client) {
    return (messages, tools) -> modelCall(what, timeoutMs, () -> client.chat(messages, tools));
  }

  /** {@code embedder} limited to {@code timeoutMs} per call; a timeout is transient, as for a chat model. */
  public static Embedder bound(String what, long timeoutMs, Embedder embedder) {
    return new Embedder() {
      @Override
      public float[] embed(String text) {
        return modelCall(what, timeoutMs, () -> embedder.embed(text));
      }

      @Override
      public int dim() {
        return modelCall(what, timeoutMs, embedder::dim); // a provider may ask its model
      }
    };
  }

  private static <T> T modelCall(String what, long timeoutMs, Callable<T> body) {
    try {
      return call(what, timeoutMs, body, () -> { });
    } catch (DeadlineExceeded e) {
      throw new TransientFailure(e.getMessage(), e);
    }
  }

  private static <T> T call(String what, long timeoutMs, Callable<T> body, Runnable onTimeout) {
    Future<T> running = WORKERS.submit(body);
    try {
      return running.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      running.cancel(true);
      onTimeout.run();
      throw new DeadlineExceeded(what + " timed out after " + timeoutMs + " ms");
    } catch (InterruptedException e) {
      running.cancel(true);
      Thread.currentThread().interrupt();
      throw new DeadlineExceeded(what + " was interrupted");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException r) {
        throw r;
      }
      if (e.getCause() instanceof Error err) {
        throw err;
      }
      throw new RuntimeException(e.getCause());
    }
  }
}
