package org.jagentic.ports.pulsar;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.LoggerFactory;

/**
 * An in-memory stand-in for a Pulsar Functions {@link Context} — the state store +
 * current record, with no broker or BookKeeper. Used by {@link LocalDemo} and by the
 * port's tests to run {@link BankingFunction} end-to-end on a plain JVM, the way
 * Pekko's {@code LocalDemo} runs an actor without a cluster.
 *
 * <p>The {@code Context}/{@code Record} interfaces are realized as dynamic proxies so
 * we only implement the handful of methods the function actually uses
 * ({@code getState}/{@code putState}/{@code deleteState}, {@code getCurrentRecord},
 * {@code getLogger}); everything else returns a type-appropriate default, and any
 * {@code *Async} variant returns a completed future of the synchronous result.
 */
public final class InMemoryContext {

  private InMemoryContext() {}

  /** Builds a {@link Context} bound to {@code record}, backed by the shared {@code state} map. */
  public static Context create(Record<String> record, Map<String, byte[]> state) {
    InvocationHandler base = (proxy, method, args) -> {
      switch (method.getName()) {
        case "getState": {
          byte[] v = state.get((String) args[0]);
          return v == null ? null : ByteBuffer.wrap(v);
        }
        case "putState": {
          ByteBuffer buf = ((ByteBuffer) args[1]).duplicate();
          byte[] copy = new byte[buf.remaining()];
          buf.get(copy);
          state.put((String) args[0], copy);
          return null;
        }
        case "deleteState":
          state.remove((String) args[0]);
          return null;
        case "getCurrentRecord":
          return record;
        case "getLogger":
          return LoggerFactory.getLogger("agentic-pulsar-function");
        default:
          return defaultFor(method);
      }
    };
    return (Context) Proxy.newProxyInstance(
        Context.class.getClassLoader(), new Class<?>[] {Context.class}, asyncAware(base));
  }

  /** A {@link Record} proxy exposing key/value/properties for one turn. */
  @SuppressWarnings("unchecked")
  public static Record<String> record(String conversationId, String text, String userId) {
    InvocationHandler h = (proxy, method, args) -> {
      switch (method.getName()) {
        case "getKey":
          return Optional.of(conversationId);
        case "getValue":
          return text;
        case "getProperties":
          return Map.of("userId", userId);
        case "toString":
          return "Record(" + conversationId + ")";
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          return defaultFor(method);
      }
    };
    return (Record<String>) Proxy.newProxyInstance(
        Record.class.getClassLoader(), new Class<?>[] {Record.class}, h);
  }

  /** Wraps a handler so {@code *Async} methods return a completed future of the
   * synchronous result, and {@code Object} methods behave sanely. */
  private static InvocationHandler asyncAware(InvocationHandler inner) {
    return (proxy, method, args) -> {
      String name = method.getName();
      if (name.equals("toString")) {
        return "InMemoryContext";
      }
      if (name.equals("hashCode")) {
        return System.identityHashCode(proxy);
      }
      if (name.equals("equals")) {
        return proxy == args[0];
      }
      if (name.endsWith("Async") && CompletableFuture.class.isAssignableFrom(method.getReturnType())) {
        String sync = name.substring(0, name.length() - "Async".length());
        try {
          Method syncMethod = findSync(method, sync);
          Object result = syncMethod == null ? null : inner.invoke(proxy, syncMethod, args);
          return CompletableFuture.completedFuture(result);
        } catch (Throwable t) {
          CompletableFuture<Object> failed = new CompletableFuture<>();
          failed.completeExceptionally(t);
          return failed;
        }
      }
      return inner.invoke(proxy, method, args);
    };
  }

  private static Method findSync(Method asyncMethod, String syncName) {
    for (Method m : asyncMethod.getDeclaringClass().getMethods()) {
      if (m.getName().equals(syncName) && m.getParameterCount() == asyncMethod.getParameterCount()) {
        return m;
      }
    }
    return null;
  }

  /** A type-appropriate default for an un-handled interface method. */
  private static Object defaultFor(Method method) {
    Class<?> r = method.getReturnType();
    if (r.equals(Optional.class)) {
      return Optional.empty();
    }
    if (CompletableFuture.class.isAssignableFrom(r)) {
      return CompletableFuture.completedFuture(null);
    }
    if (r.equals(Map.class)) {
      return Map.of();
    }
    if (r.equals(boolean.class)) {
      return false;
    }
    if (r.equals(int.class)) {
      return 0;
    }
    if (r.equals(long.class)) {
      return 0L;
    }
    if (r.equals(double.class)) {
      return 0.0d;
    }
    if (r.equals(float.class)) {
      return 0.0f;
    }
    return null;
  }
}
