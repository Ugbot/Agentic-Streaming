package org.agentic.flink.python;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thin wrapper around a PEMJA {@code PythonInterpreter}. One executor instance per task slot;
 * keeps the embedded interpreter warm across invocations, and caches unpickled callables so the
 * cloudpickle cost is paid once per registered tool / action.
 *
 * <p>PEMJA itself is an <b>optional</b> dependency. If {@code com.alibaba.pemja.PythonInterpreter}
 * is not on the classpath, {@link #isAvailable()} returns {@code false} and {@link #open()} throws
 * a descriptive {@link IllegalStateException} so the caller can fall back or report the error
 * cleanly. This is the same pattern the framework uses for Jedis (Redis) and DJL.
 *
 * <p>The wrapper avoids a direct compile-time reference to PEMJA classes; all PEMJA interaction
 * goes through reflection so the framework compiles and the rest of the test suite passes without
 * the optional jar.
 */
public final class PythonExecutor implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PythonExecutor.class);

  private static final String PEMJA_INTERPRETER_CLASS = "pemja.core.PythonInterpreter";
  private static final String PEMJA_CONFIG_CLASS = "pemja.core.PythonInterpreterConfig";

  private static final AtomicLong HANDLE_SEQ = new AtomicLong();

  private final String pythonExec;
  private final List<String> pythonPaths;
  private final Map<String, String> registeredHandles = new HashMap<>();

  private Object interpreter; // pemja.core.PythonInterpreter, opaque

  public PythonExecutor() {
    this(System.getenv().getOrDefault("PYTHON", "python3"), List.of());
  }

  public PythonExecutor(String pythonExec, List<String> pythonPaths) {
    this.pythonExec = pythonExec;
    this.pythonPaths = List.copyOf(pythonPaths);
  }

  /** Returns true when PEMJA is on the classpath. Use to skip / fall back cleanly. */
  public static boolean isAvailable() {
    try {
      Class.forName(PEMJA_INTERPRETER_CLASS);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /** Boot the embedded Python interpreter. Idempotent. */
  public synchronized void open() {
    if (interpreter != null) {
      return;
    }
    if (!isAvailable()) {
      throw new IllegalStateException(
          "PEMJA is not on the classpath. Add the optional dependency"
              + " com.alibaba:pemja:0.4.1 to use Python callbacks in agent plans.");
    }
    try {
      Class<?> configCls = Class.forName(PEMJA_CONFIG_CLASS);
      Object builder = configCls.getMethod("newBuilder").invoke(null);
      builder = builder.getClass().getMethod("setPythonExec", String.class).invoke(builder,
          pythonExec);
      if (!pythonPaths.isEmpty()) {
        builder = builder.getClass().getMethod("addPythonPaths", String[].class).invoke(builder,
            (Object) pythonPaths.toArray(new String[0]));
      }
      Object config = builder.getClass().getMethod("build").invoke(builder);
      Class<?> interpCls = Class.forName(PEMJA_INTERPRETER_CLASS);
      interpreter = interpCls.getConstructor(configCls).newInstance(config);
      exec("import cloudpickle, base64");
      exec("_AGFLINK_REG = {}");
      exec("def _agflink_load(handle, b64):\n"
          + "    _AGFLINK_REG[handle] = cloudpickle.loads(base64.b64decode(b64))\n");
      exec("def _agflink_call(handle, args, kwargs):\n"
          + "    fn = _AGFLINK_REG[handle]\n"
          + "    return fn(*args, **(kwargs or {}))\n");
      LOG.info("PythonExecutor opened: pythonExec={}, paths={}", pythonExec, pythonPaths);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to initialize PEMJA interpreter: " + e, e);
    }
  }

  /**
   * Deserialize a cloudpickled callable, register it under an opaque handle, and return the
   * handle. Subsequent calls to {@link #invoke(String, List, Map)} reuse the cached callable.
   */
  public synchronized String register(String cloudpickleB64) {
    require();
    String handle = "h" + HANDLE_SEQ.incrementAndGet();
    invokeFunction("_agflink_load", new Object[] {handle, cloudpickleB64});
    registeredHandles.put(handle, cloudpickleB64);
    return handle;
  }

  /** Invoke a previously registered Python callable. */
  public synchronized Object invoke(String handle, List<Object> args, Map<String, Object> kwargs) {
    require();
    if (!registeredHandles.containsKey(handle)) {
      throw new IllegalArgumentException("Unknown Python handle: " + handle);
    }
    return invokeFunction("_agflink_call",
        new Object[] {handle, args == null ? List.of() : args, kwargs == null ? Map.of() : kwargs});
  }

  /** Direct cloudpickle round-trip without caching — useful for tests and one-off callbacks. */
  public synchronized Object invokeOnce(String cloudpickleB64, List<Object> args) {
    String h = register(cloudpickleB64);
    try {
      return invoke(h, args, null);
    } finally {
      // Best-effort cleanup so single-shot calls don't grow the registry forever.
      try {
        exec("_AGFLINK_REG.pop('" + h + "', None)");
      } catch (Exception ignored) {
        // ignored
      }
      registeredHandles.remove(h);
    }
  }

  /** Fetch a global variable from the interpreter. Returns {@code null} if unset. */
  public synchronized Object get(String name) {
    require();
    try {
      return interpreter.getClass().getMethod("get", String.class).invoke(interpreter, name);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Python get('" + name + "') failed: " + e, e);
    }
  }

  /** Execute an arbitrary Python statement in the interpreter (for setup / tests). */
  public synchronized void exec(String code) {
    require();
    try {
      interpreter.getClass().getMethod("exec", String.class).invoke(interpreter, code);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Python exec failed: " + e, e);
    }
  }

  private Object invokeFunction(String name, Object[] args) {
    try {
      return interpreter.getClass()
          .getMethod("invoke", String.class, Object[].class)
          .invoke(interpreter, name, args);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Python invoke('" + name + "') failed: " + e.getCause() + " -> " + e, e);
    }
  }

  private void require() {
    if (interpreter == null) {
      throw new IllegalStateException("PythonExecutor.open() must be called before use");
    }
  }

  @Override
  public synchronized void close() {
    if (interpreter == null) {
      return;
    }
    try {
      interpreter.getClass().getMethod("close").invoke(interpreter);
    } catch (ReflectiveOperationException e) {
      LOG.warn("Failed to close PEMJA interpreter cleanly: {}", e.toString());
    } finally {
      interpreter = null;
      registeredHandles.clear();
    }
  }
}
