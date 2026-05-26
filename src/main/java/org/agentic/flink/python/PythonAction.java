package org.agentic.flink.python;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event-keyed Python action: a cloudpickled callable registered against one or more event-type
 * names. The {@code AgentPlanProcessFunction} dispatches incoming events to every {@code
 * PythonAction} whose {@link #events()} contains the inferred event type.
 *
 * <p>The Python callable is invoked as {@code fn(event, ctx)} where {@code event} is the routed
 * element (passed through to Python as-is) and {@code ctx} is a {@link Map} of contextual state
 * the operator chooses to expose (agent id, processing time, key, etc.). PEMJA marshals JVM
 * collections to native Python types automatically.
 */
public final class PythonAction implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PythonAction.class);

  private final String name;
  private final List<String> events;
  private final String cloudpickleB64;

  private transient PythonExecutor python;
  private transient String handle;

  public PythonAction(String name, List<String> events, String cloudpickleB64) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("name must be non-empty");
    }
    if (cloudpickleB64 == null || cloudpickleB64.isEmpty()) {
      throw new IllegalArgumentException("cloudpickleB64 must be non-empty");
    }
    this.name = name;
    this.events = events == null ? List.of() : List.copyOf(events);
    this.cloudpickleB64 = cloudpickleB64;
  }

  public String name() {
    return name;
  }

  public List<String> events() {
    return events;
  }

  public boolean matches(String eventType) {
    // Empty events list means "match anything" (a catch-all action).
    return events.isEmpty() || events.contains(eventType);
  }

  public synchronized void bind(PythonExecutor executor) {
    if (executor == null) {
      throw new IllegalArgumentException("PythonExecutor must not be null");
    }
    this.python = executor;
  }

  public synchronized Object invoke(Object event, Map<String, Object> ctx) {
    if (python == null) {
      throw new IllegalStateException("PythonAction '" + name + "' not bound");
    }
    if (handle == null) {
      handle = python.register(cloudpickleB64);
      LOG.debug("Registered Python action '{}' → handle {}", name, handle);
    }
    return python.invoke(handle, List.of(event, ctx == null ? Map.of() : ctx), null);
  }
}
