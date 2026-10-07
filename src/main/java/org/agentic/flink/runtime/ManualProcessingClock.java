package org.agentic.flink.runtime;

import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.streaming.api.TimerService;

/**
 * A {@link ProcessingClock} whose reading moves only when {@link #advance(long)} is called.
 *
 * <p>The clock is identified by a {@code clockId}; its reading lives in a registry the whole JVM
 * shares, not in the clock object. That is what lets one logical time be observed by every
 * parallel instance of the workflow operator on an in-process cluster, and by the process that
 * drives it (a JUnit harness, or Python over Py4J or JPype), while the object itself stays a small
 * serializable handle Flink can ship inside the job graph and restore from a savepoint. The
 * reading is stored as a system property ({@value #PROPERTY_PREFIX}{@code <clockId>}) rather than
 * in a static field because an in-process cluster may load this class more than once: a job's
 * user code class loader (PyFlink adds the framework jar to the pipeline) sees its own copy of
 * every static, whereas system properties are one per JVM. A stop-with-savepoint followed by a
 * restore in the same JVM therefore keeps the reading, and pending workflow timers restored with
 * the conversation log fire exactly once when the clock reaches their deadline.
 *
 * <p>The registry entry exists from {@link #named(String)} until {@link #release()}. Reading or
 * advancing a released (or never registered) clock fails: an operator that runs in another JVM,
 * such as a TaskManager of a real cluster, cannot see a manual clock and must not silently read
 * zero. Production jobs keep the default {@link ProcessingClock#flink()}.
 */
public final class ManualProcessingClock implements ProcessingClock {
  private static final long serialVersionUID = 1L;

  /** System property prefix under which a clock's reading is registered. */
  public static final String PROPERTY_PREFIX = "agentic.flink.manual-clock.";

  private final String clockId;

  private ManualProcessingClock(String clockId) {
    this.clockId = clockId;
  }

  /** Registers a new clock at reading zero under a fresh random id. */
  public static ManualProcessingClock create() {
    return named("clock-" + UUID.randomUUID());
  }

  /**
   * The clock registered under {@code clockId}, registering it at reading zero first when it is
   * not registered yet. Two handles with the same id are the same clock.
   */
  public static ManualProcessingClock named(String clockId) {
    Objects.requireNonNull(clockId, "clockId");
    if (clockId.isBlank()) {
      throw new IllegalArgumentException("clockId must not be blank");
    }
    Properties registry = System.getProperties();
    synchronized (registry) {
      registry.putIfAbsent(key(clockId), "0");
    }
    return new ManualProcessingClock(clockId);
  }

  /** Whether a reading is registered for {@code clockId}. */
  public static boolean isRegistered(String clockId) {
    return System.getProperty(key(clockId)) != null;
  }

  public String clockId() {
    return clockId;
  }

  /** Moves the reading forward by {@code ms} (never backwards) and returns the new reading. */
  public long advance(long ms) {
    if (ms < 0) {
      throw new IllegalArgumentException("logical time never moves backwards: advance by " + ms);
    }
    Properties registry = System.getProperties();
    synchronized (registry) {
      long next = Math.addExact(reading(registry), ms);
      registry.setProperty(key(clockId), Long.toString(next));
      return next;
    }
  }

  /** The current reading in milliseconds. */
  public long nowMs() {
    Properties registry = System.getProperties();
    synchronized (registry) {
      return reading(registry);
    }
  }

  @Override
  public long nowMs(TimerService timerService) {
    return nowMs();
  }

  /** Removes the reading from the registry; the clock can no longer be read or advanced. */
  public void release() {
    Properties registry = System.getProperties();
    synchronized (registry) {
      registry.remove(key(clockId));
    }
  }

  private long reading(Properties registry) {
    String value = registry.getProperty(key(clockId));
    if (value == null) {
      throw new IllegalStateException("manual clock " + clockId + " is not registered in this JVM: it was"
          + " released, or the operator reading it does not share the JVM of the process that drives it");
    }
    return Long.parseLong(value);
  }

  private static String key(String clockId) {
    return PROPERTY_PREFIX + clockId;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ManualProcessingClock c && c.clockId.equals(clockId);
  }

  @Override
  public int hashCode() {
    return clockId.hashCode();
  }

  @Override
  public String toString() {
    return "ManualProcessingClock[" + clockId + "]";
  }
}
