package org.jagentic.core.conformance;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jagentic.core.TurnProbe;

/**
 * The fixtures' {@code crash_at} verb on the JVM. A {@link Probe} is shared with every copy of the
 * runtime, serialized into a Flink job if need be, and finds the armed plan through its id, so the
 * crash is thrown by whichever thread reaches the point. {@link SimulatedCrash} is an {@link Error}:
 * nothing a turn does catches it, as nothing a turn does survives a process death.
 */
public final class Crash {

  /** The process died here. */
  public static final class SimulatedCrash extends Error {
    private static final long serialVersionUID = 1L;

    SimulatedCrash(String where) {
      super("crash_at " + where);
    }
  }

  private static final Map<String, Crash> ARMED = new ConcurrentHashMap<>();

  private final String axis;
  private final String name;
  private final String when;
  private final int occurrence;
  private final AtomicInteger seen = new AtomicInteger();
  private final CountDownLatch fired = new CountDownLatch(1);

  private Crash(String axis, String name, String when, int occurrence) {
    this.axis = axis;
    this.name = name;
    this.when = when;
    this.occurrence = occurrence;
  }

  /** The probe every run of this fixture carries; it does nothing until a plan is armed. */
  public static Probe newProbe() {
    return new Probe(UUID.randomUUID().toString());
  }

  /** The seam, with the id of the plan it serves. */
  public static final class Probe implements TurnProbe {
    private static final long serialVersionUID = 1L;
    private final String id;

    Probe(String id) {
      this.id = id;
    }

    /** Arms {@code spec}, a fixture's {@code crash_at} block, for the next delivery. */
    public void arm(Map<String, Object> spec) {
      String axis = spec.containsKey("event") ? "event" : "call";
      int occurrence = spec.get("occurrence") instanceof Number n ? n.intValue() : 1;
      ARMED.put(id, new Crash(axis, String.valueOf(spec.get(axis)), String.valueOf(spec.get("when")), occurrence));
    }

    /** Waits for the armed crash and reports whether it fired. */
    public boolean awaitFired(long millis) throws InterruptedException {
      Crash plan = ARMED.get(id);
      return plan != null && plan.fired.await(millis, TimeUnit.MILLISECONDS);
    }

    public void disarm() {
      ARMED.remove(id);
    }

    private void hit(String axis, String name, String when) {
      Crash plan = ARMED.get(id);
      if (plan == null || !plan.axis.equals(axis) || !plan.name.equals(name) || !plan.when.equals(when)) {
        return;
      }
      if (plan.fired.getCount() > 0 && plan.seen.incrementAndGet() == plan.occurrence) {
        plan.fired.countDown();
        throw new SimulatedCrash(axis + " " + name + " " + when);
      }
    }

    @Override
    public void beforeAppend(String eventType) {
      hit("event", eventType, "before");
    }

    @Override
    public void afterAppend(String eventType) {
      hit("event", eventType, "after");
    }

    @Override
    public void beforeCall(String toolId) {
      hit("call", toolId, "before");
    }

    @Override
    public void duringCall(String toolId) {
      hit("call", toolId, "during");
    }

    @Override
    public void afterCall(String toolId) {
      hit("call", toolId, "after");
    }
  }
}
