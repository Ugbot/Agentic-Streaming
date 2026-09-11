package org.agentic.flink.runtime;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.streaming.api.TimeDomain;

/**
 * Flink-only knobs for running a workflow document on this runtime. They are read from the
 * document's {@code runtime.flink} block (the workflow schema allows per-runtime extension blocks
 * under {@code runtime}) and can be overridden programmatically.
 *
 * <pre>
 * runtime:
 *   flink:
 *     state_ttl_ms: 86400000      # keyed conversation state expires after a day of inactivity
 *     resume_after_ms: 5000       # suspended turns are resumed by a registered timer after 5 s
 *     timer_domain: processing_time | event_time
 * </pre>
 *
 * <p>Neither setting changes the observable turn semantics of the spec: TTL only bounds how long a
 * conversation's event log is retained, and the resume timer only decides <em>when</em> a suspended
 * turn is resumed (the spec's {@code turn_suspended → timer_scheduled → timer_fired → turn_resumed}
 * sequence). When {@code resume_after_ms} is absent, suspended turns wait for an explicit resume
 * signal and no timer is registered.
 *
 * @param stateTtl retention for keyed conversation state, or {@code null} for unbounded retention
 * @param resumeAfter delay before a suspended turn is resumed by timer, or {@code null} for none
 * @param timerDomain the Flink time domain the resume timer is registered in
 */
public record FlinkRuntimeOptions(Duration stateTtl, Duration resumeAfter, TimeDomain timerDomain)
    implements Serializable {

  public static final String RUNTIME_KEY = "runtime";
  public static final String FLINK_KEY = "flink";
  public static final String STATE_TTL_MS = "state_ttl_ms";
  public static final String RESUME_AFTER_MS = "resume_after_ms";
  public static final String TIMER_DOMAIN = "timer_domain";

  public static final FlinkRuntimeOptions DEFAULTS =
      new FlinkRuntimeOptions(null, null, TimeDomain.PROCESSING_TIME);

  public FlinkRuntimeOptions {
    timerDomain = timerDomain == null ? TimeDomain.PROCESSING_TIME : timerDomain;
    if (stateTtl != null && (stateTtl.isZero() || stateTtl.isNegative())) {
      throw new IllegalArgumentException(STATE_TTL_MS + " must be positive, got " + stateTtl);
    }
    if (resumeAfter != null && resumeAfter.isNegative()) {
      throw new IllegalArgumentException(RESUME_AFTER_MS + " must be >= 0, got " + resumeAfter);
    }
  }

  public FlinkRuntimeOptions withStateTtl(Duration ttl) {
    return new FlinkRuntimeOptions(ttl, resumeAfter, timerDomain);
  }

  public FlinkRuntimeOptions withResumeAfter(Duration delay) {
    return new FlinkRuntimeOptions(stateTtl, delay, timerDomain);
  }

  public FlinkRuntimeOptions withTimerDomain(TimeDomain domain) {
    return new FlinkRuntimeOptions(stateTtl, resumeAfter, domain);
  }

  /** Whether suspended turns are resumed by a registered timer rather than an explicit signal. */
  public boolean timerResume() {
    return resumeAfter != null;
  }

  /** Reads {@code runtime.flink} from a workflow document; absent block yields {@link #DEFAULTS}. */
  public static FlinkRuntimeOptions fromSpec(Map<String, Object> spec) {
    Objects.requireNonNull(spec, "spec");
    Object runtime = spec.get(RUNTIME_KEY);
    if (!(runtime instanceof Map<?, ?> runtimeMap)) {
      return DEFAULTS;
    }
    Object flink = runtimeMap.get(FLINK_KEY);
    if (!(flink instanceof Map<?, ?> f)) {
      return DEFAULTS;
    }
    Duration ttl = millis(f.get(STATE_TTL_MS), STATE_TTL_MS);
    Duration resume = millis(f.get(RESUME_AFTER_MS), RESUME_AFTER_MS);
    TimeDomain domain = TimeDomain.PROCESSING_TIME;
    Object rawDomain = f.get(TIMER_DOMAIN);
    if (rawDomain != null) {
      String d = String.valueOf(rawDomain).trim().toLowerCase();
      switch (d) {
        case "processing_time" -> domain = TimeDomain.PROCESSING_TIME;
        case "event_time" -> domain = TimeDomain.EVENT_TIME;
        default -> throw new IllegalArgumentException(
            "runtime.flink." + TIMER_DOMAIN + " must be processing_time|event_time, got " + rawDomain);
      }
    }
    return new FlinkRuntimeOptions(ttl, resume, domain);
  }

  private static Duration millis(Object raw, String key) {
    if (raw == null) {
      return null;
    }
    if (!(raw instanceof Number n)) {
      throw new IllegalArgumentException("runtime.flink." + key + " must be a number of milliseconds, got " + raw);
    }
    return Duration.ofMillis(n.longValue());
  }
}
