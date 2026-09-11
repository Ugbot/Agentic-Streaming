package org.jagentic.core;

import java.io.Serializable;
import java.util.Map;

/**
 * The {@code policies} block of a workflow (spec/v1/workflow.schema.json {@code $defs.policies}).
 * Defaults are normative: per-conversation ordering, turn-id idempotency, no retries, one
 * verification attempt, tool errors fail the turn.
 */
public record Policies(Ordering ordering, Idempotency idempotency, RetryPolicy retry,
                       VerificationPolicy verification, OnToolError onToolError)
    implements Serializable {

  public enum Ordering { PER_CONVERSATION, NONE }

  public enum Idempotency { TURN_ID, NONE }

  public enum OnToolError { FAIL, CONTINUE }

  public static final Policies DEFAULTS = new Policies(Ordering.PER_CONVERSATION,
      Idempotency.TURN_ID, RetryPolicy.NONE, VerificationPolicy.DEFAULT, OnToolError.FAIL);

  public Policies {
    ordering = ordering == null ? Ordering.PER_CONVERSATION : ordering;
    idempotency = idempotency == null ? Idempotency.TURN_ID : idempotency;
    retry = retry == null ? RetryPolicy.NONE : retry;
    verification = verification == null ? VerificationPolicy.DEFAULT : verification;
    onToolError = onToolError == null ? OnToolError.FAIL : onToolError;
  }

  public Policies withRetry(RetryPolicy r) {
    return new Policies(ordering, idempotency, r, verification, onToolError);
  }

  public Policies withVerification(VerificationPolicy v) {
    return new Policies(ordering, idempotency, retry, v, onToolError);
  }

  public Policies withOnToolError(OnToolError o) {
    return new Policies(ordering, idempotency, retry, verification, o);
  }

  public Policies withIdempotency(Idempotency i) {
    return new Policies(ordering, i, retry, verification, onToolError);
  }

  /** Parses a (schema-valid) {@code policies} map; {@code null} yields the defaults. */
  public static Policies fromMap(Map<String, Object> m) {
    if (m == null) {
      return DEFAULTS;
    }
    Ordering ordering = "none".equals(m.get("ordering")) ? Ordering.NONE : Ordering.PER_CONVERSATION;
    Idempotency idem = "none".equals(m.get("idempotency")) ? Idempotency.NONE : Idempotency.TURN_ID;
    OnToolError onErr = "continue".equals(m.get("on_tool_error")) ? OnToolError.CONTINUE
        : OnToolError.FAIL;
    return new Policies(ordering, idem, RetryPolicy.fromMap(sub(m, "retry")),
        VerificationPolicy.fromMap(sub(m, "verification")), onErr);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> sub(Map<String, Object> m, String key) {
    Object o = m.get(key);
    return o instanceof Map<?, ?> mm ? (Map<String, Object>) mm : null;
  }

  /** {@code policies.retry}: bounded, deterministic unless {@code jitter} is set. */
  public record RetryPolicy(Kind kind, int maxAttempts, long initialDelayMs, double multiplier,
                            long maxDelayMs, boolean jitter) implements Serializable {

    public enum Kind { NONE, FIXED, EXPONENTIAL }

    public static final RetryPolicy NONE = new RetryPolicy(Kind.NONE, 1, 100, 2.0, 5000, false);

    public RetryPolicy {
      kind = kind == null ? Kind.NONE : kind;
      if (maxAttempts < 1) {
        throw new IllegalArgumentException("policies.retry.max_attempts must be >= 1");
      }
      if (multiplier < 1) {
        throw new IllegalArgumentException("policies.retry.multiplier must be >= 1");
      }
    }

    public static RetryPolicy fixed(int maxAttempts, long delayMs) {
      return new RetryPolicy(Kind.FIXED, maxAttempts, delayMs, 1.0, delayMs, false);
    }

    public static RetryPolicy exponential(int maxAttempts, long initialDelayMs, double multiplier,
                                          long maxDelayMs) {
      return new RetryPolicy(Kind.EXPONENTIAL, maxAttempts, initialDelayMs, multiplier, maxDelayMs,
          false);
    }

    /** The attempt budget the tool loop uses: 1 when {@code kind} is {@code none}. */
    public int attempts() {
      return kind == Kind.NONE ? 1 : maxAttempts;
    }

    /**
     * The {@code retry} verb: delay before the attempt numbered {@code nextAttempt} (2-based), or
     * {@code -1} to give up. Without jitter the answer is a pure function of the attempt number.
     */
    public long delayBefore(int nextAttempt, java.util.random.RandomGenerator random) {
      if (nextAttempt > attempts() || kind == Kind.NONE) {
        return -1;
      }
      long delay;
      if (kind == Kind.FIXED) {
        delay = initialDelayMs;
      } else {
        double d = initialDelayMs * Math.pow(multiplier, nextAttempt - 2);
        delay = (long) Math.min(d, (double) maxDelayMs);
      }
      delay = Math.min(delay, maxDelayMs);
      if (jitter && delay > 0 && random != null) {
        delay = random.nextLong(delay + 1);
      }
      return delay;
    }

    public static RetryPolicy fromMap(Map<String, Object> m) {
      if (m == null) {
        return NONE;
      }
      Kind kind = Kind.valueOf(String.valueOf(m.getOrDefault("kind", "none")).toUpperCase());
      return new RetryPolicy(kind,
          num(m.get("max_attempts"), 1).intValue(),
          num(m.get("initial_delay_ms"), 100).longValue(),
          num(m.get("multiplier"), 2.0).doubleValue(),
          num(m.get("max_delay_ms"), 5000).longValue(),
          Boolean.TRUE.equals(m.get("jitter")));
    }
  }

  /** {@code policies.verification}: how many drafts the verifier may reject and what follows. */
  public record VerificationPolicy(int maxAttempts, OnExhausted onExhausted) implements Serializable {

    public enum OnExhausted { UNVERIFIED, FAIL }

    public static final VerificationPolicy DEFAULT = new VerificationPolicy(1, OnExhausted.UNVERIFIED);

    public VerificationPolicy {
      if (maxAttempts < 1) {
        throw new IllegalArgumentException("policies.verification.max_attempts must be >= 1");
      }
      onExhausted = onExhausted == null ? OnExhausted.UNVERIFIED : onExhausted;
    }

    public static VerificationPolicy fromMap(Map<String, Object> m) {
      if (m == null) {
        return DEFAULT;
      }
      OnExhausted on = "fail".equals(m.get("on_exhausted")) ? OnExhausted.FAIL : OnExhausted.UNVERIFIED;
      return new VerificationPolicy(num(m.get("max_attempts"), 1).intValue(), on);
    }
  }

  private static Number num(Object o, Number dflt) {
    return o instanceof Number n ? n : dflt;
  }
}
