package org.jagentic.core;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The record of every tool and model call, kept outside the runtime's own state so that it survives
 * whatever rolls the log back (spec section 10). On Flink the log is keyed state, which returns to
 * the last checkpoint after a failure while the outside world does not; the journal is what lets a
 * re-run turn reuse what already happened instead of doing it again.
 *
 * <p>An entry is written whole, so a store needs only a keyed upsert: a table or a compacted topic.
 * {@link #put} and {@link #putIfAbsent} must not return before the write is durable. A store that
 * cannot be reached throws {@link Unavailable}, which stops the call from being made.
 *
 * <p>The design borrows from Flink Agents' action state store: write-ahead intent, then the outcome,
 * reused on recovery. No code is copied. This is a keyed upsert store rather than an offset replay,
 * since entries are small and keyed.
 */
public interface EffectJournal extends AutoCloseable {

  /** The kind of a {@code tool} entry. */
  String TOOL = "tool";
  /** The kind of a model call's entry, counted by its own index within the turn. */
  String LLM = "llm";

  /** What the store holds per call. {@code FAILED} and {@code SUCCEEDED} are final. */
  enum State {
    PENDING,
    SUCCEEDED,
    FAILED,
    UNKNOWN
  }

  /** The identity of one call. Derived from recorded input alone, so a re-run regenerates it. */
  record Key(String conversationId, String turnId, String kind, int index) implements Serializable {
    /** The form a string-keyed store uses. */
    public String asString() {
      return conversationId + "\u0000" + turnId + "\u0000" + kind + "\u0000" + index;
    }
  }

  /** One failed attempt at a call. */
  record Attempt(int attempt, String error) implements Serializable {}

  /**
   * One call. {@code intent} is what the call was, as {@link #intentOf} and {@link #modelIntentOf}
   * write it.
   * {@code released} means a person said an unknown call did not land and may be made again.
   */
  record Entry(Key key, String intent, String effect, State state, List<Attempt> attempts,
               Object result, String error, boolean released) implements Serializable {
    public Entry {
      attempts = attempts == null ? List.of() : List.copyOf(attempts);
    }

    public static Entry pending(Key key, String intent, String effect) {
      return new Entry(key, intent, effect, State.PENDING, List.of(), null, null, false);
    }

    public Entry succeeded(Object value) {
      return new Entry(key, intent, effect, State.SUCCEEDED, attempts, value, null, false);
    }

    public Entry failed(String message) {
      return new Entry(key, intent, effect, State.FAILED, attempts, null, message, false);
    }

    public Entry unknown() {
      return new Entry(key, intent, effect, State.UNKNOWN, attempts, null, null, false);
    }

    /** A person's decision that the call may be made again, under the same key. */
    public Entry release() {
      return new Entry(key, intent, effect, State.PENDING, attempts, null, null, true);
    }

    public Entry withAttempt(int attempt, String message) {
      List<Attempt> more = new ArrayList<>(attempts);
      more.add(new Attempt(attempt, message));
      return new Entry(key, intent, effect, state, more, result, error, released);
    }

    public boolean isFinal() {
      return state == State.SUCCEEDED || state == State.FAILED;
    }
  }

  /** The journal could not be read or written: infrastructure, like an unreachable log. */
  final class Unavailable extends LogUnavailable {
    private static final long serialVersionUID = 1L;

    public Unavailable(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** A re-run asked for a different call under a recorded key: the turn did not replay. */
  final class Divergence extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** The run ended with a recorded call still ahead of it. */
    public Divergence(Key key) {
      super("call " + key.index() + " of turn " + key.turnId() + " is recorded but this run ended before it");
    }

    public Divergence(Key key, String recorded, String asked) {
      super("call " + key.index() + " of turn " + key.turnId() + " was recorded as " + recorded
          + " but this run asked for " + asked);
    }
  }

  /** One way of reaching the outside world, driven by {@link #recordOrGet}. */
  interface Call {
    /** The durable intent exists. Runs before any attempt, on every run of the turn. */
    void onIntent(Entry entry);

    /** A failed attempt, either replayed from the journal or just made. */
    void onFailedAttempt(Attempt attempt);

    /** The number of attempts the call may use in all, counting those already recorded. */
    int budget();

    /** Makes attempt number {@code attempt}. The retry back-off is the caller's business. */
    Object attempt(int attempt);

    /** What an attempt's error is recorded as. */
    default String recorded(String error) {
      return error;
    }

    /** After an attempt returned and before its outcome is journaled. */
    default void afterAttempt() {}

    /** After the outcome is durable and before it is logged. */
    default void afterRecord() {}
  }

  Optional<Entry> get(Key key);

  /** Durable upsert of the whole entry. */
  void put(Entry entry);

  /** Durably writes {@code entry} unless the key is taken, and returns what was there, if anything. */
  Optional<Entry> putIfAbsent(Entry entry);

  /** True when entries outlive this process, so a {@code non_idempotent} tool may rely on it. */
  default boolean durable() {
    return true;
  }

  @Override
  default void close() {}

  /**
   * The only way a tool is reached (spec section 10). The returned entry is {@code SUCCEEDED},
   * {@code FAILED} or {@code UNKNOWN}. Nothing is called when an outcome is already recorded. A
   * {@code pending} entry means the previous run stopped without an answer: a {@code read} or
   * {@code idempotent} call carries on with its next attempt under the same key, and a {@code
   * non_idempotent} call becomes {@code UNKNOWN}, never a second call.
   */
  default Entry recordOrGet(Key key, String intent, String effect, Call call) {
    Entry fresh = Entry.pending(key, intent, effect);
    Optional<Entry> existing = putIfAbsent(fresh);
    Entry entry = existing.orElse(fresh);
    if (!entry.intent().equals(intent)) {
      throw new Divergence(key, entry.intent(), intent);
    }
    call.onIntent(entry);
    for (Attempt failed : entry.attempts()) {
      call.onFailedAttempt(failed);
    }
    if (entry.isFinal() || entry.state() == State.UNKNOWN) {
      return entry;
    }
    // What a run recorded outranks what a later version of the workflow declares.
    boolean once = ToolEffect.NON_IDEMPOTENT.equals(effect) || ToolEffect.NON_IDEMPOTENT.equals(entry.effect());
    if (entry.released()) {
      // The person's permission covers one call, so it is spent before the call is made: a crash
      // during it leaves a pending entry, which is unknown again.
      entry = new Entry(key, entry.intent(), entry.effect(), State.PENDING, entry.attempts(), null, null, false);
      put(entry);
    } else if (existing.isPresent() && once) {
      entry = entry.unknown();
      put(entry);
      return entry;
    }
    int budget = once ? entry.attempts().size() + 1 : call.budget();
    for (int attempt = entry.attempts().size() + 1; attempt <= budget; attempt++) {
      Object result;
      try {
        result = call.attempt(attempt);
      } catch (LogUnavailable e) {
        throw e;
      } catch (RuntimeException e) {
        if (e instanceof OutcomeUnknown && once) {
          entry = entry.unknown();
          put(entry);
          return entry;
        }
        String message = call.recorded(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        entry = entry.withAttempt(attempt, message);
        if (attempt >= budget) {
          entry = entry.failed(message);
        }
        put(entry);
        call.onFailedAttempt(new Attempt(attempt, message));
        continue;
      }
      call.afterAttempt();
      entry = entry.succeeded(result);
      put(entry);
      call.afterRecord();
      return entry;
    }
    entry = entry.failed(entry.attempts().isEmpty() ? "no attempts left"
        : entry.attempts().get(entry.attempts().size() - 1).error());
    put(entry);
    return entry;
  }

  /**
   * {@code lowercase hex SHA-256 of conversation_id NUL turn_id NUL index} (spec section 10): the
   * same on every run of the turn and on every runtime, and safe to send as a header.
   */
  static String idempotencyKey(String conversationId, String turnId, int index) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      sha.update(conversationId.getBytes(StandardCharsets.UTF_8));
      sha.update((byte) 0);
      sha.update(turnId.getBytes(StandardCharsets.UTF_8));
      sha.update((byte) 0);
      sha.update(Integer.toString(index).getBytes(StandardCharsets.US_ASCII));
      return HexFormat.of().formatHex(sha.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is mandatory on the JVM", e);
    }
  }

  /**
   * What a re-run must ask for again under the same key: the tool, then a SHA-256 of the workflow
   * and the arguments as canonical JSON. A short hash would let two different calls pass for one.
   *
   * @param scope the agent id, so a store two workflows share cannot answer one with the other's call
   */
  static String intentOf(String scope, String toolId, Map<String, Object> args) {
    return toolId + ":" + digest(scope, EffectJournals.canonicalJson(args));
  }

  /** As {@link #intentOf} for a model call; {@code request} is what the brain was told to do. */
  static String modelIntentOf(String scope, String request) {
    return "llm:" + digest(scope, request);
  }

  private static String digest(String scope, String body) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      sha.update(scope.getBytes(StandardCharsets.UTF_8));
      sha.update((byte) 0);
      sha.update(body.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(sha.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is mandatory on the JVM", e);
    }
  }

  /** A journal that records nothing: every run calls again, under the same key. */
  EffectJournal NONE = new EffectJournal() {
    @Override
    public Optional<Entry> get(Key key) {
      return Optional.empty();
    }

    @Override
    public void put(Entry entry) {}

    @Override
    public Optional<Entry> putIfAbsent(Entry entry) {
      return Optional.empty();
    }

    @Override
    public boolean durable() {
      return false;
    }
  };

  /** Holds entries in this process: it outlives a runtime or a task, not the JVM. */
  final class InMemory implements EffectJournal {
    private final Map<Key, Entry> entries = new ConcurrentHashMap<>();

    @Override
    public Optional<Entry> get(Key key) {
      return Optional.ofNullable(entries.get(key));
    }

    @Override
    public void put(Entry entry) {
      entries.put(entry.key(), entry);
    }

    @Override
    public Optional<Entry> putIfAbsent(Entry entry) {
      return Optional.ofNullable(entries.putIfAbsent(entry.key(), entry));
    }

    @Override
    public boolean durable() {
      return false;
    }
  }
}
