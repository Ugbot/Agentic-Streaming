package org.jagentic.pekko.entity;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.pekko.actor.NoSerializationVerificationNeeded;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.BackoffSupervisorStrategy;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.RecoveryCompleted;
import org.apache.pekko.persistence.typed.javadsl.CommandHandler;
import org.apache.pekko.persistence.typed.javadsl.Effect;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehavior;
import org.apache.pekko.persistence.typed.javadsl.SignalHandler;

import org.jagentic.core.AgentContext;
import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationState;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LogEvent;
import org.jagentic.core.Policies;
import org.jagentic.core.TurnError;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.pekko.runtime.AgentDeps;
import org.jagentic.pekko.serialization.CborSerializable;

/**
 * One conversation as an event-sourced Pekko entity. The persisted journal <em>is</em> the spec's
 * conversation log ({@code spec/v1/primitives.md}): every {@link LogEvent} the canonical
 * {@link org.jagentic.core.RoutedGraph} appends during a turn becomes one journal entry, in order,
 * with the dense zero-based {@code sequence} the graph assigned. Entity state is nothing but the
 * journal folded through {@link ConversationState#fold}; recovery replays the journal into that fold
 * and never touches the brain, tools, guardrails or memory.
 *
 * <p>The mailbox is the per-conversation single writer: a turn runs off the actor thread on the
 * blocking dispatcher while later commands are stashed, so turns of one conversation are applied
 * strictly in arrival order. A redelivered {@code turn_id} is answered from the fold with status
 * {@code duplicate}; nothing is appended and nothing re-runs.</p>
 *
 * <p>Timers are durable: {@link ScheduleTimer} appends {@code timer_scheduled} carrying the event to
 * deliver, the entity arms a Pekko timer, and on expiry appends {@code timer_fired} and processes the
 * carried event as an ordinary turn (typically a {@link Event#resume resume signal} for a suspended
 * turn). Pending timers are re-armed from the fold after recovery, so a restart cannot lose them.</p>
 */
public final class ConversationEntity
    extends EventSourcedBehavior<ConversationEntity.Command, ConversationEntity.Appended, ConversationEntity.State> {

  public static final String PERSISTENCE_PREFIX = "Conversation|";

  /** A failing journal write restarts the entity with backoff; the journal, not the heap, is the truth. */
  static final BackoffSupervisorStrategy PERSIST_FAILURE_BACKOFF =
      SupervisorStrategy.restartWithBackoff(Duration.ofMillis(50), Duration.ofSeconds(5), 0.1);

  // ---- protocol ----
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
  @JsonSubTypes({
      @JsonSubTypes.Type(value = ProcessTurn.class, name = "process_turn"),
      @JsonSubTypes.Type(value = GetState.class, name = "get_state"),
      @JsonSubTypes.Type(value = ScheduleTimer.class, name = "schedule_timer"),
      @JsonSubTypes.Type(value = TimerDue.class, name = "timer_due")})
  public sealed interface Command extends CborSerializable
      permits ProcessTurn, GetState, ScheduleTimer, TurnFinished, TimerDue {}

  /** Apply one turn (or resume signal) and reply with its normalized result. {@code event.turnId()} is the idempotency key. */
  public record ProcessTurn(Event event, ActorRef<TurnReply> replyTo) implements Command {
    public ProcessTurn {
      if (event == null || event.turnId() == null || event.turnId().isBlank()) {
        throw new IllegalArgumentException("ProcessTurn requires an event with a turn_id");
      }
    }
  }

  /** Read the folded state and the journal (tests, admin, front doors). */
  public record GetState(ActorRef<StateSnapshot> replyTo) implements Command {}

  /**
   * Durably schedule {@code fire} to be applied as a turn after {@code delayMs}. Re-sending the same
   * {@code timerId} is idempotent: the first schedule wins and is acknowledged again.
   */
  public record ScheduleTimer(String timerId, long delayMs, Event fire, ActorRef<TimerAck> replyTo)
      implements Command {
    public ScheduleTimer {
      if (timerId == null || timerId.isBlank()) {
        throw new IllegalArgumentException("ScheduleTimer requires a timer_id");
      }
      if (delayMs < 0) {
        throw new IllegalArgumentException("ScheduleTimer delay must be >= 0");
      }
      if (fire == null || fire.turnId() == null) {
        throw new IllegalArgumentException("ScheduleTimer requires an event with a turn_id to fire");
      }
    }
  }

  /** Internal: the off-thread turn finished. Never leaves the node. */
  public record TurnFinished(Event event, ActorRef<TurnReply> replyTo, List<LogEvent> staged,
                             TurnResult result, Throwable error)
      implements Command, NoSerializationVerificationNeeded {}

  /** Internal: a Pekko timer expired for a durably scheduled timer. */
  public record TimerDue(String timerId) implements Command {}

  /** The normalized result document ({@code spec/v1/result.schema.json}) as a message. */
  public record TurnReply(String conversationId, String turnId, TurnStatus status, String path,
                          String reply, TurnError error, List<org.jagentic.core.ToolCall> calls,
                          List<LogEvent> events, Map<String, Object> state) implements CborSerializable {

    public static TurnReply of(TurnResult r) {
      return new TurnReply(r.conversationId, r.turnId, r.status, r.path, r.reply, r.error, r.calls,
          r.events, r.state);
    }

    public TurnResult toResult() {
      return new TurnResult(conversationId, turnId, status, path, reply, error, calls, events, state);
    }

    public Map<String, Object> toMap() {
      return toResult().toMap();
    }

    public boolean ok() {
      return status == TurnStatus.COMPLETED;
    }
  }

  public record TimerAck(String conversationId, String timerId, long fireAt, boolean alreadyScheduled)
      implements CborSerializable {}

  /** A timer that has been scheduled but not yet fired: derived from the journal. */
  public record PendingTimer(String timerId, long fireAt, Event fire) implements CborSerializable {}

  public record StateSnapshot(String conversationId, long turnCount, long transcriptLength,
                              Map<String, Object> reduced, Set<String> suspendedTurnIds,
                              Map<String, PendingTimer> pendingTimers, List<LogEvent> events)
      implements CborSerializable {
    public int messageCount() {
      return (int) transcriptLength;
    }
  }

  // ---- journal entry: exactly one spec log event ----
  public record Appended(long sequence, String turnId, String type, Map<String, Object> payload)
      implements CborSerializable {
    public static Appended of(LogEvent e) {
      return new Appended(e.sequence(), e.turnId(), e.type(), e.payload());
    }

    public LogEvent toLogEvent(String conversationId) {
      return new LogEvent(conversationId, sequence, turnId, type, payload);
    }
  }

  // ---- state: the folded journal ----
  public static final class State {
    private final List<LogEvent> events = new ArrayList<>();
    private final Map<String, PendingTimer> timers = new LinkedHashMap<>();
    private ConversationState folded = ConversationState.empty();

    public List<LogEvent> events() {
      return List.copyOf(events);
    }

    public ConversationState folded() {
      return folded;
    }

    public Map<String, PendingTimer> pendingTimers() {
      return Map.copyOf(timers);
    }

    State apply(String conversationId, Appended a) {
      LogEvent e = a.toLogEvent(conversationId);
      if (e.sequence() != events.size()) {
        throw new IllegalStateException("conversation " + conversationId + " journal is not dense: expected "
            + events.size() + " but replayed " + e.sequence());
      }
      events.add(e);
      folded = ConversationState.fold(events);
      if (e.is(EventType.TIMER_SCHEDULED)) {
        Map<String, Object> p = e.payload();
        String id = String.valueOf(p.get("timer_id"));
        timers.put(id, new PendingTimer(id, ((Number) p.get("fire_at")).longValue(),
            eventFromMap(p.get("event"))));
      } else if (e.is(EventType.TIMER_FIRED)) {
        timers.remove(String.valueOf(e.payload().get("timer_id")));
      }
      return this;
    }
  }

  private final ActorContext<Command> context;
  private final TimerScheduler<Command> timers;
  private final String conversationId;
  private final AgentDeps deps;
  private final Executor blockingExecutor;
  private boolean inFlight = false;

  public static Behavior<Command> create(String conversationId, AgentDeps deps) {
    return Behaviors.withTimers(timers ->
        Behaviors.setup(ctx -> new ConversationEntity(ctx, timers, conversationId, deps)));
  }

  public static PersistenceId persistenceId(String conversationId) {
    return PersistenceId.ofUniqueId(PERSISTENCE_PREFIX + conversationId);
  }

  private ConversationEntity(ActorContext<Command> context, TimerScheduler<Command> timers,
                             String conversationId, AgentDeps deps) {
    super(persistenceId(conversationId), PERSIST_FAILURE_BACKOFF);
    this.context = context;
    this.timers = timers;
    this.conversationId = conversationId;
    this.deps = deps;
    this.blockingExecutor = context.getSystem().dispatchers().lookup(DispatcherSelector.blocking());
  }

  @Override
  public State emptyState() {
    return new State();
  }

  @Override
  public CommandHandler<Command, Appended, State> commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(ProcessTurn.class, this::onProcessTurn)
        .onCommand(TurnFinished.class, this::onTurnFinished)
        .onCommand(ScheduleTimer.class, this::onScheduleTimer)
        .onCommand(TimerDue.class, this::onTimerDue)
        .onCommand(GetState.class, this::onGetState)
        .build();
  }

  @Override
  public SignalHandler<State> signalHandler() {
    return newSignalHandlerBuilder()
        .onSignal(RecoveryCompleted.class, (state, sig) -> {
          for (PendingTimer t : state.timers.values()) {
            arm(t);
          }
        })
        .build();
  }

  // ---- turns ----

  private Effect<Appended, State> onProcessTurn(State state, ProcessTurn cmd) {
    Event event = cmd.event();
    ConversationState folded = state.folded;
    boolean resumesSuspended = event.isResume() && folded.suspended().containsKey(event.turnId());
    ConversationState.TurnRecord prior = folded.turn(event.turnId());
    if (!resumesSuspended && prior != null && prior.status() != null
        && deps.policies().idempotency() == Policies.Idempotency.TURN_ID) {
      TurnReply dup = TurnReply.of(recorded(prior).asDuplicate(folded.reduced()));
      return Effect().none().thenReply(cmd.replyTo(), s -> dup);
    }
    if (inFlight) {
      return Effect().stash();
    }
    inFlight = true;
    List<LogEvent> committed = state.events();
    CompletableFuture<TurnFinished> future =
        CompletableFuture.supplyAsync(() -> runTurn(committed, cmd), blockingExecutor);
    context.pipeToSelf(future, (done, error) -> error == null ? done
        : new TurnFinished(event, cmd.replyTo(), List.of(), null, error));
    return Effect().none();
  }

  /** Runs OFF the actor thread against a staged copy of the log; the actor persists what it staged. */
  private TurnFinished runTurn(List<LogEvent> committed, ProcessTurn cmd) {
    StagedLog log = new StagedLog(conversationId, committed);
    ConversationStore.InMemory store = new ConversationStore.InMemory();
    for (ChatMessage m : ConversationState.fold(committed).transcript()) {
      store.append(conversationId, m);
    }
    Event event = cmd.event();
    AgentContext ctx = new AgentContext(conversationId, event.turnId(), event.userId(), store,
        new KeyedStateStore.InMemory(), deps.tools(), deps.retriever(), log, deps.policies());
    TurnResult result;
    try {
      result = deps.graph().handle(event, ctx);
    } catch (RuntimeException e) {
      String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      TurnError error = new TurnError(TurnError.ErrorClass.FATAL, message);
      Map<String, Object> p = new LinkedHashMap<>();
      p.put("status", TurnStatus.FAILED.wire());
      p.put("reason", message);
      p.put("error_class", error.errorClass().wire());
      ctx.record(EventType.TURN_FAILED, p);
      result = new TurnResult(conversationId, event.turnId(), TurnStatus.FAILED, null, null, error,
          ctx.calls, ctx.events, ctx.conversationState().reduced());
    }
    return new TurnFinished(event, cmd.replyTo(), log.staged(), result, null);
  }

  private Effect<Appended, State> onTurnFinished(State state, TurnFinished c) {
    inFlight = false;
    if (c.error() != null) {
      throw new IllegalStateException("turn " + c.event().turnId() + " of " + conversationId
          + " could not be applied", c.error());
    }
    List<Appended> journal = new ArrayList<>(c.staged().size());
    for (LogEvent e : c.staged()) {
      journal.add(Appended.of(e));
    }
    TurnReply reply = TurnReply.of(c.result());
    if (journal.isEmpty()) {
      return Effect().none().thenReply(c.replyTo(), s -> reply).thenUnstashAll();
    }
    return Effect().persist(journal).thenReply(c.replyTo(), s -> reply).thenUnstashAll();
  }

  // ---- timers ----

  private Effect<Appended, State> onScheduleTimer(State state, ScheduleTimer cmd) {
    PendingTimer existing = state.timers.get(cmd.timerId());
    if (existing != null) {
      TimerAck ack = new TimerAck(conversationId, cmd.timerId(), existing.fireAt(), true);
      return Effect().none().thenReply(cmd.replyTo(), s -> ack);
    }
    if (alreadyFired(state, cmd.timerId())) {
      TimerAck ack = new TimerAck(conversationId, cmd.timerId(), -1L, true);
      return Effect().none().thenReply(cmd.replyTo(), s -> ack);
    }
    if (inFlight) {
      return Effect().stash();
    }
    long fireAt = System.currentTimeMillis() + cmd.delayMs();
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("timer_id", cmd.timerId());
    p.put("fire_at", fireAt);
    p.put("delay_ms", cmd.delayMs());
    p.put("event", eventToMap(cmd.fire()));
    Appended evt = new Appended(state.events.size(), cmd.fire().turnId(), EventType.TIMER_SCHEDULED.wire(), p);
    TimerAck ack = new TimerAck(conversationId, cmd.timerId(), fireAt, false);
    return Effect().persist(evt)
        .thenRun(s -> arm(s.timers.get(cmd.timerId())))
        .thenReply(cmd.replyTo(), s -> ack);
  }

  private Effect<Appended, State> onTimerDue(State state, TimerDue due) {
    PendingTimer t = state.timers.get(due.timerId());
    if (t == null) {
      return Effect().none();
    }
    if (inFlight) {
      return Effect().stash();
    }
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("timer_id", t.timerId());
    p.put("fire_at", t.fireAt());
    Appended evt = new Appended(state.events.size(), t.fire().turnId(), EventType.TIMER_FIRED.wire(), p);
    return Effect().persist(evt)
        .thenRun(s -> context.getSelf().tell(new ProcessTurn(t.fire(), context.getSystem().ignoreRef())));
  }

  private void arm(PendingTimer t) {
    long remaining = Math.max(0L, t.fireAt() - System.currentTimeMillis());
    timers.startSingleTimer("timer|" + t.timerId(), new TimerDue(t.timerId()), Duration.ofMillis(remaining));
  }

  private static boolean alreadyFired(State state, String timerId) {
    for (LogEvent e : state.events) {
      if (e.is(EventType.TIMER_FIRED) && timerId.equals(String.valueOf(e.payload().get("timer_id")))) {
        return true;
      }
    }
    return false;
  }

  // ---- reads ----

  private Effect<Appended, State> onGetState(State state, GetState cmd) {
    ConversationState f = state.folded;
    StateSnapshot snap = new StateSnapshot(conversationId, f.turnCount(), f.transcriptLength(),
        f.reduced(), Set.copyOf(f.suspended().keySet()), state.pendingTimers(), state.events());
    return Effect().none().thenReply(cmd.replyTo(), s -> snap);
  }

  @Override
  public EventHandler<State, Appended> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onEvent(Appended.class, (state, evt) -> state.apply(conversationId, evt))
        .build();
  }

  /** The recorded result of an applied turn, rebuilt from the fold alone (mirrors the core). */
  private TurnResult recorded(ConversationState.TurnRecord rec) {
    return new TurnResult(conversationId, rec.turnId(), rec.status(), rec.path(), rec.reply(),
        rec.error(), rec.calls(), rec.events(), Map.of());
  }

  static Map<String, Object> eventToMap(Event e) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("conversation_id", e.conversationId());
    m.put("turn_id", e.turnId());
    m.put("user_id", e.userId());
    m.put("text", e.text());
    m.put("metadata", e.metadata() == null ? Map.of() : new LinkedHashMap<>(e.metadata()));
    m.put("signal", e.signal() == null ? null : new LinkedHashMap<>(e.signal()));
    return m;
  }

  @SuppressWarnings("unchecked")
  static Event eventFromMap(Object o) {
    if (!(o instanceof Map<?, ?> raw)) {
      throw new IllegalStateException("timer_scheduled payload carries no event: " + o);
    }
    Map<String, Object> m = (Map<String, Object>) raw;
    Map<String, String> metadata = new LinkedHashMap<>();
    if (m.get("metadata") instanceof Map<?, ?> md) {
      md.forEach((k, v) -> metadata.put(String.valueOf(k), v == null ? null : String.valueOf(v)));
    }
    Map<String, Object> signal = m.get("signal") instanceof Map<?, ?> s
        ? new LinkedHashMap<>((Map<String, Object>) s) : null;
    return new Event(str(m.get("conversation_id")), str(m.get("turn_id")), str(m.get("user_id")),
        str(m.get("text")), metadata, signal);
  }

  private static String str(Object o) {
    return o == null ? null : String.valueOf(o);
  }
}
