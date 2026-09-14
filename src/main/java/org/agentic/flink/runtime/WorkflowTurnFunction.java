package org.agentic.flink.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.agentic.flink.typeinfo.JsonTypeInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jagentic.core.AgentContext;
import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationState;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LogEvent;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;
import org.jagentic.core.pipeline.WorkflowValidator;

/**
 * The Flink runtime adapter for an {@code agentic/v1} workflow: one keyed operator that turns
 * {@link Event}s into normalized {@link TurnResult}s using the canonical core graph
 * ({@link org.jagentic.core.RoutedGraph} built by {@link GraphBuilder}).
 *
 * <p>What Flink provides here, and how it maps onto the spec's primitives:
 *
 * <ul>
 *   <li><b>Event log as keyed state.</b> The per-conversation log is a {@link ListState} of
 *       {@link LogEvent} plus a dense sequence counter; the graph appends to it through
 *       {@link KeyedConversationLog}. Conversation state is never stored separately: it is
 *       {@link ConversationState#fold} over that list, computed when a turn arrives.
 *   <li><b>Ordering.</b> The stream must be {@code keyBy(conversation_id)}; Flink then serializes
 *       all turns of a conversation onto one task, one at a time.
 *   <li><b>Idempotency and recovery.</b> The log is checkpointed with the operator, so a turn id
 *       already recorded in the restored log is answered as {@code duplicate} by the core without
 *       re-running the brain, tools or saga. Flink snapshots state between elements, so a partially
 *       processed turn is never checkpointed: after a failure the turn is either fully in the log
 *       (answered as duplicate) or not at all (executed once on redelivery).
 *   <li><b>Timers.</b> When {@link FlinkRuntimeOptions#resumeAfter()} is set, a suspended turn
 *       registers a Flink timer (processing or event time), appends {@code timer_scheduled}, and
 *       on firing appends {@code timer_fired} and resumes the turn with a
 *       {@code {kind: "timer"}} signal. Pending timers live in keyed state so they survive restore.
 *   <li><b>TTL.</b> {@link FlinkRuntimeOptions#stateTtl()} applies Flink state TTL to the log,
 *       counter and timer registry.
 *   <li><b>Serialization.</b> Log entries use {@link JsonTypeInfo}; results use
 *       {@link TurnResultTypeInfo}; nothing falls back to Kryo.
 *   <li><b>Metrics.</b> Per-status turn counters and a fired-timer counter on the operator's
 *       metric group.
 * </ul>
 *
 * <p>The workflow document (a {@code Map}) is the only serialized configuration; the graph, tool
 * registry and retriever are rebuilt in {@link #open} on every (re)start. Tools declared as
 * {@code kind: failing} count attempts in-process, so their budget also restarts with the task.
 */
public final class WorkflowTurnFunction extends KeyedProcessFunction<String, Event, TurnResult>
    implements ResultTypeQueryable<TurnResult> {
  private static final long serialVersionUID = 1L;

  public static final TypeInformation<Event> EVENT_TYPE = JsonTypeInfo.of(Event.class);
  public static final TypeInformation<LogEvent> LOG_EVENT_TYPE = JsonTypeInfo.of(LogEvent.class);

  static final String LOG_STATE = "conversation-log";
  static final String SEQUENCE_STATE = "conversation-log-next-sequence";
  static final String TIMER_STATE = "suspended-turn-timers";

  public static final String TIMER_SIGNAL_KIND = "timer";

  private final Map<String, Object> spec;
  private final FlinkRuntimeOptions options;
  private final ChatClientFactories.SerializableChatClientFactory chatClientFactory;

  private transient GraphBuilder.Built built;
  private transient ListState<LogEvent> log;
  private transient ValueState<Long> nextSequence;
  private transient MapState<Long, String> timers;
  private transient Map<TurnStatus, Counter> turnCounters;
  private transient Counter timersFired;

  /** Runtime options are taken from the document's {@code runtime.flink} block. */
  public WorkflowTurnFunction(Map<String, Object> spec) {
    this(spec, FlinkRuntimeOptions.fromSpec(spec));
  }

  public WorkflowTurnFunction(Map<String, Object> spec, FlinkRuntimeOptions options) {
    this(spec, options, ChatClientFactories.failFast());
  }

  /**
   * @param chatClientFactory builds the {@link org.jagentic.core.llm.ChatClient} for paths with
   *     {@code brain: llm}; it is serialized with the operator. With the default
   *     {@link ChatClientFactories#failFast()} a spec that declares an {@code llm} brain is
   *     rejected here, at job build time, unless its provider is the spec's deterministic
   *     {@code stub}, which {@link GraphBuilder} resolves without a factory.
   */
  public WorkflowTurnFunction(
      Map<String, Object> spec,
      FlinkRuntimeOptions options,
      ChatClientFactories.SerializableChatClientFactory chatClientFactory) {
    Objects.requireNonNull(spec, "spec");
    this.options = Objects.requireNonNull(options, "options");
    this.chatClientFactory = Objects.requireNonNull(chatClientFactory, "chatClientFactory");
    Map<String, Object> copy = new HashMap<>(spec);
    copy.remove("cep"); // CEP is wired natively by the job graph, not by the turn graph
    this.spec = copy;
    WorkflowValidator.validate(this.spec);
    if (ChatClientFactories.isFailFast(chatClientFactory) && !GraphBuilder.usesScriptedLlm(this.spec)) {
      List<String> llmPaths = llmBrainPaths(this.spec);
      if (!llmPaths.isEmpty()) {
        throw new IllegalArgumentException(
            "agent.paths " + llmPaths + " declare brain: llm but no ChatClientFactory was"
                + " configured; pass one to WorkflowTurnFunction(spec, options, factory)");
      }
    }
  }

  @SuppressWarnings("unchecked")
  static List<String> llmBrainPaths(Map<String, Object> spec) {
    List<String> out = new ArrayList<>();
    Map<String, Object> agent = (Map<String, Object>) spec.get("agent");
    if (agent == null) {
      return out;
    }
    Map<String, Object> paths = (Map<String, Object>) agent.get("paths");
    if (paths == null) {
      return out;
    }
    for (Map.Entry<String, Object> e : paths.entrySet()) {
      if (e.getValue() instanceof Map<?, ?> ps && "llm".equals(ps.get("brain"))) {
        out.add(e.getKey());
      }
    }
    return out;
  }

  public FlinkRuntimeOptions options() {
    return options;
  }

  @Override
  public TypeInformation<TurnResult> getProducedType() {
    return TurnResultTypeInfo.INSTANCE;
  }

  @Override
  public void open(OpenContext openContext) {
    built = GraphBuilder.build(spec, chatClientFactory);

    ListStateDescriptor<LogEvent> logDesc = new ListStateDescriptor<>(LOG_STATE, LOG_EVENT_TYPE);
    ValueStateDescriptor<Long> seqDesc = new ValueStateDescriptor<>(SEQUENCE_STATE, Types.LONG);
    MapStateDescriptor<Long, String> timerDesc = new MapStateDescriptor<>(TIMER_STATE, Types.LONG, Types.STRING);
    if (options.stateTtl() != null) {
      StateTtlConfig ttl = ttlConfig(options.stateTtl());
      logDesc.enableTimeToLive(ttl);
      seqDesc.enableTimeToLive(ttl);
      timerDesc.enableTimeToLive(ttl);
    }
    log = getRuntimeContext().getListState(logDesc);
    nextSequence = getRuntimeContext().getState(seqDesc);
    timers = getRuntimeContext().getMapState(timerDesc);

    turnCounters = new LinkedHashMap<>();
    for (TurnStatus s : TurnStatus.values()) {
      turnCounters.put(s, getRuntimeContext().getMetricGroup().counter("turns_" + s.wire()));
    }
    timersFired = getRuntimeContext().getMetricGroup().counter("timers_fired");
  }

  static StateTtlConfig ttlConfig(Duration ttl) {
    return StateTtlConfig.newBuilder(ttl)
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();
  }

  @Override
  public void processElement(Event event, Context ctx, Collector<TurnResult> out) throws Exception {
    String cid = ctx.getCurrentKey();
    if (!cid.equals(event.conversationId())) {
      throw new IllegalStateException("stream must be keyed by Event::conversationId; key " + cid
          + " carried event for " + event.conversationId());
    }
    ConversationLog keyedLog = new KeyedConversationLog(cid, log, nextSequence);
    TurnResult result = handle(event, keyedLog);

    if (result.status == TurnStatus.SUSPENDED && options.timerResume()) {
      LogEvent scheduled = scheduleResume(event.turnId(), keyedLog, ctx);
      result = withEvents(result, append(result.events, scheduled));
    }
    emit(result, out);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<TurnResult> out) throws Exception {
    String turnId = timers.get(timestamp);
    if (turnId == null) {
      return;
    }
    timers.remove(timestamp);
    String cid = ctx.getCurrentKey();
    ConversationLog keyedLog = new KeyedConversationLog(cid, log, nextSequence);
    String timerId = timerId(turnId, timestamp);
    LogEvent fired = keyedLog.append(cid, turnId, EventType.TIMER_FIRED,
        payload("timer_id", timerId, "turn_id", turnId, "fired_at", timestamp));
    timersFired.inc();

    ConversationState state = keyedLog.state(cid);
    if (!state.suspended().containsKey(turnId)) {
      return; // resumed by an explicit signal before the timer fired; the firing is recorded only
    }
    Map<String, Object> signal = payload("kind", TIMER_SIGNAL_KIND, "timer_id", timerId);
    TurnResult result = handle(Event.resume(cid, turnId, signal), keyedLog);
    emit(withEvents(result, prepend(fired, result.events)), out);
  }

  private TurnResult handle(Event event, ConversationLog keyedLog) {
    ConversationState before = keyedLog.state(event.conversationId());
    ConversationStore store = new ConversationStore.InMemory();
    for (ChatMessage m : before.transcript()) {
      store.append(event.conversationId(), m);
    }
    AgentContext agentCtx = new AgentContext(event.conversationId(), event.turnId(), event.userId(),
        store, new KeyedStateStore.InMemory(), built.tools(), built.retriever(), keyedLog,
        built.graph().policies());
    return built.graph().handle(event, agentCtx);
  }

  private LogEvent scheduleResume(String turnId, ConversationLog keyedLog, Context ctx) throws Exception {
    long now = options.timerDomain() == TimeDomain.EVENT_TIME
        ? requireTimestamp(ctx) : ctx.timerService().currentProcessingTime();
    long fireAt = now + options.resumeAfter().toMillis();
    while (timers.contains(fireAt)) {
      fireAt++; // one timer per timestamp per key; keep distinct turns distinct
    }
    if (options.timerDomain() == TimeDomain.EVENT_TIME) {
      ctx.timerService().registerEventTimeTimer(fireAt);
    } else {
      ctx.timerService().registerProcessingTimeTimer(fireAt);
    }
    timers.put(fireAt, turnId);
    return keyedLog.append(ctx.getCurrentKey(), turnId, EventType.TIMER_SCHEDULED,
        payload("timer_id", timerId(turnId, fireAt), "turn_id", turnId, "fire_at", fireAt,
            "domain", options.timerDomain().name().toLowerCase()));
  }

  private static long requireTimestamp(Context ctx) {
    Long ts = ctx.timestamp();
    if (ts == null) {
      throw new IllegalStateException("runtime.flink.timer_domain=event_time requires event timestamps;"
          + " assign a WatermarkStrategy to the source");
    }
    return ts;
  }

  private void emit(TurnResult result, Collector<TurnResult> out) {
    turnCounters.get(result.status).inc();
    out.collect(result);
  }

  private static String timerId(String turnId, long fireAt) {
    return UUID.nameUUIDFromBytes((turnId + "@" + fireAt).getBytes()).toString();
  }

  private static TurnResult withEvents(TurnResult r, List<LogEvent> events) {
    return new TurnResult(r.conversationId, r.turnId, r.status, r.path, r.reply, r.error, r.calls,
        events, r.state);
  }

  private static List<LogEvent> append(List<LogEvent> events, LogEvent last) {
    List<LogEvent> out = new ArrayList<>(events);
    out.add(last);
    return out;
  }

  private static List<LogEvent> prepend(LogEvent first, List<LogEvent> events) {
    List<LogEvent> out = new ArrayList<>(events.size() + 1);
    out.add(first);
    out.addAll(events);
    return out;
  }

  private static Map<String, Object> payload(Object... kv) {
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }
}
