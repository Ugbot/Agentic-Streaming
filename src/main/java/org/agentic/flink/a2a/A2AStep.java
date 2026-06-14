package org.agentic.flink.a2a;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * An explicit, deterministic remote-A2A step in an agent workflow.
 *
 * <p>Where {@link A2AToolExecutor} exposes a peer as an LLM-selectable tool (the model decides when
 * to call it), {@code A2AStep} splices a remote agent into the stream graph at a fixed position —
 * for orchestrations like {@code localAgent → A2AStep(peer) → localAgent} where the delegation is
 * part of the topology, not a model choice. {@link #applyTo(DataStream)} keys the stream by A2A
 * {@code contextId} and runs an {@link A2ADelegatingProcessFunction}, returning the enriched stream.
 *
 * <p>Serializable so it can be recorded on an {@link org.agentic.flink.job.AgentJob} and shipped in
 * the job graph. The default key selector groups by {@link AgentEvent} {@code correlationId} (then
 * {@code flowId}); override with {@link Builder#withKeySelector}.
 */
public final class A2AStep implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Default key selector: correlationId, falling back to flowId. */
  public static final class ContextKeySelector implements KeySelector<AgentEvent, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String getKey(AgentEvent event) {
      if (event.getCorrelationId() != null) {
        return event.getCorrelationId();
      }
      return event.getFlowId() != null ? event.getFlowId() : "default";
    }
  }

  private final String name;
  private final RemoteAgentSpec spec;
  private final A2AClientFactory clientFactory;
  private final String inputKey;
  private final String outputKey;
  private final boolean failOnError;
  private final KeySelector<AgentEvent, String> keySelector;
  private final int capacity;
  private final ConversationStore conversationStore;
  private final long asyncTimeoutMs;

  private A2AStep(Builder b) {
    this.spec = Objects.requireNonNull(b.spec, "spec");
    this.name = b.name == null ? spec.name() : b.name;
    this.clientFactory =
        b.clientFactory == null ? A2AClientFactory.discovering() : b.clientFactory;
    this.inputKey = b.inputKey;
    this.outputKey = b.outputKey == null ? "a2a." + name : b.outputKey;
    this.failOnError = b.failOnError;
    this.keySelector = b.keySelector == null ? new ContextKeySelector() : b.keySelector;
    this.capacity = b.capacity;
    this.conversationStore = b.conversationStore;
    // Default async-operator timeout: a margin above the client's own deadline so the client
    // produces a proper timeout/error result first; the operator timeout is only a backstop.
    this.asyncTimeoutMs = b.asyncTimeoutMs > 0 ? b.asyncTimeoutMs : spec.requestTimeoutMs() + 10_000L;
  }

  /** Convenience for a step delegating to the given peer with default mapping. */
  public static A2AStep of(RemoteAgentSpec spec) {
    return builder().withSpec(spec).build();
  }

  public String name() {
    return name;
  }

  public RemoteAgentSpec spec() {
    return spec;
  }

  public A2AClientFactory clientFactory() {
    return clientFactory;
  }

  /** AgentEvent data key holding the prompt; null = default resolution (input/result/output/prompt). */
  public String inputKey() {
    return inputKey;
  }

  /** AgentEvent data key the artifact text is written under (default {@code a2a.<name>}). */
  public String outputKey() {
    return outputKey;
  }

  public boolean failOnError() {
    return failOnError;
  }

  public KeySelector<AgentEvent, String> keySelector() {
    return keySelector;
  }

  /** Max in-flight async remote calls per subtask (Async I/O capacity). Default 100. */
  public int capacity() {
    return capacity;
  }

  /** Optional shared store for cross-turn contextId continuity in {@link #applyToStateful}. */
  public ConversationStore conversationStore() {
    return conversationStore;
  }

  /**
   * Operator-level async timeout: the peer client already enforces its own deadline
   * ({@link RemoteAgentSpec#requestTimeoutMs()}), so the Async-I/O timeout sits a margin above it as a
   * backstop — the client should produce a proper timeout/error result first.
   */
  long asyncTimeoutMs() {
    return asyncTimeoutMs;
  }

  /**
   * Wire this step into a stream: keyBy({@code contextId}) → delegate to the remote agent → emit the
   * enriched events. Chain steps by feeding the result into the next transform.
   */
  public SingleOutputStreamOperator<AgentEvent> applyTo(DataStream<AgentEvent> stream) {
    return stream
        .keyBy(keySelector)
        .process(new A2ADelegatingProcessFunction(this))
        .name("a2a-step:" + name)
        .uid("a2a-step-" + name);
  }

  /**
   * Alias for {@link #applyTo}: the blocking remote call runs inside a single keyed operator, so its
   * keyed {@code ValueState} (remote contextId) is naturally correct across turns. Use this when the
   * call latency is acceptable on the operator thread; use {@link #applyToAsync}/{@link
   * #applyToStateful} when a slow peer must not stall the pipeline.
   */
  public SingleOutputStreamOperator<AgentEvent> applyToKeyed(DataStream<AgentEvent> stream) {
    return applyTo(stream);
  }

  /**
   * Non-blocking, <b>stateless</b> wiring: the remote call runs via Flink Async I/O ({@link
   * A2AAsyncFunction}) so a slow peer never stalls the pipeline. No keyed state is held; the remote
   * {@code contextId} only persists across turns if an upstream operator carries it (see {@link
   * #applyToStateful} for cross-turn continuity). Best for fire-and-enrich delegations.
   */
  public SingleOutputStreamOperator<AgentEvent> applyToAsync(DataStream<AgentEvent> stream) {
    return AsyncDataStream.unorderedWait(
            stream,
            new A2AAsyncFunction(this, capacity),
            asyncTimeoutMs(),
            TimeUnit.MILLISECONDS,
            capacity)
        .name("a2a-async:" + name)
        .uid("a2a-async-" + name);
  }

  /**
   * The recommended non-blocking-and-state-correct pattern: keyed pre-step → stateless async call →
   * keyed post-step. The keyed operators mediate per-conversation continuity through the shared
   * {@link ConversationStore} (so it survives the round trip across two distinct operators, across
   * turns, and across checkpoint/restore), while the async operator holds no state and therefore
   * cannot corrupt keyed state. Correlation is by the {@code keyBy} key.
   */
  public SingleOutputStreamOperator<AgentEvent> applyToStateful(DataStream<AgentEvent> stream) {
    SingleOutputStreamOperator<AgentEvent> pre =
        stream
            .keyBy(keySelector)
            .process(new A2APreCallStateFunction(this, conversationStore))
            .name("a2a-pre:" + name)
            .uid("a2a-pre-" + name)
            .returns(AgentEvent.class);
    SingleOutputStreamOperator<AgentEvent> async =
        AsyncDataStream.unorderedWait(
                pre,
                new A2AAsyncFunction(this, capacity),
                asyncTimeoutMs(),
                TimeUnit.MILLISECONDS,
                capacity)
            .name("a2a-async:" + name)
            .uid("a2a-async-" + name);
    return async
        .keyBy(keySelector)
        .process(new A2APostCallStateFunction(this, conversationStore))
        .name("a2a-post:" + name)
        .uid("a2a-post-" + name)
        .returns(AgentEvent.class);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private RemoteAgentSpec spec;
    private A2AClientFactory clientFactory;
    private String inputKey;
    private String outputKey;
    private boolean failOnError = false;
    private KeySelector<AgentEvent, String> keySelector;
    private int capacity = 100;
    private ConversationStore conversationStore;
    private long asyncTimeoutMs = 0; // 0 = derive from requestTimeout + margin

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withSpec(RemoteAgentSpec spec) {
      this.spec = spec;
      return this;
    }

    public Builder withClientFactory(A2AClientFactory factory) {
      this.clientFactory = factory;
      return this;
    }

    public Builder withInputKey(String inputKey) {
      this.inputKey = inputKey;
      return this;
    }

    public Builder withOutputKey(String outputKey) {
      this.outputKey = outputKey;
      return this;
    }

    public Builder withFailOnError(boolean failOnError) {
      this.failOnError = failOnError;
      return this;
    }

    public Builder withKeySelector(KeySelector<AgentEvent, String> keySelector) {
      this.keySelector = keySelector;
      return this;
    }

    /** Max in-flight async remote calls per subtask for {@link #applyToAsync}/{@link #applyToStateful}. */
    public Builder withCapacity(int capacity) {
      this.capacity = Math.max(1, capacity);
      return this;
    }

    /** Shared store for cross-turn contextId continuity in {@link #applyToStateful} (else discovered). */
    public Builder withConversationStore(ConversationStore conversationStore) {
      this.conversationStore = conversationStore;
      return this;
    }

    /** Async-operator timeout backstop (defaults to the peer requestTimeout + 10s margin). */
    public Builder withAsyncTimeout(java.time.Duration timeout) {
      this.asyncTimeoutMs = Math.max(1, timeout.toMillis());
      return this;
    }

    public A2AStep build() {
      return new A2AStep(this);
    }
  }

  @Override
  public String toString() {
    return "A2AStep{name=" + name + ", peer=" + spec.name() + ", outputKey=" + outputKey + '}';
  }
}
