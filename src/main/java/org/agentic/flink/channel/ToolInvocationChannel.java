package org.agentic.flink.channel;

import org.agentic.flink.tools.ToolExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool that, when invoked by the LLM, materializes the request as an element on a
 * {@link Channel} for some downstream operator to consume.
 *
 * <p>Implements both {@link ToolExecutor} (so the LLM can call it through the normal tool-call
 * path) and {@link Channel} (so the job graph can read what was emitted). The choice of
 * **transport** decides how the invocation reaches the consumer:
 *
 * <ul>
 *   <li><b>{@link #sideOutput(String, Class, Function)}</b> — the recommended default for
 *       intra-job consumers. Tool invocations are recorded on a Flink {@link OutputTag}; the
 *       consumer reads them via {@code agentOpStream.getSideOutput(channel.outputTag())}. Cross-
 *       TM safe, exactly-once with checkpoints. The associated {@link #open} returns an
 *       in-process fallback stream that's also populated, so unit tests that don't have an
 *       operator context still work.
 *   <li><b>{@link #via(String, Class, Function, Channel, java.util.function.Consumer)}</b> —
 *       wraps another {@link Channel} (Kafka, Redis, custom). Tool invocations are published
 *       through a user-supplied {@link java.util.function.Consumer}; the consumer reads from
 *       the wrapped channel. Use this when the consumer is a different Flink job or not a
 *       Flink operator at all.
 *   <li><b>{@link #inJvm(String, Class, Function)}</b> — per-task {@link BlockingQueue}. Single
 *       JVM only; loses in-flight items on operator restart. Useful for unit tests and
 *       single-JVM dev.
 * </ul>
 *
 * <p>The agent operator hosting the tool should set the current
 * {@code KeyedProcessFunction.Context} via {@link #currentContext} before invoking the LLM, so
 * the side-output transport can find the context in its thread-local lookup.
 */
public final class ToolInvocationChannel<T> implements Channel<T>, ToolExecutor {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ToolInvocationChannel.class);

  /** Per-thread current operator context, set by the agent operator before tool dispatch. */
  private static final ThreadLocal<EmitContext<?>> CURRENT_CONTEXT = new ThreadLocal<>();

  /** Per-tool-id in-JVM queues, used by side-output fallback and the {@code inJvm} transport. */
  private static final ConcurrentMap<String, BlockingQueue<Object>> IN_JVM_QUEUES =
      new ConcurrentHashMap<>();

  /** Functional interface: {@code (Context, T) -> void} that emits to a Flink side-output. */
  @FunctionalInterface
  public interface EmitContext<T> {
    void emit(OutputTag<T> tag, T value);
  }

  /** Wire format the channel selects between. */
  public enum Transport {
    SIDE_OUTPUT,
    EXTERNAL,
    IN_JVM
  }

  private final Transport transport;
  private final String toolId;
  private final String description;
  private final Class<T> type;
  private final TypeInformation<T> typeInfo;
  private final Function<Map<String, Object>, T> mapper;

  // Side-output transport
  private transient OutputTag<T> outputTag;

  // External transport
  private final Channel<T> wrappedChannel;
  private final java.util.function.Consumer<T> publisher;

  private ToolInvocationChannel(
      Transport transport,
      String toolId,
      String description,
      Class<T> type,
      TypeInformation<T> typeInfo,
      Function<Map<String, Object>, T> mapper,
      Channel<T> wrappedChannel,
      java.util.function.Consumer<T> publisher) {
    this.transport = transport;
    this.toolId = Objects.requireNonNull(toolId, "toolId");
    this.description = description == null ? toolId : description;
    this.type = Objects.requireNonNull(type, "type");
    this.typeInfo = Objects.requireNonNull(typeInfo, "typeInfo");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.wrappedChannel = wrappedChannel;
    this.publisher = publisher;
  }

  /**
   * Build a side-output-transported tool channel.
   *
   * <p>The consumer reads via {@code agentOpStream.getSideOutput(channel.outputTag())}.
   * The {@link #open(StreamExecutionEnvironment)} call returns an in-process fallback stream
   * (drained from the per-tool BlockingQueue) so unit tests still work without an operator
   * context.
   */
  public static <T> ToolInvocationChannel<T> sideOutput(
      String toolId, Class<T> type, Function<Map<String, Object>, T> mapper) {
    return new ToolInvocationChannel<>(
        Transport.SIDE_OUTPUT, toolId, null, type, TypeInformation.of(type), mapper, null, null);
  }

  /** Build a tool channel that publishes through an external {@link Channel} transport. */
  public static <T> ToolInvocationChannel<T> via(
      String toolId,
      Class<T> type,
      Function<Map<String, Object>, T> mapper,
      Channel<T> wrapped,
      java.util.function.Consumer<T> publisher) {
    Objects.requireNonNull(wrapped, "wrapped");
    Objects.requireNonNull(publisher, "publisher");
    return new ToolInvocationChannel<>(
        Transport.EXTERNAL,
        toolId,
        null,
        type,
        TypeInformation.of(type),
        mapper,
        wrapped,
        publisher);
  }

  /** Build an in-JVM BlockingQueue-backed tool channel. Test-only. */
  public static <T> ToolInvocationChannel<T> inJvm(
      String toolId, Class<T> type, Function<Map<String, Object>, T> mapper) {
    return new ToolInvocationChannel<>(
        Transport.IN_JVM, toolId, null, type, TypeInformation.of(type), mapper, null, null);
  }

  /**
   * Set the current operator emit-context for the calling thread. Agent operators call this
   * before invoking the LLM (or any code that might trigger tools), and clear it after.
   */
  public static void setCurrentContext(EmitContext<?> ctx) {
    if (ctx == null) {
      CURRENT_CONTEXT.remove();
    } else {
      CURRENT_CONTEXT.set(ctx);
    }
  }

  /** Returns the per-tool side-output tag (lazy-initialized on first read). */
  public OutputTag<T> outputTag() {
    if (outputTag == null) {
      outputTag = new OutputTag<T>(toolId, typeInfo) {};
    }
    return outputTag;
  }

  @Override
  public CompletableFuture<Object> execute(Map<String, Object> parameters) {
    T value;
    try {
      value = mapper.apply(parameters);
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
    switch (transport) {
      case SIDE_OUTPUT:
        return emitViaSideOutputOrQueue(value);
      case EXTERNAL:
        return emitViaExternal(value);
      case IN_JVM:
        return emitViaQueue(value, "in-jvm");
      default:
        return CompletableFuture.failedFuture(new IllegalStateException("Unknown transport"));
    }
  }

  @Override
  public String getToolId() {
    return toolId;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public DataStream<T> open(StreamExecutionEnvironment env) throws Exception {
    switch (transport) {
      case SIDE_OUTPUT:
      case IN_JVM:
        // Both transports expose a BlockingQueue-backed source; for SIDE_OUTPUT this is the
        // fallback path used when no operator context is available (tests, single-JVM dev).
        return env.addSource(new QueueSource<>(toolId), typeInfo)
            .name("tool-channel[" + toolId + "/" + transport.name().toLowerCase() + "]")
            .setParallelism(1);
      case EXTERNAL:
        return wrappedChannel.open(env);
      default:
        throw new IllegalStateException("Unknown transport");
    }
  }

  @Override
  public TypeInformation<T> elementType() {
    return typeInfo;
  }

  @Override
  public String providerName() {
    return "tool-channel:" + transport.name().toLowerCase();
  }

  public Transport getTransport() {
    return transport;
  }

  // ---------- internals ----------

  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> emitViaSideOutputOrQueue(T value) {
    EmitContext<T> ctx = (EmitContext<T>) CURRENT_CONTEXT.get();
    if (ctx != null) {
      try {
        ctx.emit(outputTag(), value);
        Map<String, Object> result = new HashMap<>();
        result.put("queued", true);
        result.put("transport", "side-output");
        return CompletableFuture.completedFuture(result);
      } catch (Exception e) {
        LOG.warn(
            "Side-output emit failed for tool {}; falling back to in-JVM queue: {}",
            toolId, e.getMessage());
      }
    }
    return emitViaQueue(value, "side-output:fallback");
  }

  private CompletableFuture<Object> emitViaExternal(T value) {
    try {
      publisher.accept(value);
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
    Map<String, Object> result = new HashMap<>();
    result.put("queued", true);
    result.put("transport", "external:" + wrappedChannel.providerName());
    return CompletableFuture.completedFuture(result);
  }

  private CompletableFuture<Object> emitViaQueue(T value, String transportLabel) {
    BlockingQueue<Object> q = IN_JVM_QUEUES.computeIfAbsent(toolId, k -> new LinkedBlockingQueue<>());
    boolean offered = q.offer(value);
    Map<String, Object> result = new HashMap<>();
    result.put("queued", offered);
    result.put("transport", transportLabel);
    return CompletableFuture.completedFuture(result);
  }

  /** Per-tool-id source that drains the shared BlockingQueue. */
  static final class QueueSource<T> implements SourceFunction<T> {
    private static final long serialVersionUID = 1L;
    private final String toolId;
    private volatile boolean running = true;

    QueueSource(String toolId) {
      this.toolId = toolId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run(SourceContext<T> ctx) throws Exception {
      BlockingQueue<Object> q = IN_JVM_QUEUES.computeIfAbsent(toolId, k -> new LinkedBlockingQueue<>());
      while (running) {
        Object item = q.poll(500, TimeUnit.MILLISECONDS);
        if (item != null) {
          synchronized (ctx.getCheckpointLock()) {
            ctx.collect((T) item);
          }
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  /** Test-only helper: empty the in-JVM queue for a given tool id. */
  public static void resetInJvmQueueForTests(String toolId) {
    BlockingQueue<Object> q = IN_JVM_QUEUES.get(toolId);
    if (q != null) q.clear();
  }
}
