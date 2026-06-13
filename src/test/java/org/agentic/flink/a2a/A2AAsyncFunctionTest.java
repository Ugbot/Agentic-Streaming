package org.agentic.flink.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.core.AgentEventType;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.agentic.flink.memory.conversation.InMemoryConversationStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Exercises the non-blocking {@link A2AAsyncFunction} / {@link A2AStep#applyToAsync} /
 * {@link A2AStep#applyToStateful} paths on a Flink minicluster: a slow peer must time out instead of
 * hanging, a failing peer must surface a failure, and the keyed → async → keyed split must keep the
 * remote contextId correct across turns via the shared {@link ConversationStore}.
 */
final class A2AAsyncFunctionTest {

  private static final ConcurrentLinkedQueue<AgentEvent> OUT = new ConcurrentLinkedQueue<>();

  private static A2AStep asyncEchoStep() {
    return A2AStep.builder()
        .withName("aecho")
        .withSpec(
            RemoteAgentSpec.builder()
                .withName("echo-peer")
                .withEndpointUrl("https://peer/a2a")
                .withPollInterval(Duration.ofMillis(1))
                .build())
        .withClientFactory(s -> new FakeA2AClient(s, 1, false))
        .withOutputKey("a2a.aecho")
        .withCapacity(8)
        .build();
  }

  @Test
  @DisplayName("applyToAsync enriches each event with the peer's artifact text (non-blocking)")
  void asyncEnrichesEvents() throws Exception {
    OUT.clear();
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(2, new Configuration());
    List<String> prompts = List.of("plan-a", "plan-b", "plan-c", "plan-d");
    DataStream<AgentEvent> src = env.addSource(new EventSource(prompts));

    asyncEchoStep().applyToAsync(src).addSink(new Collect());
    env.execute("a2a-async-test");

    assertEquals(prompts.size(), OUT.size());
    for (AgentEvent e : OUT) {
      assertEquals("completed", e.getData("a2a.aecho.state"));
      assertTrue(((String) e.getData("a2a.aecho")).startsWith("echo: plan-"));
      assertEquals("aecho", e.getCurrentStage());
    }
  }

  @Test
  @DisplayName("a slow peer triggers timeout() — the event is annotated, the pipeline never hangs")
  void slowPeerTimesOut() throws Exception {
    OUT.clear();
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());

    A2AStep slow =
        A2AStep.builder()
            .withName("slow")
            .withSpec(
                RemoteAgentSpec.builder()
                    .withName("slow-peer")
                    .withEndpointUrl("https://peer/a2a")
                    .withRequestTimeout(Duration.ofSeconds(30))
                    .build())
            .withClientFactory(s -> new SlowClient(s, 5_000)) // 5s call …
            .withAsyncTimeout(Duration.ofMillis(400)) // … but 400ms operator timeout
            .withOutputKey("a2a.slow")
            .build();

    DataStream<AgentEvent> src = env.addSource(new EventSource(List.of("q")));
    slow.applyToAsync(src).addSink(new Collect());
    env.execute("a2a-async-timeout-test");

    assertEquals(1, OUT.size());
    AgentEvent e = OUT.peek();
    assertEquals("timeout", e.getData("a2a.slow.state"));
    assertNotNull(e.getData("a2a.slow.error"));
  }

  @Test
  @DisplayName("failOnError=true turns a failed remote task into a FLOW_FAILED event (async)")
  void failingPeerEmitsFailure() throws Exception {
    OUT.clear();
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());

    A2AStep failing =
        A2AStep.builder()
            .withName("boom")
            .withSpec(
                RemoteAgentSpec.builder()
                    .withName("boom-peer")
                    .withEndpointUrl("https://peer/a2a")
                    .withPollInterval(Duration.ofMillis(1))
                    .build())
            .withClientFactory(s -> new FakeA2AClient(s, 1, true))
            .withFailOnError(true)
            .build();

    DataStream<AgentEvent> src = env.addSource(new EventSource(List.of("x")));
    failing.applyToAsync(src).addSink(new Collect());
    env.execute("a2a-async-fail-test");

    assertEquals(1, OUT.size());
    assertEquals(AgentEventType.FLOW_FAILED, OUT.peek().getEventType());
  }

  @Test
  @DisplayName("applyToStateful keeps the remote contextId correct across turns via the shared store")
  void statefulContinuityAcrossTurns() throws Exception {
    // Use the process-wide shared store: a plain instance would serialize into the job graph and
    // deserialize to a separate copy on the task side, so the test could never observe the write.
    // shared() resolves back to the one JVM singleton on both sides; a unique conversation id keeps
    // this test isolated from any other conversation state in the singleton.
    ConversationStore store = InMemoryConversationStore.shared();
    String conv = "conv-continuity-" + UUID.randomUUID();
    store.clear(conv);

    A2AStep step =
        A2AStep.builder()
            .withName("cont")
            .withSpec(
                RemoteAgentSpec.builder()
                    .withName("echo-peer")
                    .withEndpointUrl("https://peer/a2a")
                    .withPollInterval(Duration.ofMillis(1))
                    .build())
            // The fake reuses a provided contextId and assigns a fresh one only when none is given.
            .withClientFactory(s -> new FakeA2AClient(s, 0, false))
            .withConversationStore(store)
            .withOutputKey("a2a.cont")
            .build();

    String turn1Context = runOneTurn(step, conv, "first turn");
    assertNotNull(turn1Context, "turn 1 must obtain a contextId from the peer");
    // The post-step must have persisted it under the conversation key.
    assertEquals(
        turn1Context, store.getAttribute(conv, "a2a.cont.contextId").orElseThrow());

    String turn2Context = runOneTurn(step, conv, "second turn");
    // Continuity: turn 2's pre-step read the stored contextId, the peer reused it → same id.
    assertEquals(turn1Context, turn2Context,
        "the remote contextId must be reused across turns (continuity via shared store)");
  }

  /** Runs a single event through applyToStateful and returns the contextId stamped on the output. */
  private static String runOneTurn(A2AStep step, String conversationId, String prompt)
      throws Exception {
    OUT.clear();
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(1, new Configuration());
    DataStream<AgentEvent> src = env.addSource(new EventSource(List.of(prompt), conversationId));
    step.applyToStateful(src).addSink(new Collect());
    env.execute("a2a-stateful-turn");
    assertEquals(1, OUT.size());
    Object cid = OUT.peek().getData("a2a.cont.contextId");
    return cid == null ? null : cid.toString();
  }

  static final class EventSource implements SourceFunction<AgentEvent> {
    private static final long serialVersionUID = 1L;
    private final List<String> prompts;
    private final String fixedCorrelationId; // null => per-prompt correlation id
    private volatile boolean running = true;

    EventSource(List<String> prompts) {
      this(prompts, null);
    }

    EventSource(List<String> prompts, String fixedCorrelationId) {
      this.prompts = Collections.unmodifiableList(new ArrayList<>(prompts));
      this.fixedCorrelationId = fixedCorrelationId;
    }

    @Override
    public void run(SourceContext<AgentEvent> ctx) {
      for (String p : prompts) {
        if (!running) {
          return;
        }
        AgentEvent e =
            new AgentEvent(
                UUID.randomUUID().toString(), "user", "local-agent", AgentEventType.FLOW_STARTED);
        e.setCorrelationId(fixedCorrelationId != null ? fixedCorrelationId : "conv-" + p);
        e.putData("result", p);
        ctx.collect(e);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  static final class Collect implements SinkFunction<AgentEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(AgentEvent value, Context context) {
      OUT.add(value);
    }
  }

  /** A peer client whose call blocks for {@code delayMs} — used to trip the async operator timeout. */
  static final class SlowClient implements A2AClient {
    private final RemoteAgentSpec spec;
    private final long delayMs;

    SlowClient(RemoteAgentSpec spec, long delayMs) {
      this.spec = spec;
      this.delayMs = delayMs;
    }

    @Override
    public RemoteAgentSpec spec() {
      return spec;
    }

    @Override
    public A2AAgentCard fetchCard() {
      throw new A2AClientException("not used");
    }

    @Override
    public A2ATask send(A2AMessage message) {
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new A2AClientException("interrupted", e);
      }
      return new A2ATask(
          UUID.randomUUID().toString(), "ctx", A2ATaskState.COMPLETED, "ok",
          List.of(), List.of(), null, 0L, 0L);
    }

    @Override
    public A2ATask getTask(String taskId) {
      return send(null);
    }

    @Override
    public A2ATask cancel(String taskId) {
      throw new A2AClientException("not used");
    }

    @Override
    public A2ATask stream(A2AMessage message, Consumer<A2ATask> onUpdate) {
      A2ATask t = send(message);
      onUpdate.accept(t);
      return t;
    }
  }
}
