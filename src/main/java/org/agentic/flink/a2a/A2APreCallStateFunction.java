package org.agentic.flink.a2a;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.agentic.flink.memory.conversation.ConversationStores;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed pre-step of {@link A2AStep#applyToStateful}: before the stateless async A2A call, stamps the
 * conversation's remembered remote {@code contextId} onto the event so it rides through the
 * (state-less) async operator to the peer.
 *
 * <p>Continuity is held in the shared {@link ConversationStore} keyed by the conversation id (the
 * {@code keyBy} key), <b>not</b> in keyed {@code ValueState}: this pre operator and the {@link
 * A2APostCallStateFunction} post operator are distinct operators and therefore do not share keyed
 * state, so the contextId would not survive the round trip in per-operator state. The shared store
 * (in-JVM singleton by default; Redis/Postgres in a cluster) is correct across operators, across
 * turns, and across checkpoint/restore.
 */
public final class A2APreCallStateFunction
    extends KeyedProcessFunction<String, AgentEvent, AgentEvent> {
  private static final long serialVersionUID = 1L;

  private final A2AStep step;
  private final ConversationStore providedStore;
  private transient ConversationStore store;

  public A2APreCallStateFunction(A2AStep step, ConversationStore providedStore) {
    this.step = java.util.Objects.requireNonNull(step, "step");
    this.providedStore = providedStore;
  }

  @Override
  public void open(OpenContext openContext) {
    store = providedStore != null ? providedStore : ConversationStores.discover();
  }

  @Override
  public void processElement(AgentEvent event, Context ctx, Collector<AgentEvent> out) {
    String conversationId = ctx.getCurrentKey();
    store
        .getAttribute(conversationId, A2AStepSupport.contextIdKey(step))
        .ifPresent(cid -> event.putData(A2AStepSupport.contextIdKey(step), cid));
    out.collect(event);
  }
}
