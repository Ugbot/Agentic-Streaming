package org.agentic.flink.a2a;

import org.agentic.flink.core.AgentEvent;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.agentic.flink.memory.conversation.ConversationStores;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed post-step of {@link A2AStep#applyToStateful}: after the stateless async A2A call, persists
 * the peer's returned remote {@code contextId} into the shared {@link ConversationStore} keyed by
 * the conversation id, so the next turn's {@link A2APreCallStateFunction} can resume the same remote
 * conversation.
 *
 * <p>See {@link A2APreCallStateFunction} for why continuity lives in the shared store rather than
 * per-operator keyed state. This is the "apply the response to state" half of the
 * keyed → async → keyed split: the async operator stays state-free (so it cannot corrupt keyed
 * state), and all per-conversation continuity is mediated here in a keyed operator.
 */
public final class A2APostCallStateFunction
    extends KeyedProcessFunction<String, AgentEvent, AgentEvent> {
  private static final long serialVersionUID = 1L;

  private final A2AStep step;
  private final ConversationStore providedStore;
  private transient ConversationStore store;

  public A2APostCallStateFunction(A2AStep step, ConversationStore providedStore) {
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
    if (event.getData() != null) {
      Object cid = event.getData().get(A2AStepSupport.contextIdKey(step));
      if (cid != null) {
        store.putAttribute(conversationId, A2AStepSupport.contextIdKey(step), cid.toString());
      }
    }
    out.collect(event);
  }
}
