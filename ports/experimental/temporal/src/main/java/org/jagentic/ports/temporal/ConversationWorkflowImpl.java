package org.jagentic.ports.temporal;

import io.temporal.workflow.Workflow;

import org.jagentic.core.AgentContext;
import org.jagentic.core.Banking;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.Retrieval;
import org.jagentic.core.RoutedGraph;
import org.jagentic.core.ToolRegistry;
import org.jagentic.core.TurnResult;

import org.jagentic.ports.temporal.TurnMessages.TurnReply;
import org.jagentic.ports.temporal.TurnMessages.TurnRequest;

/**
 * Implementation of the per-conversation entity workflow. The graph/tools/retriever are
 * rebuilt deterministically in the constructor (config, not persisted state); the
 * {@link ConversationStore} is mutated by each {@link #turn} update and is the durable
 * keyed state Temporal recovers by replaying history.
 *
 * <p><b>Why the banking graph runs inside the workflow.</b> Temporal workflow code must
 * be deterministic. The model-free banking graph is: rule-based routing, a constant
 * {@code get_balance} tool, and pure-math hashing-embedder retrieval — no clock, no
 * randomness, no I/O — so it replays identically and needs no Activity. A <em>real</em>
 * LLM/A2A/tool call is non-deterministic and would move into an
 * {@code @ActivityMethod} (its result is recorded in history); the workflow would then
 * orchestrate router → path-activity → verifier. The seam is the {@code graph.handle}
 * call below.
 */
public final class ConversationWorkflowImpl implements ConversationWorkflow {

  private final RoutedGraph graph;
  private final ToolRegistry tools;
  private final Retrieval.TwoTierRetriever retriever;
  private final ConversationStore store = new ConversationStore.InMemory();
  private final KeyedStateStore state = new KeyedStateStore.InMemory();
  // Captured once at construction (on the workflow thread); workflowId == conversationId.
  // Cached so the @QueryMethod doesn't call Workflow.getInfo() off the workflow thread.
  private final String cid = Workflow.getInfo().getWorkflowId();

  private boolean closed = false;

  /** Default: the shared banking essence. Adding a tool/path to the core's {@code
   * Banking} factory propagates here with no change to this class. */
  public ConversationWorkflowImpl() {
    this(Banking.buildGraph(), Banking.defaultTools(), Banking.retriever());
  }

  /** Injectable (used by a worker factory) to run any graph built from the public core
   * abstractions — e.g. an extended graph with a new path + tool. */
  public ConversationWorkflowImpl(RoutedGraph graph, ToolRegistry tools,
                                  Retrieval.TwoTierRetriever retriever) {
    this.graph = graph;
    this.tools = tools;
    this.retriever = retriever;
  }

  @Override
  public void run() {
    Workflow.await(() -> closed);
  }

  @Override
  public TurnReply turn(TurnRequest request) {
    Event event = new Event(cid, request.userId, request.text);
    AgentContext ctx = new AgentContext(cid, request.userId, store, state, tools, retriever);
    // === The engine seam: the portable router->path->verifier graph ===
    TurnResult result = graph.handle(event, ctx);
    return new TurnReply(result.reply, result.path, result.ok, store.messageCount(cid));
  }

  @Override
  public void close() {
    this.closed = true;
  }

  @Override
  public int messageCount() {
    return store.messageCount(cid);
  }
}
