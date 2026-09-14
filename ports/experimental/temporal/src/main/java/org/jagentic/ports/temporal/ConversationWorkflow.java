package org.jagentic.ports.temporal;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import org.jagentic.ports.temporal.TurnMessages.TurnReply;
import org.jagentic.ports.temporal.TurnMessages.TurnRequest;

/**
 * The per-conversation <b>entity workflow</b> — Temporal's expression of Flink's keyed
 * operator. There is exactly one running execution per {@code workflowId}, and the
 * workflowId <em>is</em> the {@code conversationId}, so Temporal guarantees a single
 * live writer per conversation (C2). The workflow's in-memory state (the
 * ConversationStore) is made durable and fault-tolerant by Temporal's event-sourced
 * history (C1+C3): on worker crash/restart the history replays and rebuilds it.
 *
 * <p>A turn is delivered as a synchronous <b>Update</b> ({@link #turn}); updates are
 * applied serially on the workflow's single thread, in arrival order, and return the
 * verified reply to the caller.
 */
@WorkflowInterface
public interface ConversationWorkflow {

  /** Long-running entry point: the entity lives until {@link #close} is signalled. */
  @WorkflowMethod
  void run();

  /** Process one conversational turn and return its reply (synchronous update). */
  @UpdateMethod
  TurnReply turn(TurnRequest request);

  /** Terminate the entity workflow. */
  @SignalMethod
  void close();

  /** Read the durable transcript size without mutating state. */
  @QueryMethod
  int messageCount();
}
