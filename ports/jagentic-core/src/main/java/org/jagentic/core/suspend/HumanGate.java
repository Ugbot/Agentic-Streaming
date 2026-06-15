package org.jagentic.core.suspend;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.jagentic.core.Event;
import org.jagentic.core.Runtime;
import org.jagentic.core.TurnResult;

/**
 * A human-in-the-loop gate around a {@link Runtime}. A turn that {@code needsApproval} suspends
 * instead of completing; a later {@link #resume} (CQRS: resume is just another command) replays the
 * held turn (approved) or denies it. {@link #checkTimeouts} escalates suspensions that age past the
 * timeout — the portable pattern for "approve within N minutes or escalate", composing with the
 * Phase-2 timer service.
 */
public final class HumanGate {

  private final Runtime runtime;
  private final SuspensionService suspensions;
  private final Predicate<Event> needsApproval;
  private final long timeoutMillis; // 0 = no timeout

  public HumanGate(Runtime runtime, SuspensionService suspensions, Predicate<Event> needsApproval) {
    this(runtime, suspensions, needsApproval, 0);
  }

  public HumanGate(Runtime runtime, SuspensionService suspensions, Predicate<Event> needsApproval,
                   long timeoutMillis) {
    this.runtime = runtime;
    this.suspensions = suspensions;
    this.needsApproval = needsApproval;
    this.timeoutMillis = timeoutMillis;
  }

  /** Submit through the gate. Suspends (awaiting approval) when required; otherwise a normal turn. */
  public TurnResult submit(Event event, long now) {
    String cid = event.conversationId();
    if (suspensions.isSuspended(cid)) {
      return result(cid, "[awaiting-approval] a turn is already pending approval", "awaiting-approval", false);
    }
    if (needsApproval.test(event)) {
      suspensions.suspend(cid, "approval required: " + event.text(), event.text(), now);
      return result(cid, "[awaiting-approval] " + event.text(), "awaiting-approval", false);
    }
    return runtime.submit(event);
  }

  /** Resume a suspended conversation: approved → replay the held turn; denied → report. */
  public TurnResult resume(String conversationId, boolean approved, long now) {
    var pending = suspensions.clear(conversationId);
    if (pending.isEmpty()) {
      return result(conversationId, "[resume] nothing pending", "resume", false);
    }
    if (!approved) {
      return result(conversationId, "[denied] " + pending.get().reason(), "denied", false);
    }
    // Replay the held turn as a fresh (metadata-free, so it won't re-trigger the gate) event.
    return runtime.submit(new Event(conversationId, "system", pending.get().pendingText()));
  }

  /** Escalate (and clear) suspensions older than the timeout; one result per escalated conversation. */
  public List<TurnResult> checkTimeouts(long now) {
    List<TurnResult> out = new ArrayList<>();
    if (timeoutMillis <= 0) {
      return out;
    }
    for (Suspension s : suspensions.allPending()) {
      if (now - s.since() > timeoutMillis) {
        suspensions.clear(s.conversationId());
        out.add(result(s.conversationId(), "[escalated] approval timed out: " + s.reason(), "escalated", false));
      }
    }
    return out;
  }

  private static TurnResult result(String cid, String reply, String path, boolean ok) {
    TurnResult r = new TurnResult(cid, reply, List.of());
    r.path = path;
    r.ok = ok;
    return r;
  }
}
