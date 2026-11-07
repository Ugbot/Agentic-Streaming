package org.jagentic.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/** The rules of {@code record_or_get} (spec section 10), on the store that holds nothing back. */
class EffectJournalTest {
  private static final EffectJournal.Key KEY = new EffectJournal.Key("c", "t", EffectJournal.TOOL, 0);

  private final EffectJournal journal = new EffectJournal.InMemory();
  private final List<Integer> attempts = new ArrayList<>();

  private EffectJournal.Call call(int budget, RuntimeException failure) {
    return new EffectJournal.Call() {
      @Override public void onIntent(EffectJournal.Entry entry) {}

      @Override public void onFailedAttempt(EffectJournal.Attempt failed) {}

      @Override public int budget() {
        return budget;
      }

      @Override public Object attempt(int attempt) {
        attempts.add(attempt);
        if (failure != null) {
          throw failure;
        }
        return "done";
      }
    };
  }

  @Test
  void aRecordedResultIsReusedAndNothingIsCalled() {
    journal.recordOrGet(KEY, "pay:1", ToolEffect.IDEMPOTENT, call(1, null));
    EffectJournal.Entry again = journal.recordOrGet(KEY, "pay:1", ToolEffect.IDEMPOTENT, call(1, null));
    assertEquals("done", again.result());
    assertEquals(List.of(1), attempts);
  }

  @Test
  void aPendingNonIdempotentCallIsUnknownAndNeverCalled() {
    journal.put(EffectJournal.Entry.pending(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT));
    EffectJournal.Entry e = journal.recordOrGet(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT, call(3, null));
    assertEquals(EffectJournal.State.UNKNOWN, e.state());
    assertEquals(List.of(), attempts);
  }

  @Test
  void aPendingIdempotentCallIsMadeAgain() {
    journal.put(EffectJournal.Entry.pending(KEY, "pay:1", ToolEffect.IDEMPOTENT));
    assertEquals(EffectJournal.State.SUCCEEDED,
        journal.recordOrGet(KEY, "pay:1", ToolEffect.IDEMPOTENT, call(1, null)).state());
    assertEquals(List.of(1), attempts);
  }

  @Test
  void aReleasedCallRunsAgainAfterAPersonDecides() {
    journal.put(EffectJournal.Entry.pending(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT).unknown().release());
    assertEquals(EffectJournal.State.SUCCEEDED,
        journal.recordOrGet(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT, call(1, null)).state());
  }

  @Test
  void aPersonsPermissionCoversOneCallEvenIfThatCallCrashes() {
    journal.put(EffectJournal.Entry.pending(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT).unknown().release());
    Error crash = new Error("died during the approved call");
    assertThrows(Error.class, () -> journal.recordOrGet(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT,
        new EffectJournal.Call() {
          @Override public void onIntent(EffectJournal.Entry entry) {}

          @Override public void onFailedAttempt(EffectJournal.Attempt failed) {}

          @Override public int budget() {
            return 1;
          }

          @Override public Object attempt(int attempt) {
            throw crash;
          }
        }));
    assertEquals(EffectJournal.State.UNKNOWN,
        journal.recordOrGet(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT, call(1, null)).state());
    assertEquals(List.of(), attempts);
  }

  @Test
  void aNonIdempotentCallHasOneAttemptWhateverTheBudget() {
    EffectJournal.Entry e = journal.recordOrGet(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT,
        call(3, new IllegalStateException("declined")));
    assertEquals(EffectJournal.State.FAILED, e.state());
    assertEquals(List.of(1), attempts);
  }

  @Test
  void anAnswerThatDidNotArriveOnANonIdempotentCallIsUnknown() {
    EffectJournal.Entry e = journal.recordOrGet(KEY, "pay:1", ToolEffect.NON_IDEMPOTENT,
        call(3, new OutcomeUnknown("timed out")));
    assertEquals(EffectJournal.State.UNKNOWN, e.state());
  }

  @Test
  void aDifferentCallUnderARecordedKeyIsADivergence() {
    journal.recordOrGet(KEY, "pay:1", ToolEffect.IDEMPOTENT, call(1, null));
    assertThrows(EffectJournal.Divergence.class,
        () -> journal.recordOrGet(KEY, "refund:2", ToolEffect.IDEMPOTENT, call(1, null)));
  }

  @Test
  void theKeyIsTheOneTheSpecDefines() {
    assertEquals("4dce4871000d3c792626309090b13fe7", EffectJournal.idempotencyKey("c1", "t1", 0).substring(0, 32));
  }
}
