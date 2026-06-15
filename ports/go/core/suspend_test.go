package core

import (
	"strings"
	"testing"
)

// approvalNeeds is the gate predicate: an event needs approval iff it carries a
// "needs_approval" metadata key.
func approvalNeeds(e Event) bool {
	_, ok := e.Metadata["needs_approval"]
	return ok
}

// approvalEvent is the shared fixture: a balance question tagged for approval on c1.
func approvalEvent() Event {
	return Event{
		ConversationID: "c1",
		UserID:         "alice",
		Text:           "what is my balance?",
		Metadata:       map[string]string{"needs_approval": "true"},
	}
}

// TestGateSuspendsOnApproval: an approval-tagged submit suspends instead of running.
func TestGateSuspendsOnApproval(t *testing.T) {
	susp := NewInMemorySuspensionService()
	gate := NewHumanGate(NewBankingRuntime(), susp, approvalNeeds)

	res := gate.Submit(approvalEvent(), 0)

	if res.Path != "awaiting-approval" {
		t.Fatalf("path = %q, want awaiting-approval", res.Path)
	}
	if res.OK {
		t.Fatal("awaiting-approval result should have OK=false")
	}
	if !susp.IsSuspended("c1") {
		t.Fatal("c1 should be suspended after an approval-tagged submit")
	}
}

// TestResumeApprovedReplaysHeldTurn: approving replays the held turn through the runtime,
// which answers the balance question and clears the suspension.
func TestResumeApprovedReplaysHeldTurn(t *testing.T) {
	susp := NewInMemorySuspensionService()
	gate := NewHumanGate(NewBankingRuntime(), susp, approvalNeeds)
	gate.Submit(approvalEvent(), 0)

	res := gate.Resume("c1", true, 10)

	if res.Path != "payments" {
		t.Fatalf("path = %q, want payments", res.Path)
	}
	if !strings.Contains(res.Reply, "1234.56") {
		t.Fatalf("reply %q missing balance amount", res.Reply)
	}
	if susp.IsSuspended("c1") {
		t.Fatal("c1 should no longer be suspended after resume")
	}
}

// TestResumeDenied: denying reports a denial and clears the suspension without running.
func TestResumeDenied(t *testing.T) {
	susp := NewInMemorySuspensionService()
	gate := NewHumanGate(NewBankingRuntime(), susp, approvalNeeds)
	gate.Submit(approvalEvent(), 0)

	res := gate.Resume("c1", false, 10)

	if res.Path != "denied" {
		t.Fatalf("path = %q, want denied", res.Path)
	}
	if res.OK {
		t.Fatal("denied result should have OK=false")
	}
	if susp.IsSuspended("c1") {
		t.Fatal("c1 should no longer be suspended after a denied resume")
	}
}

// TestNormalSubmitPassesThrough: an event with no approval metadata runs as an ordinary
// turn and never suspends.
func TestNormalSubmitPassesThrough(t *testing.T) {
	susp := NewInMemorySuspensionService()
	gate := NewHumanGate(NewBankingRuntime(), susp, approvalNeeds)

	res := gate.Submit(NewEvent("c1", "alice", "what is my balance?"), 0)

	if res.Path != "payments" {
		t.Fatalf("path = %q, want payments", res.Path)
	}
	if !res.OK {
		t.Fatal("a normal balance turn should have OK=true")
	}
	if susp.IsSuspended("c1") {
		t.Fatal("a normal submit should not suspend the conversation")
	}
}

// TestCheckTimeoutsEscalates: a suspension older than the timeout is escalated and cleared;
// younger ones are left pending.
func TestCheckTimeoutsEscalates(t *testing.T) {
	susp := NewInMemorySuspensionService()
	gate := NewHumanGateWithTimeout(NewBankingRuntime(), susp, approvalNeeds, 1000)
	gate.Submit(approvalEvent(), 0)

	if escalated := gate.CheckTimeouts(500); len(escalated) != 0 {
		t.Fatalf("within-timeout sweep escalated %d, want 0", len(escalated))
	}
	if !susp.IsSuspended("c1") {
		t.Fatal("c1 should still be suspended within the timeout")
	}

	escalated := gate.CheckTimeouts(2000)
	if len(escalated) != 1 {
		t.Fatalf("past-timeout sweep escalated %d, want 1", len(escalated))
	}
	if escalated[0].Path != "escalated" {
		t.Fatalf("path = %q, want escalated", escalated[0].Path)
	}
	if susp.IsSuspended("c1") {
		t.Fatal("c1 should be cleared after escalation")
	}
}

// TestResumeNothingPending: resuming a conversation with no held turn reports it.
func TestResumeNothingPending(t *testing.T) {
	susp := NewInMemorySuspensionService()
	gate := NewHumanGate(NewBankingRuntime(), susp, approvalNeeds)

	res := gate.Resume("c1", true, 0)

	if res.Path != "resume" {
		t.Fatalf("path = %q, want resume", res.Path)
	}
	if res.OK {
		t.Fatal("nothing-pending resume should have OK=false")
	}
}
