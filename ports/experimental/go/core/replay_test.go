package core

import (
	"reflect"
	"testing"
)

// replayTestEvents is the shared three-event fixture, mirroring stream_test.go and the Java
// goldens: a balance question (payments), a crypto cash-back question (cards), and a greeting
// (general), interleaved across two conversations.
func replayTestEvents() []Event {
	return []Event{
		NewEvent("c1", "alice", "what is my balance?"),
		NewEvent("c2", "bob", "tell me about crypto cash-back"),
		NewEvent("c1", "alice", "hello there"),
	}
}

// pathsOf extracts the routed path from each turn result, in order.
func pathsOf(results []TurnResult) []string {
	paths := make([]string, len(results))
	for i, r := range results {
		paths[i] = r.Path
	}
	return paths
}

// TestEventLogRecordsViaStreamObserver asserts the StreamRuntime observer seam records every
// event in arrival order, and that the per-conversation index is correct.
func TestEventLogRecordsViaStreamObserver(t *testing.T) {
	events := replayTestEvents()
	log := NewInMemoryEventLog()

	// Recording is free: log.Record is passed directly as an EventObserver (method value).
	NewStreamRuntime(NewBankingRuntime()).Observe(log.Record).Run(SeedChannelOf(events...))

	got := log.Events()
	if len(got) != len(events) {
		t.Fatalf("log recorded %d events, want %d", len(got), len(events))
	}
	for i, e := range events {
		if got[i].ConversationID != e.ConversationID || got[i].Text != e.Text {
			t.Fatalf("event %d = {%s,%q}, want {%s,%q}",
				i, got[i].ConversationID, got[i].Text, e.ConversationID, e.Text)
		}
	}

	if n := len(log.EventsFor("c1")); n != 2 {
		t.Fatalf("EventsFor(c1) = %d events, want 2", n)
	}
	if n := len(log.EventsFor("c2")); n != 1 {
		t.Fatalf("EventsFor(c2) = %d events, want 1", n)
	}
}

// TestReplayReproducesOutcomes asserts replaying the recorded log through a fresh banking
// runtime reproduces the same routed paths — determinism.
func TestReplayReproducesOutcomes(t *testing.T) {
	events := replayTestEvents()
	log := NewInMemoryEventLog()
	NewStreamRuntime(NewBankingRuntime()).Observe(log.Record).Run(SeedChannelOf(events...))

	results := Replay(log.Events(), NewBankingRuntime())

	want := []string{"payments", "cards", "general"}
	if got := pathsOf(results); !reflect.DeepEqual(got, want) {
		t.Fatalf("replay paths = %v, want %v", got, want)
	}
}

// TestReplayUntilTimeTravel asserts ReplayUntil stops early — state as-of the first two
// events — yielding only the payments and cards turns.
func TestReplayUntilTimeTravel(t *testing.T) {
	events := replayTestEvents()
	log := NewInMemoryEventLog()
	NewStreamRuntime(NewBankingRuntime()).Observe(log.Record).Run(SeedChannelOf(events...))

	results := ReplayUntil(log.Events(), 2, NewBankingRuntime())

	if len(results) != 2 {
		t.Fatalf("ReplayUntil(2) = %d results, want 2", len(results))
	}
	want := []string{"payments", "cards"}
	if got := pathsOf(results); !reflect.DeepEqual(got, want) {
		t.Fatalf("ReplayUntil paths = %v, want %v", got, want)
	}
}

// TestReplayUntilClampsCount guards the [0, len] clamp on the count argument.
func TestReplayUntilClampsCount(t *testing.T) {
	events := replayTestEvents()

	if got := ReplayUntil(events, -5, NewBankingRuntime()); len(got) != 0 {
		t.Fatalf("ReplayUntil(-5) = %d results, want 0", len(got))
	}
	if got := ReplayUntil(events, 99, NewBankingRuntime()); len(got) != len(events) {
		t.Fatalf("ReplayUntil(99) = %d results, want %d", len(got), len(events))
	}
}
