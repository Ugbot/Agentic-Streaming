package core

import (
	"reflect"
	"strings"
	"testing"
)

// streamTestEvents is the shared three-event fixture, mirroring the Java goldens:
// a balance question (payments), a crypto cash-back question (cards), and a greeting
// (general), interleaved across two conversations.
func streamTestEvents() []Event {
	return []Event{
		NewEvent("c1", "alice", "what is my balance?"),
		NewEvent("c2", "bob", "tell me about crypto cash-back"),
		NewEvent("c1", "alice", "hello there"),
	}
}

// TestStreamingEqualsRepeatedSubmit asserts that draining a SeedChannel through a
// StreamRuntime yields the same (conversationID, path) sequence as calling Submit on each
// event directly — the substrate adds no behavioral change.
func TestStreamingEqualsRepeatedSubmit(t *testing.T) {
	events := streamTestEvents()

	// Baseline: submit each event individually.
	baseline := NewBankingRuntime()
	var directPaths []string
	var directCids []string
	for _, e := range events {
		res := baseline.Submit(e)
		directPaths = append(directPaths, res.Path)
		directCids = append(directCids, res.ConversationID)
	}

	// Streamed: same events through a SeedChannel + StreamRuntime.
	streamed := NewStreamRuntime(NewBankingRuntime())
	results := streamed.Run(NewSeedChannel(events))

	var streamPaths []string
	var streamCids []string
	for _, res := range results {
		streamPaths = append(streamPaths, res.Path)
		streamCids = append(streamCids, res.ConversationID)
	}

	wantPaths := []string{"payments", "cards", "general"}
	if !reflect.DeepEqual(directPaths, wantPaths) {
		t.Fatalf("baseline paths = %v, want %v", directPaths, wantPaths)
	}
	if !reflect.DeepEqual(streamPaths, wantPaths) {
		t.Fatalf("streamed paths = %v, want %v", streamPaths, wantPaths)
	}
	if !reflect.DeepEqual(streamCids, directCids) {
		t.Fatalf("streamed conversation order %v != direct %v", streamCids, directCids)
	}
	if len(results) != len(events) {
		t.Fatalf("want %d results, got %d", len(events), len(results))
	}
}

// TestStreamObserversSeeEveryEventInOrder asserts an EventObserver is invoked for every
// event in arrival order, before submission.
func TestStreamObserversSeeEveryEventInOrder(t *testing.T) {
	events := streamTestEvents()

	var seen []string
	rt := NewStreamRuntime(NewBankingRuntime()).
		Observe(func(e Event) { seen = append(seen, e.ConversationID) })

	rt.Run(NewSeedChannel(events))

	want := []string{"c1", "c2", "c1"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("observer saw %v, want %v", seen, want)
	}
}

// TestQueueChannelFIFO asserts QueueChannel.Offer is chainable, FIFO, and drives the
// runtime in enqueue order — balance (payments) then crypto cash-back (cards) on c1.
func TestQueueChannelFIFO(t *testing.T) {
	q := NewQueueChannel[Event]().
		Offer(NewEvent("c1", "alice", "what is my balance?")).
		Offer(NewEvent("c1", "alice", "tell me about crypto cash-back"))

	results := NewStreamRuntime(NewBankingRuntime()).Run(q)

	if len(results) != 2 {
		t.Fatalf("want 2 results, got %d", len(results))
	}
	paths := []string{results[0].Path, results[1].Path}
	if !reflect.DeepEqual(paths, []string{"payments", "cards"}) {
		t.Fatalf("queue paths = %v, want [payments cards]", paths)
	}
	if !strings.Contains(results[0].Reply, "1234.56") {
		t.Fatalf("first reply missing balance amount: %q", results[0].Reply)
	}

	// Drained queue keeps reporting ok=false.
	if _, ok := q.Poll(); ok {
		t.Fatal("drained QueueChannel should Poll ok=false")
	}
}

// TestSeedChannelDrainsThenFalseForever guards the bounded-source contract.
func TestSeedChannelDrainsThenFalseForever(t *testing.T) {
	ch := SeedChannelOf(NewEvent("c1", "alice", "hello there"))
	if _, ok := ch.Poll(); !ok {
		t.Fatal("first poll should yield the seeded event")
	}
	for i := 0; i < 3; i++ {
		if _, ok := ch.Poll(); ok {
			t.Fatalf("drained SeedChannel poll %d should be ok=false", i)
		}
	}
}
