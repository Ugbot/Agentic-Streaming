package core

import (
	"strconv"
	"testing"
)

// incidentPattern is the shared three-stage relaxed pattern bounded to 5 minutes, keyed by
// conversation. Mirrors the Java CepMatcher goldens.
func incidentPattern() *Pattern {
	return Begin("first", AnyCondition()).
		FollowedBy("second", AnyCondition()).
		FollowedBy("third", AnyCondition()).
		Within(5 * 60 * 1000)
}

func anomaly(conversationID string) Event {
	return NewEvent(conversationID, "u", "anomaly")
}

func TestCepThreeStageCompletes(t *testing.T) {
	m := NewCepMatcher(incidentPattern())

	if got := m.Match("h1", 0, anomaly("h1")); len(got) != 0 {
		t.Fatalf("first event: expected 0 matches, got %d", len(got))
	}
	if got := m.Match("h1", 60000, anomaly("h1")); len(got) != 0 {
		t.Fatalf("second event: expected 0 matches, got %d", len(got))
	}
	got := m.Match("h1", 120000, anomaly("h1"))
	if len(got) != 1 {
		t.Fatalf("third event: expected 1 match, got %d", len(got))
	}
	if len(got[0].Events) != 3 {
		t.Fatalf("expected match of 3 events, got %d", len(got[0].Events))
	}
	for _, name := range []string{"first", "second", "third"} {
		if _, ok := got[0].Named[name]; !ok {
			t.Fatalf("expected Named to contain %q, got %v", name, got[0].Named)
		}
	}
}

func TestCepKeysIndependent(t *testing.T) {
	m := NewCepMatcher(incidentPattern())

	// h2 only reaches 2 events → no completion.
	m.Match("h2", 0, anomaly("h2"))
	if got := m.Match("h2", 1000, anomaly("h2")); len(got) != 0 {
		t.Fatalf("h2 with 2 events: expected 0 matches, got %d", len(got))
	}

	// h1 reaches 3 → exactly one completion, independent of h2's progress.
	m.Match("h1", 0, anomaly("h1"))
	m.Match("h1", 1000, anomaly("h1"))
	if got := m.Match("h1", 2000, anomaly("h1")); len(got) != 1 {
		t.Fatalf("h1 with 3 events: expected 1 match, got %d", len(got))
	}
}

func TestCepWithinExpiry(t *testing.T) {
	m := NewCepMatcher(incidentPattern())

	m.Match("h1", 0, anomaly("h1"))
	m.Match("h1", 60000, anomaly("h1"))
	// Third event arrives past the 5-minute bound from the first event → expired, no match.
	late := int64(5*60*1000) + 1
	if got := m.Match("h1", late, anomaly("h1")); len(got) != 0 {
		t.Fatalf("late third event: expected 0 matches, got %d", len(got))
	}

	expired := m.FlushExpired("h1", 1_000_000_000)
	if len(expired) < 1 {
		t.Fatalf("FlushExpired: expected >= 1 expired partial, got %d", len(expired))
	}
}

func TestCepStrictNextDrops(t *testing.T) {
	p := Begin("a", SimpleCondition(func(e Event) bool { return e.Text == "a" })).
		Next("b", SimpleCondition(func(e Event) bool { return e.Text == "b" }))
	m := NewCepMatcher(p)

	if got := m.Match("k", 0, NewEvent("k", "u", "a")); len(got) != 0 {
		t.Fatalf("event a: expected 0 matches, got %d", len(got))
	}
	// Intervening non-matching event breaks strict contiguity, dropping the partial.
	if got := m.Match("k", 1, NewEvent("k", "u", "x")); len(got) != 0 {
		t.Fatalf("event x: expected 0 matches, got %d", len(got))
	}
	if got := m.Match("k", 2, NewEvent("k", "u", "b")); len(got) != 0 {
		t.Fatalf("event b: expected 0 matches after strict break, got %d", len(got))
	}
}

func TestCepRelaxedFollowedBy(t *testing.T) {
	p := Begin("a", SimpleCondition(func(e Event) bool { return e.Text == "a" })).
		FollowedBy("b", SimpleCondition(func(e Event) bool { return e.Text == "b" }))
	m := NewCepMatcher(p)

	if got := m.Match("k", 0, NewEvent("k", "u", "a")); len(got) != 0 {
		t.Fatalf("event a: expected 0 matches, got %d", len(got))
	}
	// Relaxed contiguity skips the non-matching event.
	if got := m.Match("k", 1, NewEvent("k", "u", "x")); len(got) != 0 {
		t.Fatalf("event x: expected 0 matches, got %d", len(got))
	}
	if got := m.Match("k", 2, NewEvent("k", "u", "b")); len(got) != 1 {
		t.Fatalf("event b: expected 1 match after relaxed skip, got %d", len(got))
	}
}

func TestCepIterativeCondition(t *testing.T) {
	p := Begin("first", AnyCondition()).
		FollowedBy("second", func(e Event, soFar []Event) bool {
			return e.ConversationID == soFar[0].ConversationID
		})
	m := NewCepMatcher(p)

	if got := m.Match("h", 0, anomaly("h")); len(got) != 0 {
		t.Fatalf("first event: expected 0 matches, got %d", len(got))
	}
	// Second same-host event satisfies the iterative condition.
	if got := m.Match("h", 1, anomaly("h")); len(got) != 1 {
		t.Fatalf("second same-host event: expected 1 match, got %d", len(got))
	}
}

func TestCepObserverFiresOnce(t *testing.T) {
	count := 0
	keyFn := func(e Event) string { return e.ConversationID }
	tsFn := func(e Event) int64 {
		v, _ := strconv.ParseInt(e.Metadata["ts"], 10, 64)
		return v
	}
	obs := NewCepObserver(incidentPattern(), keyFn, tsFn, func(Match) { count++ })

	for _, ts := range []string{"0", "1000", "2000"} {
		e := Event{ConversationID: "h1", UserID: "u", Text: "anomaly", Metadata: map[string]string{"ts": ts}}
		obs(e)
	}

	if count != 1 {
		t.Fatalf("CepObserver: expected onMatch to fire once, fired %d times", count)
	}
}
