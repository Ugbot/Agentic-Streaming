package core

import (
	"reflect"
	"strings"
	"testing"
)

// ids extracts the timer ids in order, for golden comparison.
func ids(timers []Timer) []string {
	out := make([]string, 0, len(timers))
	for _, t := range timers {
		out = append(out, t.ID)
	}
	return out
}

// TestTimerScheduleAdvanceOrder mirrors the Java golden: timers come out ascending by
// FireAt, and a drained deadline does not fire twice.
func TestTimerScheduleAdvanceOrder(t *testing.T) {
	ts := NewInMemoryTimerService()
	ts.Schedule("a", 100, NewEvent("c", "u", "a"))
	ts.Schedule("b", 50, NewEvent("c", "u", "b"))
	ts.Schedule("c", 200, NewEvent("c", "u", "c"))

	if d, ok := ts.NextDeadline(); !ok || d != 50 {
		t.Fatalf("NextDeadline = (%d, %v), want (50, true)", d, ok)
	}

	due := ts.AdvanceTo(150)
	if got := ids(due); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("AdvanceTo(150) ids = %v, want [b a]", got)
	}

	if d, ok := ts.NextDeadline(); !ok || d != 200 {
		t.Fatalf("NextDeadline after fire = (%d, %v), want (200, true)", d, ok)
	}

	if again := ts.AdvanceTo(150); len(again) != 0 {
		t.Fatalf("AdvanceTo(150) again = %v, want empty", ids(again))
	}
}

// TestTimerEqualDeadlinesKeepScheduleOrder asserts equal FireAt values are a stable
// tie-break on schedule order.
func TestTimerEqualDeadlinesKeepScheduleOrder(t *testing.T) {
	ts := NewInMemoryTimerService()
	ts.Schedule("x", 100, NewEvent("c", "u", "x"))
	ts.Schedule("y", 100, NewEvent("c", "u", "y"))
	ts.Schedule("z", 100, NewEvent("c", "u", "z"))

	due := ts.AdvanceTo(100)
	if got := ids(due); !reflect.DeepEqual(got, []string{"x", "y", "z"}) {
		t.Fatalf("equal-deadline order = %v, want [x y z]", got)
	}
}

// TestTimerCancel asserts cancel removes a pending timer, is idempotent on the boolean,
// and prevents the timer from firing.
func TestTimerCancel(t *testing.T) {
	ts := NewInMemoryTimerService()
	ts.Schedule("a", 100, NewEvent("c", "u", "a"))

	if !ts.Cancel("a") {
		t.Fatal("Cancel(a) first time = false, want true")
	}
	if ts.Cancel("a") {
		t.Fatal("Cancel(a) second time = true, want false")
	}
	if due := ts.AdvanceTo(1000); len(due) != 0 {
		t.Fatalf("AdvanceTo(1000) after cancel = %v, want empty", ids(due))
	}
}

// TestTimerReplaceTakesNewScheduleOrder asserts re-scheduling an id replaces it and the
// replacement takes the new schedule order for tie-breaking.
func TestTimerReplaceTakesNewScheduleOrder(t *testing.T) {
	ts := NewInMemoryTimerService()
	ts.Schedule("a", 100, NewEvent("c", "u", "a"))
	ts.Schedule("b", 100, NewEvent("c", "u", "b"))
	// Replace "a" — now scheduled after "b", same deadline.
	ts.Schedule("a", 100, NewEvent("c", "u", "a2"))

	due := ts.AdvanceTo(100)
	if got := ids(due); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("replaced order = %v, want [b a]", got)
	}
	if len(due) != 2 {
		t.Fatalf("want 2 timers (no duplicate), got %d", len(due))
	}
	if due[1].Payload.Text != "a2" {
		t.Fatalf("replaced payload text = %q, want a2", due[1].Payload.Text)
	}
}

// TestDurableTimerSurvivesRestore asserts a scheduled timer round-trips through a
// KeyedStateStore and fires after a fresh service restores from the same store.
func TestDurableTimerSurvivesRestore(t *testing.T) {
	store := NewInMemoryKeyedStateStore()
	d1 := NewDurableTimerService(store)
	d1.Schedule("escalate", 500, NewEvent("c9", "u9", "what is my balance?"))

	// New service over the SAME store; Restore.
	d2 := NewDurableTimerService(store)
	d2.Restore()

	if dl, ok := d2.NextDeadline(); !ok || dl != 500 {
		t.Fatalf("restored NextDeadline = (%d, %v), want (500, true)", dl, ok)
	}

	due := d2.AdvanceTo(500)
	if len(due) != 1 {
		t.Fatalf("restored AdvanceTo(500) = %d timers, want 1", len(due))
	}
	if due[0].Payload.ConversationID != "c9" {
		t.Fatalf("restored payload ConversationID = %q, want c9", due[0].Payload.ConversationID)
	}
	if due[0].Payload.Text != "what is my balance?" {
		t.Fatalf("restored payload Text = %q, want %q", due[0].Payload.Text, "what is my balance?")
	}
}

// TestFireDueTimersThroughStream asserts a due timer re-enters the StreamRuntime as a
// turn: nothing fires before the deadline, and at the deadline the payload routes through
// the banking graph (payments path, balance reply).
func TestFireDueTimersThroughStream(t *testing.T) {
	timers := NewInMemoryTimerService()
	timers.Schedule("sla", 1000, NewEvent("c1", "alice", "what is my balance?"))

	stream := NewStreamRuntime(NewBankingRuntime())

	if early := stream.FireDueTimers(timers, 999); len(early) != 0 {
		t.Fatalf("FireDueTimers(999) = %d results, want 0", len(early))
	}

	results := stream.FireDueTimers(timers, 1000)
	if len(results) != 1 {
		t.Fatalf("FireDueTimers(1000) = %d results, want 1", len(results))
	}
	if results[0].Path != "payments" {
		t.Fatalf("fired timer path = %q, want payments", results[0].Path)
	}
	if !strings.Contains(results[0].Reply, "1234.56") {
		t.Fatalf("fired timer reply missing balance: %q", results[0].Reply)
	}
}
