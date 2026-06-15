package core

import "sync"

// EventLog is the append-only log of inbound events — the source of truth the agent's state
// is a materialized view over. Recording is free via the StreamRuntime observer seam
// (NewStreamRuntime(rt).Observe(log.Record).Run(channel)); Replay re-materializes state by
// replaying it (debug, eval, migration, "what would the new graph version have done"). The Go
// peer of jagentic-core's EventLog.
type EventLog interface {
	// Record appends one event to the log. Its signature matches EventObserver
	// (func(Event)), so a *InMemoryEventLog's Record method value can be passed directly
	// to StreamRuntime.Observe.
	Record(event Event)

	// Events returns all recorded events, in arrival order.
	Events() []Event

	// EventsFor returns recorded events for one conversation, in arrival order.
	EventsFor(conversationID string) []Event
}

// InMemoryEventLog is the process-local default EventLog: a mutex-guarded slice in arrival
// order plus a per-conversation index. Safe for concurrent use.
type InMemoryEventLog struct {
	mu    sync.Mutex
	all   []Event
	byKey map[string][]Event
}

// NewInMemoryEventLog builds an empty InMemoryEventLog.
func NewInMemoryEventLog() *InMemoryEventLog {
	return &InMemoryEventLog{byKey: make(map[string][]Event)}
}

// Record appends the event to the arrival-order log and the per-conversation index.
func (l *InMemoryEventLog) Record(event Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.all = append(l.all, event)
	l.byKey[event.ConversationID] = append(l.byKey[event.ConversationID], event)
}

// Events returns a copy of all recorded events, in arrival order.
func (l *InMemoryEventLog) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, len(l.all))
	copy(out, l.all)
	return out
}

// EventsFor returns a copy of the recorded events for one conversation, in arrival order.
func (l *InMemoryEventLog) EventsFor(conversationID string) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	src := l.byKey[conversationID]
	out := make([]Event, len(src))
	copy(out, src)
	return out
}

// Replay submits each event to runtime in arrival order and returns the turn results.
// Replaying through a fresh runtime over the same graph reproduces the outcomes
// (determinism); replaying through a runtime built on a new graph version answers "what
// would the new prompts/routing have done".
func Replay(events []Event, runtime Runtime) []TurnResult {
	out := make([]TurnResult, 0, len(events))
	for _, e := range events {
		out = append(out, runtime.Submit(e))
	}
	return out
}

// ReplayUntil replays only the first count events — state as-of that point in the log
// (time-travel — the portable form of Datomic as-of / a checkpoint restore). count is
// clamped to [0, len(events)].
func ReplayUntil(events []Event, count int, runtime Runtime) []TurnResult {
	n := count
	if n < 0 {
		n = 0
	}
	if n > len(events) {
		n = len(events)
	}
	return Replay(events[:n], runtime)
}
