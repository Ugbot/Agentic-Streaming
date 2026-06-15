package core

import "sync"

// Channel is a pull-based event source: Poll returns the next event and an ok flag.
// ok=false means "nothing available right now" — Poll never blocks. This is the seam an
// engine adapter fills: a SeedChannel for a fixed batch, a QueueChannel for a live feed,
// or (later) a Kafka/NATS-backed channel. The peer of jagentic-core's Channel<T>.
type Channel[T any] interface {
	// Poll returns the next event and true, or the zero value and false when none is
	// available now. It must not block.
	Poll() (T, bool)
}

// SeedChannel replays a fixed slice of events in arrival order, then returns ok=false
// forever. Bounded and deterministic — the substrate's stand-in for a finite source.
type SeedChannel[T any] struct {
	events []T
	pos    int
}

// NewSeedChannel builds a SeedChannel over a copy of the given events.
func NewSeedChannel[T any](events []T) *SeedChannel[T] {
	cp := make([]T, len(events))
	copy(cp, events)
	return &SeedChannel[T]{events: cp}
}

// SeedChannelOf is the variadic constructor for a SeedChannel.
func SeedChannelOf[T any](events ...T) *SeedChannel[T] {
	return NewSeedChannel(events)
}

// Poll returns the next seeded event in order, then ok=false once drained.
func (s *SeedChannel[T]) Poll() (T, bool) {
	if s.pos >= len(s.events) {
		var zero T
		return zero, false
	}
	e := s.events[s.pos]
	s.pos++
	return e, true
}

// QueueChannel is an unbounded, goroutine-safe FIFO channel. Offer enqueues (chainable);
// Poll dequeues the oldest event. A producer can keep offering while a StreamRuntime
// drains — the local stand-in for an append-only partition.
type QueueChannel[T any] struct {
	mu    sync.Mutex
	queue []T
}

// NewQueueChannel builds an empty QueueChannel.
func NewQueueChannel[T any]() *QueueChannel[T] { return &QueueChannel[T]{} }

// Offer enqueues an event and returns the channel for chaining.
func (q *QueueChannel[T]) Offer(event T) *QueueChannel[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, event)
	return q
}

// Poll dequeues the oldest event, or returns ok=false when the queue is empty.
func (q *QueueChannel[T]) Poll() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) == 0 {
		var zero T
		return zero, false
	}
	e := q.queue[0]
	q.queue = q.queue[1:]
	return e, true
}

// Runtime is the minimal contract a StreamRuntime drives: process one event as a turn.
// *LocalRuntime satisfies it; NATS/Temporal adapters can supply their own. Defined here
// (rather than in runtime.go) because the stream substrate is the first consumer that
// needs the event source decoupled from the concrete runtime.
type Runtime interface {
	Submit(event Event) TurnResult
}

// EventObserver sees every event before it becomes a turn — the seam for CEP, windowing,
// and tracing added in later phases. It must not mutate the event.
type EventObserver func(Event)

// StreamRuntime drives a Runtime from a Channel: it drains every currently-available
// event, notifies each observer in registration order, then submits the event to the
// underlying Runtime. Per-key (per-conversation) ordering is the Runtime's concern —
// LocalRuntime already serializes by conversation.
type StreamRuntime struct {
	runtime   Runtime
	observers []EventObserver
}

// NewStreamRuntime wraps a Runtime so a Channel of events can drive it.
func NewStreamRuntime(runtime Runtime) *StreamRuntime {
	return &StreamRuntime{runtime: runtime}
}

// Observe registers an event observer and returns the runtime for chaining.
func (s *StreamRuntime) Observe(observer EventObserver) *StreamRuntime {
	s.observers = append(s.observers, observer)
	return s
}

// Run drains the channel: for each available event it calls every observer in order, then
// submits the event, collecting the TurnResults in arrival order. It stops when Poll
// reports ok=false (no event available now).
func (s *StreamRuntime) Run(channel Channel[Event]) []TurnResult {
	var results []TurnResult
	for {
		event, ok := channel.Poll()
		if !ok {
			break
		}
		for _, obs := range s.observers {
			obs(event)
		}
		results = append(results, s.runtime.Submit(event))
	}
	return results
}
