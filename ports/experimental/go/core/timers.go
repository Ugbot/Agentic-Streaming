package core

import (
	"encoding/json"
	"sort"
	"sync"
)

// Timer is a scheduled event: when logical/processing time reaches FireAt, Payload is
// fired back into the runtime as a turn. ID is unique — re-scheduling the same id
// replaces the pending timer.
type Timer struct {
	ID      string
	FireAt  int64
	Payload Event
}

// TimerService is portable timers — the counterpart of Flink's TimerService, a Pekko
// scheduler, or a Temporal timer. Time is logical: callers advance the clock with
// AdvanceTo(now) and get back the due timers (so tests are deterministic); a real-time
// driver simply calls AdvanceTo(now) on a tick. Powers SLAs, escalate-after-N, retries,
// scheduled follow-ups, and CEP "within" expiry. Engine adapters may replace this with a
// native timer service.
type TimerService interface {
	// Schedule (or replace, by id) a timer to fire payload at fireAt. A replaced timer
	// takes the new schedule order.
	Schedule(id string, fireAt int64, payload Event)
	// Cancel a pending timer; returns true if one was removed.
	Cancel(id string) bool
	// AdvanceTo removes and returns all timers due at now (FireAt <= now), ascending by
	// FireAt with schedule order as the stable tie-break.
	AdvanceTo(now int64) []Timer
	// NextDeadline is the earliest pending FireAt, or ok=false if none are pending.
	NextDeadline() (int64, bool)
}

// scheduled pairs a timer with its monotonic insertion sequence so equal deadlines keep
// schedule order as a stable tie-break.
type scheduled struct {
	timer Timer
	seq   int64
}

// InMemoryTimerService is process-local timers backed by a map keyed by id plus a
// monotonic sequence; due timers come out ascending by FireAt, with schedule order as the
// stable tie-break. Goroutine-safe via a mutex.
type InMemoryTimerService struct {
	mu     sync.Mutex
	timers map[string]scheduled
	seq    int64
}

// NewInMemoryTimerService builds an empty in-memory timer service.
func NewInMemoryTimerService() *InMemoryTimerService {
	return &InMemoryTimerService{timers: map[string]scheduled{}}
}

// Schedule inserts or replaces by id; a replaced timer takes a fresh sequence so it sorts
// after timers scheduled before this call (new schedule order).
func (s *InMemoryTimerService) Schedule(id string, fireAt int64, payload Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timers[id] = scheduled{timer: Timer{ID: id, FireAt: fireAt, Payload: payload}, seq: s.seq}
	s.seq++
}

// Cancel removes a pending timer, returning true if one was present.
func (s *InMemoryTimerService) Cancel(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.timers[id]; ok {
		delete(s.timers, id)
		return true
	}
	return false
}

// AdvanceTo removes and returns timers with FireAt <= now, ascending by FireAt then
// schedule order.
func (s *InMemoryTimerService) AdvanceTo(now int64) []Timer {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []scheduled
	for _, sc := range s.timers {
		if sc.timer.FireAt <= now {
			due = append(due, sc)
		}
	}
	sortScheduled(due)
	out := make([]Timer, 0, len(due))
	for _, sc := range due {
		out = append(out, sc.timer)
		delete(s.timers, sc.timer.ID)
	}
	return out
}

// NextDeadline returns the earliest pending FireAt, or ok=false when none are pending.
func (s *InMemoryTimerService) NextDeadline() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.timers) == 0 {
		return 0, false
	}
	var min int64
	first := true
	for _, sc := range s.timers {
		if first || sc.timer.FireAt < min {
			min = sc.timer.FireAt
			first = false
		}
	}
	return min, true
}

// pending returns a snapshot of pending timers in schedule order (for durable persistence).
func (s *InMemoryTimerService) pending() []Timer {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]scheduled, 0, len(s.timers))
	for _, sc := range s.timers {
		all = append(all, sc)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].seq < all[j].seq })
	out := make([]Timer, 0, len(all))
	for _, sc := range all {
		out = append(out, sc.timer)
	}
	return out
}

// restoreAll re-inserts timers, preserving the given schedule order via fresh sequences.
func (s *InMemoryTimerService) restoreAll(restored []Timer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range restored {
		s.timers[t.ID] = scheduled{timer: t, seq: s.seq}
		s.seq++
	}
}

// sortScheduled orders by FireAt ascending, then by insertion sequence (schedule order).
func sortScheduled(xs []scheduled) {
	sort.SliceStable(xs, func(i, j int) bool {
		if xs[i].timer.FireAt != xs[j].timer.FireAt {
			return xs[i].timer.FireAt < xs[j].timer.FireAt
		}
		return xs[i].seq < xs[j].seq
	})
}

// DurableTimerService persists its pending set through a KeyedStateStore, so timers
// survive a restart (with a durable store backing). The pending set is written to one
// scalar slot as JSON; Restore reloads it. Schedule/Cancel/AdvanceTo keep the slot
// current.
//
// The serialization is per-process self-consistent (never read across languages), so each
// core port may encode it idiomatically; behaviour — "a scheduled timer survives restore
// and fires" — is what stays at parity.
type DurableTimerService struct {
	mu       sync.Mutex
	delegate *InMemoryTimerService
	store    KeyedStateStore
	slotKey  string
	slotName string
}

// NewDurableTimerService wraps an in-memory service, persisting to the default slot
// (key "__timers__", name "pending").
func NewDurableTimerService(store KeyedStateStore) *DurableTimerService {
	return NewDurableTimerServiceWithSlot(store, "__timers__", "pending")
}

// NewDurableTimerServiceWithSlot lets callers pick the persistence slot key/name.
func NewDurableTimerServiceWithSlot(store KeyedStateStore, slotKey, slotName string) *DurableTimerService {
	return &DurableTimerService{
		delegate: NewInMemoryTimerService(),
		store:    store,
		slotKey:  slotKey,
		slotName: slotName,
	}
}

// Schedule schedules (or replaces) a timer and persists the pending set.
func (d *DurableTimerService) Schedule(id string, fireAt int64, payload Event) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.delegate.Schedule(id, fireAt, payload)
	d.persist()
}

// Cancel removes a pending timer, persisting if one was removed.
func (d *DurableTimerService) Cancel(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	removed := d.delegate.Cancel(id)
	if removed {
		d.persist()
	}
	return removed
}

// AdvanceTo returns due timers, persisting the pending set if any fired.
func (d *DurableTimerService) AdvanceTo(now int64) []Timer {
	d.mu.Lock()
	defer d.mu.Unlock()
	due := d.delegate.AdvanceTo(now)
	if len(due) > 0 {
		d.persist()
	}
	return due
}

// NextDeadline delegates to the in-memory service.
func (d *DurableTimerService) NextDeadline() (int64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.delegate.NextDeadline()
}

// Restore reloads pending timers from the store into this service (call after a restart).
func (d *DurableTimerService) Restore() {
	d.mu.Lock()
	defer d.mu.Unlock()
	raw, ok := d.store.Get(d.slotKey, d.slotName)
	if !ok || raw == nil {
		return
	}
	s, ok := raw.(string)
	if !ok {
		return
	}
	d.delegate.restoreAll(decodeTimers(s))
}

// persist writes the current pending set to the store slot. Caller holds d.mu.
func (d *DurableTimerService) persist() {
	d.store.Put(d.slotKey, d.slotName, encodeTimers(d.delegate.pending()))
}

// persistedTimer is the JSON wire shape for one timer (per-process; never cross-language).
type persistedTimer struct {
	ID             string `json:"id"`
	FireAt         int64  `json:"fireAt"`
	ConversationID string `json:"conversationID"`
	UserID         string `json:"userID"`
	Text           string `json:"text"`
}

// encodeTimers serializes pending timers to a JSON array string.
func encodeTimers(timers []Timer) string {
	out := make([]persistedTimer, 0, len(timers))
	for _, t := range timers {
		out = append(out, persistedTimer{
			ID:             t.ID,
			FireAt:         t.FireAt,
			ConversationID: t.Payload.ConversationID,
			UserID:         t.Payload.UserID,
			Text:           t.Payload.Text,
		})
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// decodeTimers parses the JSON array string back into Timers (metadata is not persisted).
func decodeTimers(s string) []Timer {
	var in []persistedTimer
	if err := json.Unmarshal([]byte(s), &in); err != nil {
		return nil
	}
	out := make([]Timer, 0, len(in))
	for _, p := range in {
		out = append(out, Timer{
			ID:     p.ID,
			FireAt: p.FireAt,
			Payload: Event{
				ConversationID: p.ConversationID,
				UserID:         p.UserID,
				Text:           p.Text,
			},
		})
	}
	return out
}
