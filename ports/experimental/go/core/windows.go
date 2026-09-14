package core

import "sync"

// WindowState is a numeric aggregate over a set of events: how many, and their
// summed value. It mirrors the JVM core's WindowState record.
type WindowState struct {
	Count int
	Sum   float64
}

// floorDiv mirrors Java's Math.floorDiv: integer division that rounds toward
// negative infinity, so bucket indices stay contiguous for negative timestamps.
func floorDiv(x, y int64) int64 {
	q := x / y
	if (x%y != 0) && ((x < 0) != (y < 0)) {
		q--
	}
	return q
}

// slidingEntry is one (timestamp, value) observation held in a key's window.
type slidingEntry struct {
	ts    int64
	value float64
}

// SlidingWindow is a keyed sliding time-window aggregate — the portable
// VelocityDetector: "how many events (and what summed value) for this key in the
// last windowMillis". Each Add evicts events older than ts-windowMillis and
// returns the current windowed WindowState, so a caller fires when
// Count >= threshold ("5 payments on one account in 60s"). Independent per key.
type SlidingWindow struct {
	mu           sync.Mutex
	windowMillis int64
	byKey        map[string][]slidingEntry
}

// NewSlidingWindow builds a sliding window with the given span in milliseconds.
func NewSlidingWindow(windowMillis int64) *SlidingWindow {
	return &SlidingWindow{windowMillis: windowMillis, byKey: map[string][]slidingEntry{}}
}

// Add records an event for key at ts; returns count + sum within (ts-window, ts].
func (w *SlidingWindow) Add(key string, ts int64, value float64) WindowState {
	w.mu.Lock()
	defer w.mu.Unlock()
	q := append(w.byKey[key], slidingEntry{ts: ts, value: value})
	cutoff := ts - w.windowMillis
	i := 0
	for i < len(q) && q[i].ts <= cutoff {
		i++
	}
	q = q[i:]
	w.byKey[key] = q
	var sum float64
	for _, e := range q {
		sum += e.value
	}
	return WindowState{Count: len(q), Sum: sum}
}

// AddCount is a convenience for a count-only event (value 1.0).
func (w *SlidingWindow) AddCount(key string, ts int64) WindowState {
	return w.Add(key, ts, 1.0)
}

// Bucket is a closed tumbling-window aggregate. Start = index * windowMillis.
type Bucket struct {
	Key   string
	Start int64
	Count int
	Sum   float64
}

// tumblingOpen is the currently-open bucket for one key.
type tumblingOpen struct {
	index int64
	count int
	sum   float64
}

// TumblingWindow is a keyed tumbling (fixed, non-overlapping) time-window
// aggregate. Events accumulate into the bucket floor(ts / windowMillis); when an
// event arrives in a later bucket, the previous bucket closes and is emitted.
// Close flushes the open bucket (end of stream).
type TumblingWindow struct {
	mu           sync.Mutex
	windowMillis int64
	byKey        map[string]*tumblingOpen
}

// NewTumblingWindow builds a tumbling window with the given span in milliseconds.
func NewTumblingWindow(windowMillis int64) *TumblingWindow {
	return &TumblingWindow{windowMillis: windowMillis, byKey: map[string]*tumblingOpen{}}
}

// Add an event; if it falls in a later bucket than the open one, the prior
// bucket closes and is returned with ok=true (and a new bucket starts) — else
// (Bucket{}, false).
func (w *TumblingWindow) Add(key string, ts int64, value float64) (Bucket, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	index := floorDiv(ts, w.windowMillis)
	open := w.byKey[key]
	var emitted Bucket
	ok := false
	if open == nil {
		open = &tumblingOpen{index: index}
		w.byKey[key] = open
	} else if index > open.index {
		emitted = Bucket{Key: key, Start: open.index * w.windowMillis, Count: open.count, Sum: open.sum}
		ok = true
		open.index = index
		open.count = 0
		open.sum = 0
	}
	open.count++
	open.sum += value
	return emitted, ok
}

// AddCount is a convenience for a count-only event (value 1.0).
func (w *TumblingWindow) AddCount(key string, ts int64) (Bucket, bool) {
	return w.Add(key, ts, 1.0)
}

// Close flushes the currently-open bucket for a key (ok=false if none open).
func (w *TumblingWindow) Close(key string) (Bucket, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	open := w.byKey[key]
	if open == nil {
		return Bucket{}, false
	}
	delete(w.byKey, key)
	return Bucket{Key: key, Start: open.index * w.windowMillis, Count: open.count, Sum: open.sum}, true
}

// Session is a closed session-window aggregate spanning [Start, End].
type Session struct {
	Key   string
	Start int64
	End   int64
	Count int
	Sum   float64
}

// sessionOpen is the currently-open session for one key.
type sessionOpen struct {
	start int64
	last  int64
	count int
	sum   float64
}

// SessionWindow is a keyed session window — groups events for a key into sessions
// separated by an inactivity gapMillis. An event arriving more than gapMillis
// after the previous one closes the prior session (emitted) and starts a new one.
// Close flushes the open session.
type SessionWindow struct {
	mu        sync.Mutex
	gapMillis int64
	byKey     map[string]*sessionOpen
}

// NewSessionWindow builds a session window with the given inactivity gap in ms.
func NewSessionWindow(gapMillis int64) *SessionWindow {
	return &SessionWindow{gapMillis: gapMillis, byKey: map[string]*sessionOpen{}}
}

// Add an event; if the open session has been idle longer than gapMillis, the
// prior session closes and is returned with ok=true (and a new one starts) — else
// it extends the open session and returns (Session{}, false).
func (w *SessionWindow) Add(key string, ts int64, value float64) (Session, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	open := w.byKey[key]
	var emitted Session
	ok := false
	if open != nil && ts-open.last > w.gapMillis {
		emitted = Session{Key: key, Start: open.start, End: open.last, Count: open.count, Sum: open.sum}
		ok = true
		open = nil
	}
	if open == nil {
		open = &sessionOpen{start: ts}
		w.byKey[key] = open
	}
	open.last = ts
	open.count++
	open.sum += value
	return emitted, ok
}

// AddCount is a convenience for a count-only event (value 1.0).
func (w *SessionWindow) AddCount(key string, ts int64) (Session, bool) {
	return w.Add(key, ts, 1.0)
}

// Close flushes the currently-open session for a key (ok=false if none open).
func (w *SessionWindow) Close(key string) (Session, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	open := w.byKey[key]
	if open == nil {
		return Session{}, false
	}
	delete(w.byKey, key)
	return Session{Key: key, Start: open.start, End: open.last, Count: open.count, Sum: open.sum}, true
}
