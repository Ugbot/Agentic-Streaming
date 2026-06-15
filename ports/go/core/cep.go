package core

import "sync"

// Condition is a CEP stage predicate. Simple conditions ignore matchedSoFar; iterative
// conditions inspect the events already matched in this partial (the portable form of
// Flink's SimpleCondition / IterativeCondition) — e.g. "this anomaly is on the same host
// as the first", or an aggregate threshold across the partial match. Peer of
// jagentic-core's Condition.
type Condition func(event Event, matchedSoFar []Event) bool

// SimpleCondition adapts an event-only predicate into a Condition.
func SimpleCondition(pred func(Event) bool) Condition {
	return func(event Event, _ []Event) bool { return pred(event) }
}

// AnyCondition always matches.
func AnyCondition() Condition {
	return func(Event, []Event) bool { return true }
}

// Contiguity is the match strictness on the transition INTO a stage. ContiguityBegin marks
// the first stage; ContiguityNext is strict (the very next event must match, else the
// partial is dropped); ContiguityFollowedBy is relaxed (non-matching events are skipped,
// the partial waits).
type Contiguity int

const (
	// ContiguityBegin marks the first stage of a pattern.
	ContiguityBegin Contiguity = iota
	// ContiguityNext is strict contiguity: the immediately-next event must match.
	ContiguityNext
	// ContiguityFollowedBy is relaxed contiguity: skip non-matching events until one matches.
	ContiguityFollowedBy
)

// Stage is a single named stage of a pattern with its contiguity and condition.
type Stage struct {
	Name       string
	Contiguity Contiguity
	Condition  Condition
}

// Pattern is an ordered list of named stages with a contiguity each, plus an optional
// within time bound on the whole match. The portable counterpart of Flink's
// Pattern.begin(..).next(..).followedBy(..).within(..).
type Pattern struct {
	stages       []Stage
	withinMillis int64 // 0 = unbounded
}

// Begin starts a new pattern with its first (BEGIN) stage.
func Begin(name string, c Condition) *Pattern {
	return &Pattern{stages: []Stage{{Name: name, Contiguity: ContiguityBegin, Condition: c}}}
}

// Next appends a strict-contiguity stage: the immediately-next event must satisfy c.
func (p *Pattern) Next(name string, c Condition) *Pattern {
	p.stages = append(p.stages, Stage{Name: name, Contiguity: ContiguityNext, Condition: c})
	return p
}

// FollowedBy appends a relaxed-contiguity stage: skip non-matching events until one matches.
func (p *Pattern) FollowedBy(name string, c Condition) *Pattern {
	p.stages = append(p.stages, Stage{Name: name, Contiguity: ContiguityFollowedBy, Condition: c})
	return p
}

// Within bounds the whole match to millis from the first matched event (0 = unbounded).
func (p *Pattern) Within(millis int64) *Pattern {
	p.withinMillis = millis
	return p
}

// Stages returns the ordered stages of the pattern.
func (p *Pattern) Stages() []Stage { return p.stages }

// WithinMillis returns the within bound in milliseconds (0 = unbounded).
func (p *Pattern) WithinMillis() int64 { return p.withinMillis }

// Match is a completed pattern match: the matched events in order, plus a name→event map
// (stage name to the event that satisfied it) — the portable form of Flink's
// Map<String, List<Event>> match.
type Match struct {
	Events []Event
	Named  map[string]Event
}

// partial is an in-flight partial match for a key.
type partial struct {
	events  []Event
	stage   int
	startTs int64
}

// CepMatcher is a portable, keyed NFA matcher over an event stream — the cross-engine
// equivalent of Flink CEP. Feed events per key with Match; it advances partial matches and
// emits completed Matches. within is enforced by expiring partials whose first event is
// older than the bound (also exposed via FlushExpired for timer-driven expiry).
//
// Semantics per event (deterministic relaxed/strict, no followedByAny non-determinism):
// existing partials advance one stage if the next stage's condition matches; on a non-match
// a Next (strict) partial is dropped and a FollowedBy (relaxed) partial waits; every event
// may also start a new partial at stage 0. A completed partial is emitted and not reused.
type CepMatcher struct {
	mu      sync.Mutex
	pattern *Pattern
	byKey   map[string][]partial
}

// NewCepMatcher builds a matcher for the given pattern.
func NewCepMatcher(p *Pattern) *CepMatcher {
	return &CepMatcher{pattern: p, byKey: make(map[string][]partial)}
}

// Match feeds one event for key at logical time ts and returns any completed matches.
func (m *CepMatcher) Match(key string, ts int64, event Event) []Match {
	m.mu.Lock()
	defer m.mu.Unlock()

	stages := m.pattern.stages
	within := m.pattern.withinMillis
	last := len(stages) - 1

	partials := m.byKey[key]
	if within > 0 {
		kept := partials[:0]
		for _, p := range partials {
			if ts-p.startTs <= within {
				kept = append(kept, p)
			}
		}
		partials = kept
	}

	var completed []Match
	var survivors []partial

	for _, p := range partials {
		nextStage := p.stage + 1
		stage := stages[nextStage]
		if stage.Condition(event, p.events) {
			advanced := appendEvent(p.events, event)
			if nextStage == last {
				completed = append(completed, toMatch(advanced, stages))
			} else {
				survivors = append(survivors, partial{events: advanced, stage: nextStage, startTs: p.startTs})
			}
		} else if stage.Contiguity == ContiguityFollowedBy {
			survivors = append(survivors, p) // relaxed: skip this event, keep waiting
		}
		// Next (strict) + non-match → drop p
	}

	first := stages[0]
	if first.Condition(event, nil) {
		ev := []Event{event}
		if last == 0 {
			completed = append(completed, toMatch(ev, stages))
		} else {
			survivors = append(survivors, partial{events: ev, stage: 0, startTs: ts})
		}
	}

	m.byKey[key] = survivors
	return completed
}

// FlushExpired removes and returns the matched-events of partials that have exceeded within
// as of now (the portable form of Flink's timed-out partial matches). Empty if unbounded.
func (m *CepMatcher) FlushExpired(key string, now int64) [][]Event {
	m.mu.Lock()
	defer m.mu.Unlock()

	within := m.pattern.withinMillis
	var out [][]Event
	if within <= 0 {
		return out
	}
	partials, ok := m.byKey[key]
	if !ok {
		return out
	}
	var survivors []partial
	for _, p := range partials {
		if now-p.startTs > within {
			out = append(out, p.events)
		} else {
			survivors = append(survivors, p)
		}
	}
	m.byKey[key] = survivors
	return out
}

// appendEvent returns a fresh slice with e appended, never aliasing the input backing array.
func appendEvent(events []Event, e Event) []Event {
	out := make([]Event, len(events)+1)
	copy(out, events)
	out[len(events)] = e
	return out
}

// toMatch builds a Match from matched events and their stages, mapping each stage name to
// the event that satisfied it.
func toMatch(events []Event, stages []Stage) Match {
	named := make(map[string]Event, len(events))
	for i := 0; i < len(events) && i < len(stages); i++ {
		named[stages[i].Name] = events[i]
	}
	return Match{Events: events, Named: named}
}

// NewCepObserver bridges a CepMatcher onto the event stream: it keys + timestamps each
// event, feeds the matcher, and invokes onMatch for every completed match — the portable
// equivalent of routing Flink CEP matches to a PatternProcessFunction. The returned
// EventObserver can be registered on a StreamRuntime via Observe.
func NewCepObserver(p *Pattern, keyFn func(Event) string, tsFn func(Event) int64, onMatch func(Match)) EventObserver {
	matcher := NewCepMatcher(p)
	return func(event Event) {
		for _, match := range matcher.Match(keyFn(event), tsFn(event), event) {
			onMatch(match)
		}
	}
}
