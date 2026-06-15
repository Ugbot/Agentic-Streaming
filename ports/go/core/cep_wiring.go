package core

// CepDerived marks an event injected by a CEP action, so a CepWiring does not re-match it
// (the recursion guard for submit actions). Peer of jagentic-core's CepWiring.DERIVED.
const CepDerived = "__cep_derived__"

// CepAction is what a CepWiring does on a completed match: inject a derived event through
// the runtime (submit) or invoke a tool. Peer of jagentic-core's CepWiring.Action.
type CepAction func(match Match, key string, runtime Runtime, tools *ToolRegistry)

// CepWiring is one declarative CEP rule wired from a pipeline's cep: section: a CepMatcher
// plus the key/timestamp extractors and the action to fire on a match. OnEvent feeds an
// inbound event to the matcher and fires the action for every completed match. Events the
// action itself produced are tagged (CepDerived) and skipped, so a submit action cannot
// recurse. Peer of jagentic-core's CepWiring.
type CepWiring struct {
	Name    string
	matcher *CepMatcher
	keyFn   func(Event) string
	tsFn    func(Event) int64
	action  CepAction
}

// NewCepWiring builds a wiring from a matcher, key/ts extractors, and an action.
func NewCepWiring(name string, matcher *CepMatcher, keyFn func(Event) string,
	tsFn func(Event) int64, action CepAction) *CepWiring {
	return &CepWiring{Name: name, matcher: matcher, keyFn: keyFn, tsFn: tsFn, action: action}
}

// OnEvent feeds one inbound event to the matcher and fires the action for every completed
// match. Events carrying the CepDerived metadata flag are skipped (recursion guard).
func (w *CepWiring) OnEvent(event Event, runtime Runtime, tools *ToolRegistry) {
	if event.Metadata != nil {
		if _, derived := event.Metadata[CepDerived]; derived {
			return // don't re-match events a CEP action produced
		}
	}
	key := w.keyFn(event)
	ts := w.tsFn(event)
	for _, match := range w.matcher.Match(key, ts, event) {
		if w.action != nil {
			w.action(match, key, runtime, tools)
		}
	}
}
