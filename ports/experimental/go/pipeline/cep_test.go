package pipeline

import (
	"os"
	"testing"

	"github.com/jagentic/goagentic/core"
)

const incidentYAML = "../../../examples/pipelines/incident.yaml"

// recordingRuntime is a core.Runtime that records every submitted event so a test can
// assert exactly what a CEP submit action injected (and that the recursion guard holds).
type recordingRuntime struct {
	events []core.Event
}

func (r *recordingRuntime) Submit(e core.Event) core.TurnResult {
	r.events = append(r.events, e)
	return core.TurnResult{ConversationID: e.ConversationID, Path: "rec", OK: true}
}

func anomaly(cid, text, ts string) core.Event {
	return core.Event{ConversationID: cid, UserID: "u", Text: text, Metadata: map[string]string{"ts": ts}}
}

// incidentSpec is the canonical 3x text_contains-anomaly rule, within 5 minutes, keyed by
// conversation_id, timestamped from metadata.ts, submitting a derived "incident on {key}".
func incidentSpec(onMatch map[string]any) []any {
	return []any{
		map[string]any{
			"name":   "host_incident",
			"key":    "conversation_id",
			"ts":     "metadata.ts",
			"within": 300000,
			"pattern": []any{
				map[string]any{"stage": "first", "where": map[string]any{"text_contains": "anomaly"}},
				map[string]any{"stage": "second", "where": map[string]any{"text_contains": "anomaly"}, "contiguity": "followedBy"},
				map[string]any{"stage": "third", "where": map[string]any{"text_contains": "anomaly"}, "contiguity": "followedBy"},
			},
			"on_match": onMatch,
		},
	}
}

// TestCepSubmitFiresOnceAndGuardsRecursion: the third anomaly completes the pattern and
// fires exactly one derived submit; re-feeding that derived event matches nothing.
func TestCepSubmitFiresOnceAndGuardsRecursion(t *testing.T) {
	wirings, err := compileCep(incidentSpec(map[string]any{"kind": "submit", "text": "incident on {key}"}))
	if err != nil {
		t.Fatalf("compileCep: %v", err)
	}
	if len(wirings) != 1 {
		t.Fatalf("expected 1 wiring, got %d", len(wirings))
	}
	rt := &recordingRuntime{}
	w := wirings[0]

	w.OnEvent(anomaly("h1", "anomaly cpu", "0"), rt, nil)
	w.OnEvent(anomaly("h1", "anomaly cpu", "60000"), rt, nil)
	if len(rt.events) != 0 {
		t.Fatalf("after two anomalies expected 0 derived events, got %d: %+v", len(rt.events), rt.events)
	}

	w.OnEvent(anomaly("h1", "anomaly cpu", "120000"), rt, nil)
	if len(rt.events) != 1 {
		t.Fatalf("after third anomaly expected 1 derived event, got %d: %+v", len(rt.events), rt.events)
	}
	derived := rt.events[0]
	if derived.Text != "incident on h1" {
		t.Fatalf("derived text = %q, want %q", derived.Text, "incident on h1")
	}
	if derived.Metadata[core.CepDerived] != "true" {
		t.Fatalf("derived event missing %s tag: %+v", core.CepDerived, derived.Metadata)
	}
	if derived.ConversationID != "h1" || derived.UserID != "cep" {
		t.Fatalf("derived event keying wrong: %+v", derived)
	}

	// Recursion guard: re-feeding the derived event matches nothing.
	w.OnEvent(derived, rt, nil)
	if len(rt.events) != 1 {
		t.Fatalf("recursion guard breached: derived event re-matched, now %d events", len(rt.events))
	}
}

// TestCepToolActionFiresOnce: a kind: tool on_match invokes the registered tool exactly
// once on the completing match, with the static args.
func TestCepToolActionFiresOnce(t *testing.T) {
	wirings, err := compileCep(incidentSpec(map[string]any{
		"kind": "tool", "tool": "open_ticket", "args": map[string]any{"sev": "high"},
	}))
	if err != nil {
		t.Fatalf("compileCep: %v", err)
	}
	var calls []map[string]any
	tools := core.NewToolRegistry().Register("open_ticket", "open a ticket", func(p map[string]any) any {
		calls = append(calls, p)
		return "TICKET-1"
	})
	w := wirings[0]
	rt := &recordingRuntime{}

	w.OnEvent(anomaly("h2", "anomaly disk", "0"), rt, tools)
	w.OnEvent(anomaly("h2", "anomaly disk", "1000"), rt, tools)
	if len(calls) != 0 {
		t.Fatalf("tool fired too early: %d calls", len(calls))
	}
	w.OnEvent(anomaly("h2", "anomaly disk", "2000"), rt, tools)
	if len(calls) != 1 {
		t.Fatalf("expected open_ticket called once, got %d", len(calls))
	}
	if calls[0]["sev"] != "high" {
		t.Fatalf("tool args wrong: %+v", calls[0])
	}
	if len(rt.events) != 0 {
		t.Fatalf("tool action must not submit; got %d submits", len(rt.events))
	}
}

// TestCepConditionMiniLanguage exercises metadata_gt and metadata_equals predicates by
// compiling single-stage patterns and feeding matching / non-matching events.
func TestCepConditionMiniLanguage(t *testing.T) {
	// metadata_gt {score: 0.9}: "0.95" matches, "0.5" does not.
	gt, err := cepCondition(map[string]any{"metadata_gt": map[string]any{"score": 0.9}})
	if err != nil {
		t.Fatalf("metadata_gt compile: %v", err)
	}
	if !gt(core.Event{Metadata: map[string]string{"score": "0.95"}}, nil) {
		t.Fatalf("metadata_gt 0.95 > 0.9 should match")
	}
	if gt(core.Event{Metadata: map[string]string{"score": "0.5"}}, nil) {
		t.Fatalf("metadata_gt 0.5 > 0.9 should not match")
	}

	// metadata_equals {region: eu}: equal matches, different / absent does not.
	eq, err := cepCondition(map[string]any{"metadata_equals": map[string]any{"region": "eu"}})
	if err != nil {
		t.Fatalf("metadata_equals compile: %v", err)
	}
	if !eq(core.Event{Metadata: map[string]string{"region": "eu"}}, nil) {
		t.Fatalf("metadata_equals region=eu should match")
	}
	if eq(core.Event{Metadata: map[string]string{"region": "us"}}, nil) {
		t.Fatalf("metadata_equals region=us should not match eu")
	}
	if eq(core.Event{}, nil) {
		t.Fatalf("metadata_equals on missing metadata should not match")
	}

	// text_contains list: any needle matches.
	tc, err := cepCondition(map[string]any{"text_contains": []any{"anomaly", "alert"}})
	if err != nil {
		t.Fatalf("text_contains compile: %v", err)
	}
	if !tc(core.Event{Text: "cpu alert raised"}, nil) {
		t.Fatalf("text_contains [anomaly,alert] should match 'cpu alert raised'")
	}
	if tc(core.Event{Text: "all healthy"}, nil) {
		t.Fatalf("text_contains should not match 'all healthy'")
	}

	// "any" / nil -> AnyCondition.
	for _, where := range []any{"any", nil} {
		c, err := cepCondition(where)
		if err != nil {
			t.Fatalf("any compile (%v): %v", where, err)
		}
		if !c(core.Event{}, nil) {
			t.Fatalf("AnyCondition should always match (where=%v)", where)
		}
	}
}

// TestIncidentYamlEscalates loads the shared incident.yaml on the local backend, feeds 3
// anomalies for host-7, and asserts the CEP rule injected a derived event that routed the
// conversation onto the escalate path (recorded in the conversation store's path attribute).
func TestIncidentYamlEscalates(t *testing.T) {
	if _, err := os.Stat(incidentYAML); err != nil {
		t.Skipf("shared %s not found: %v", incidentYAML, err)
	}
	sys, err := Load(incidentYAML, "local")
	if err != nil {
		t.Fatalf("load %s: %v", incidentYAML, err)
	}

	for _, ts := range []string{"0", "60000", "120000"} {
		sys.Submit(anomaly("host-7", "anomaly: cpu high", ts))
	}

	store := sys.Store()
	if store == nil {
		t.Fatalf("expected a reachable conversation store on the local backend")
	}
	path, ok := store.GetAttribute("host-7", core.PathAttr)
	if !ok {
		t.Fatalf("no %s attribute for host-7; CEP escalation did not fire", core.PathAttr)
	}
	if path != "escalate" {
		t.Fatalf("host-7 path = %q, want %q (CEP-derived incident should route to escalate)", path, "escalate")
	}
}
