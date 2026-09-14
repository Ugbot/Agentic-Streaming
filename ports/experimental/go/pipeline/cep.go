package pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/jagentic/goagentic/core"
)

// compileCep compiles a pipeline's cep: section into runnable core.CepWirings. The schema
// mirrors the kind-dispatch style of tools:/guardrails: and is identical to the
// Python/Java loaders:
//
//	cep:
//	  - name: incident
//	    key: conversation_id            # conversation_id | conversationId | metadata.<field>
//	    ts:  metadata.ts                 # metadata.<field> (else a per-rule arrival counter)
//	    within: 300000
//	    pattern:
//	      - { stage: first,  where: any }
//	      - { stage: second, where: { text_contains: anomaly }, contiguity: followedBy }
//	      - { stage: third,  where: any, contiguity: followedBy }
//	    on_match: { kind: submit, text: "incident on {key}" }   # or { kind: tool, tool: id, args: {...} }
//
// where: any · {text_contains: s|[..]} · {metadata_equals: {k: v}} · {metadata_gt: {k: n}}.
func compileCep(specs []any) ([]*core.CepWiring, error) {
	var wirings []*core.CepWiring
	for _, raw := range specs {
		s := asMap(raw)
		name := asString(s["name"], "cep")
		pattern, err := buildPattern(asList(s["pattern"]), int64(asInt(s["within"], 0)))
		if err != nil {
			return nil, fmt.Errorf("cep %q: %w", name, err)
		}
		keyFn := cepKeyFn(asString(s["key"], "conversation_id"))
		tsFn := cepTsFn(asString(s["ts"], ""))
		action, err := cepAction(asMap(s["on_match"]))
		if err != nil {
			return nil, fmt.Errorf("cep %q: %w", name, err)
		}
		wirings = append(wirings, core.NewCepWiring(name, core.NewCepMatcher(pattern), keyFn, tsFn, action))
	}
	return wirings, nil
}

// buildPattern turns the pattern: stages into a core.Pattern (Begin then Next/FollowedBy by
// contiguity, default followedBy), bounded by within.
func buildPattern(stages []any, within int64) (*core.Pattern, error) {
	if len(stages) == 0 {
		return nil, fmt.Errorf("pattern needs at least one stage")
	}
	var pattern *core.Pattern
	for _, raw := range stages {
		st := asMap(raw)
		stage := asString(st["stage"], "s")
		cond, err := cepCondition(st["where"])
		if err != nil {
			return nil, err
		}
		if pattern == nil {
			pattern = core.Begin(stage, cond)
		} else if strings.EqualFold(asString(st["contiguity"], "followedBy"), "next") {
			pattern = pattern.Next(stage, cond)
		} else {
			pattern = pattern.FollowedBy(stage, cond)
		}
	}
	return pattern.Within(within), nil
}

// cepCondition parses a where mini-language entry into a core.Condition.
func cepCondition(where any) (core.Condition, error) {
	if where == nil {
		return core.AnyCondition(), nil
	}
	if s, ok := where.(string); ok {
		if s == "any" {
			return core.AnyCondition(), nil
		}
		return nil, fmt.Errorf("unknown cep where: %q", s)
	}
	m := asMap(where)
	if v, ok := m["text_contains"]; ok {
		needles := asStringList(v)
		return core.SimpleCondition(func(e core.Event) bool {
			for _, n := range needles {
				if strings.Contains(e.Text, n) {
					return true
				}
			}
			return false
		}), nil
	}
	if v, ok := m["metadata_equals"]; ok {
		kv := asMap(v)
		return core.SimpleCondition(func(e core.Event) bool {
			for k, want := range kv {
				if cepMeta(e, k) != fmt.Sprint(want) {
					return false
				}
			}
			return true
		}), nil
	}
	if v, ok := m["metadata_gt"]; ok {
		kv := asMap(v)
		return core.SimpleCondition(func(e core.Event) bool {
			for k, want := range kv {
				got, err := strconv.ParseFloat(cepMeta(e, k), 64)
				if err != nil || got <= asFloat(want, 0) {
					return false
				}
			}
			return true
		}), nil
	}
	return nil, fmt.Errorf("unknown cep where: %v", where)
}

// cepKeyFn maps key: to an extractor: conversation_id/conversationId -> ConversationID,
// metadata.<field> -> Metadata[field].
func cepKeyFn(key string) func(core.Event) string {
	if field, ok := strings.CutPrefix(key, "metadata."); ok {
		return func(e core.Event) string { return cepMeta(e, field) }
	}
	return func(e core.Event) string { return e.ConversationID }
}

// cepTsFn maps ts: to a long extractor: metadata.<field> -> ParseInt(default 0), else a
// per-rule monotonic arrival counter.
func cepTsFn(ts string) func(core.Event) int64 {
	if field, ok := strings.CutPrefix(ts, "metadata."); ok {
		return func(e core.Event) int64 {
			v, err := strconv.ParseInt(cepMeta(e, field), 10, 64)
			if err != nil {
				return 0
			}
			return v
		}
	}
	var counter int64
	return func(core.Event) int64 { return atomic.AddInt64(&counter, 1) - 1 }
}

// cepAction builds the on_match action. kind submit injects a derived event (recursion
// guarded by CepDerived); kind tool invokes a registered tool with static args.
func cepAction(onMatch map[string]any) (core.CepAction, error) {
	if len(onMatch) == 0 {
		return func(core.Match, string, core.Runtime, *core.ToolRegistry) {}, nil // detect-only
	}
	switch kind := asString(onMatch["kind"], "submit"); kind {
	case "tool":
		toolID := asString(onMatch["tool"], "")
		args := map[string]any{}
		for k, v := range asMap(onMatch["args"]) {
			args[k] = v
		}
		return func(_ core.Match, _ string, _ core.Runtime, tools *core.ToolRegistry) {
			tools.Execute(toolID, args)
		}, nil
	case "submit":
		text := asString(onMatch["text"], "cep match")
		return func(_ core.Match, key string, runtime core.Runtime, _ *core.ToolRegistry) {
			body := strings.ReplaceAll(text, "{key}", key)
			runtime.Submit(core.Event{
				ConversationID: key,
				UserID:         "cep",
				Text:           body,
				Metadata:       map[string]string{core.CepDerived: "true"},
			})
		}, nil
	default:
		return nil, fmt.Errorf("unknown cep on_match kind: %q", kind)
	}
}

// cepMeta reads a metadata field, returning "" when absent.
func cepMeta(e core.Event, field string) string {
	if e.Metadata == nil {
		return ""
	}
	return e.Metadata[field]
}

// asStringList coerces a scalar or list into []string (the text_contains needle list).
func asStringList(v any) []string {
	if l, ok := v.([]any); ok {
		out := make([]string, 0, len(l))
		for _, o := range l {
			out = append(out, fmt.Sprint(o))
		}
		return out
	}
	if v == nil {
		return nil
	}
	return []string{fmt.Sprint(v)}
}
