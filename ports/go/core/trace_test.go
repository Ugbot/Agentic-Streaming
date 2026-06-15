package core

import (
	"reflect"
	"testing"
)

// TestRecordingTracerCapturesSpan mirrors the Java golden: a single span carries its
// name, attributes, and events through to the recorded slice on End().
func TestRecordingTracerCapturesSpan(t *testing.T) {
	tr := NewRecordingTracer()
	tr.Start("demo").Attr("k", "v").Event("hello").End()

	spans := tr.Spans()
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	s := spans[0]
	if s.Name != "demo" {
		t.Fatalf("span name = %q, want %q", s.Name, "demo")
	}
	if s.Attrs["k"] != "v" {
		t.Fatalf("span attr k = %q, want %q", s.Attrs["k"], "v")
	}
	if !reflect.DeepEqual(s.Events, []string{"hello"}) {
		t.Fatalf("span events = %v, want [hello]", s.Events)
	}
	if !reflect.DeepEqual(tr.Names(), []string{"demo"}) {
		t.Fatalf("tracer names = %v, want [demo]", tr.Names())
	}
}

// TestStreamRuntimeTracesTurns asserts that Run spans each turn: one span named "turn"
// with the conversation, routed path, ok flag, and a tool-call event.
func TestStreamRuntimeTracesTurns(t *testing.T) {
	tr := NewRecordingTracer()
	rt := NewStreamRuntime(NewBankingRuntime()).WithTracer(tr)

	rt.Run(SeedChannelOf(NewEvent("c1", "alice", "what is my balance?")))

	if !reflect.DeepEqual(tr.Names(), []string{"turn"}) {
		t.Fatalf("names = %v, want [turn]", tr.Names())
	}
	span := tr.Spans()[0]
	if span.Attrs["conversation"] != "c1" {
		t.Fatalf("conversation = %q, want c1", span.Attrs["conversation"])
	}
	if span.Attrs["path"] != "payments" {
		t.Fatalf("path = %q, want payments", span.Attrs["path"])
	}
	if span.Attrs["ok"] != "true" {
		t.Fatalf("ok = %q, want true", span.Attrs["ok"])
	}
	if !containsString(span.Events, "tool:get_balance") {
		t.Fatalf("events = %v, want to contain tool:get_balance", span.Events)
	}
}

// TestStreamRuntimeTracesTimerFires asserts a due timer re-enters the stream as a traced
// turn under the "timer.fire" span name, with the routed path recorded.
func TestStreamRuntimeTracesTimerFires(t *testing.T) {
	tr := NewRecordingTracer()
	rt := NewStreamRuntime(NewBankingRuntime()).WithTracer(tr)

	timers := NewInMemoryTimerService()
	timers.Schedule("t1", 1000, NewEvent("c1", "", "what is my balance?"))

	rt.FireDueTimers(timers, 1000)

	if !reflect.DeepEqual(tr.Names(), []string{"timer.fire"}) {
		t.Fatalf("names = %v, want [timer.fire]", tr.Names())
	}
	span := tr.Spans()[0]
	if span.Attrs["path"] != "payments" {
		t.Fatalf("path = %q, want payments", span.Attrs["path"])
	}
	if span.Attrs["conversation"] != "c1" {
		t.Fatalf("conversation = %q, want c1", span.Attrs["conversation"])
	}
}

// TestStreamRuntimeNoopTracerDefault asserts a StreamRuntime without WithTracer runs
// unchanged — the no-op tracer is the default and records nothing.
func TestStreamRuntimeNoopTracerDefault(t *testing.T) {
	rt := NewStreamRuntime(NewBankingRuntime())

	results := rt.Run(SeedChannelOf(NewEvent("c1", "alice", "hello there")))

	if len(results) != 1 {
		t.Fatalf("want 1 result, got %d", len(results))
	}
	if results[0].Path != "general" {
		t.Fatalf("path = %q, want general", results[0].Path)
	}
}

// containsString reports whether xs contains target.
func containsString(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}
