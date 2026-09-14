package core

import "sync"

// Span is a single trace span — attach key/value attributes and named events, then End().
// Attr and Event are chainable so instrumentation reads as one fluent statement. The peer
// of jagentic-core's Span.
type Span interface {
	// Attr records a string-valued attribute and returns the span for chaining.
	Attr(key, value string) Span
	// Event records a named, point-in-time event within the span and returns it for chaining.
	Event(name string) Span
	// End closes the span. After End the span must not be used again.
	End()
}

// noopSpan discards every attribute and event. Stateless, so a single shared instance
// serves all callers at zero cost.
type noopSpan struct{}

func (noopSpan) Attr(string, string) Span { return noopSpanInstance }
func (noopSpan) Event(string) Span        { return noopSpanInstance }
func (noopSpan) End()                      {}

// noopSpanInstance is the shared do-nothing span returned by the noop tracer.
var noopSpanInstance Span = noopSpan{}

// NoopSpan returns the shared no-op span.
func NoopSpan() Span { return noopSpanInstance }

// Tracer is the minimal tracing SPI — a span per turn, timer fire, and CEP match. The
// noop default costs nothing; RecordingTracer captures spans for tests/inspection; an
// OpenTelemetry exporter is an opt-in adapter (the heavy dependency stays out of the
// core). Engine adapters can bridge to their native tracing behind the same SPI.
type Tracer interface {
	// Start begins a span; attributes/events are added via the returned Span, closed with End().
	Start(name string) Span
}

// NoopTracer is the zero-cost default: every span discards its data.
type NoopTracer struct{}

// Start returns the shared no-op span.
func (NoopTracer) Start(string) Span { return noopSpanInstance }

// NoopTracerInstance is the shared, stateless no-op tracer used as the StreamRuntime default.
var NoopTracerInstance Tracer = NoopTracer{}

// NewNoopTracer returns the shared no-op tracer.
func NewNoopTracer() Tracer { return NoopTracerInstance }

// RecordedSpan is a completed span captured by RecordingTracer, in End() order.
type RecordedSpan struct {
	Name   string
	Attrs  map[string]string
	Events []string
}

// RecordingTracer records completed spans in End() order — for tests and local
// inspection. Safe for concurrent use. The peer of jagentic-core's RecordingTracer.
type RecordingTracer struct {
	mu    sync.Mutex
	spans []RecordedSpan
}

// NewRecordingTracer builds an empty RecordingTracer.
func NewRecordingTracer() *RecordingTracer { return &RecordingTracer{} }

// recordingSpan accumulates attrs/events and, on End(), appends a RecordedSpan to its tracer.
type recordingSpan struct {
	tracer *RecordingTracer
	name   string
	attrs  map[string]string
	events []string
}

// Start begins a recording span that captures attributes and events until End().
func (r *RecordingTracer) Start(name string) Span {
	return &recordingSpan{
		tracer: r,
		name:   name,
		attrs:  make(map[string]string),
		events: nil,
	}
}

func (s *recordingSpan) Attr(key, value string) Span {
	s.attrs[key] = value
	return s
}

func (s *recordingSpan) Event(name string) Span {
	s.events = append(s.events, name)
	return s
}

func (s *recordingSpan) End() {
	s.tracer.mu.Lock()
	defer s.tracer.mu.Unlock()
	s.tracer.spans = append(s.tracer.spans, RecordedSpan{
		Name:   s.name,
		Attrs:  s.attrs,
		Events: s.events,
	})
}

// Spans returns a copy of the completed spans in End() order.
func (r *RecordingTracer) Spans() []RecordedSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RecordedSpan, len(r.spans))
	copy(out, r.spans)
	return out
}

// Names returns the span names in End() order.
func (r *RecordingTracer) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, len(r.spans))
	for i, s := range r.spans {
		names[i] = s.Name
	}
	return names
}
