package core

import (
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
)

// Guardrail screens inbound text and/or the outbound reply. A non-empty return value
// blocks the turn (RoutedGraph short-circuits with an ok=false "[blocked]" reply).
type Guardrail interface {
	CheckInput(text string) string  // "" => allow
	CheckOutput(reply string) string
}

// RegexGuardrail blocks when any deny-pattern matches (case-insensitive).
type RegexGuardrail struct {
	patterns     []*regexp.Regexp
	reason       string
	checkOutputs bool
}

// NewRegexGuardrail compiles the deny list (case-insensitive). checkOutputs also screens replies.
func NewRegexGuardrail(deny []string, reason string, checkOutputs bool) *RegexGuardrail {
	pats := make([]*regexp.Regexp, 0, len(deny))
	for _, d := range deny {
		pats = append(pats, regexp.MustCompile("(?i)"+d))
	}
	return &RegexGuardrail{patterns: pats, reason: reason, checkOutputs: checkOutputs}
}

func (g *RegexGuardrail) hit(text string) string {
	for _, p := range g.patterns {
		if p.MatchString(text) {
			return g.reason
		}
	}
	return ""
}

func (g *RegexGuardrail) CheckInput(text string) string { return g.hit(text) }

func (g *RegexGuardrail) CheckOutput(reply string) string {
	if g.checkOutputs {
		return g.hit(reply)
	}
	return ""
}

// AgentListener has the core lifecycle hooks the RoutedGraph fires per turn:
// start → routed → end.
type AgentListener interface {
	OnTurnStart(event Event, ctx *AgentContext)
	OnRouted(path string, ctx *AgentContext)
	OnTurnEnd(result TurnResult, ctx *AgentContext)
}

// Optional hooks — a listener implements only what it cares about; the framework
// fires them via type assertion, so the core AgentListener contract stays small.
type ToolCallListener interface {
	OnToolCallStart(toolID string, ctx *AgentContext)
	OnToolCallEnd(toolID string, result any, ctx *AgentContext)
}
type ErrorListener interface {
	OnError(stage string, err any, ctx *AgentContext)
}
type GuardrailListener interface {
	OnGuardrailBlock(reason string, ctx *AgentContext)
}

// CompositeListener fans every hook out to several listeners.
type CompositeListener struct{ Listeners []AgentListener }

// NewCompositeListener builds a composite over the given listeners.
func NewCompositeListener(listeners ...AgentListener) *CompositeListener {
	return &CompositeListener{Listeners: listeners}
}

func (c *CompositeListener) OnTurnStart(e Event, ctx *AgentContext) {
	for _, l := range c.Listeners {
		l.OnTurnStart(e, ctx)
	}
}
func (c *CompositeListener) OnRouted(p string, ctx *AgentContext) {
	for _, l := range c.Listeners {
		l.OnRouted(p, ctx)
	}
}
func (c *CompositeListener) OnTurnEnd(r TurnResult, ctx *AgentContext) {
	for _, l := range c.Listeners {
		l.OnTurnEnd(r, ctx)
	}
}
func (c *CompositeListener) OnToolCallStart(t string, ctx *AgentContext) {
	for _, l := range c.Listeners {
		if tl, ok := l.(ToolCallListener); ok {
			tl.OnToolCallStart(t, ctx)
		}
	}
}
func (c *CompositeListener) OnToolCallEnd(t string, result any, ctx *AgentContext) {
	for _, l := range c.Listeners {
		if tl, ok := l.(ToolCallListener); ok {
			tl.OnToolCallEnd(t, result, ctx)
		}
	}
}
func (c *CompositeListener) OnError(stage string, err any, ctx *AgentContext) {
	for _, l := range c.Listeners {
		if el, ok := l.(ErrorListener); ok {
			el.OnError(stage, err, ctx)
		}
	}
}
func (c *CompositeListener) OnGuardrailBlock(reason string, ctx *AgentContext) {
	for _, l := range c.Listeners {
		if gl, ok := l.(GuardrailListener); ok {
			gl.OnGuardrailBlock(reason, ctx)
		}
	}
}

// MetricsListener counts turns, per-path dispatches, blocked turns, tool calls, and
// errors (via the core + optional hooks).
type MetricsListener struct {
	mu        sync.Mutex
	Turns     int64
	Blocked   int64
	ToolCalls int64
	Errors    int64
	Paths     map[string]int
}

// NewMetricsListener builds a metrics listener.
func NewMetricsListener() *MetricsListener {
	return &MetricsListener{Paths: map[string]int{}}
}

func (m *MetricsListener) OnTurnStart(event Event, ctx *AgentContext) { atomic.AddInt64(&m.Turns, 1) }

func (m *MetricsListener) OnRouted(path string, ctx *AgentContext) {
	m.mu.Lock()
	m.Paths[path]++
	m.mu.Unlock()
}

func (m *MetricsListener) OnTurnEnd(result TurnResult, ctx *AgentContext) {}

func (m *MetricsListener) OnToolCallStart(toolID string, ctx *AgentContext) {}

func (m *MetricsListener) OnToolCallEnd(toolID string, result any, ctx *AgentContext) {
	atomic.AddInt64(&m.ToolCalls, 1)
}

func (m *MetricsListener) OnGuardrailBlock(reason string, ctx *AgentContext) {
	atomic.AddInt64(&m.Blocked, 1)
}

func (m *MetricsListener) OnError(stage string, err any, ctx *AgentContext) {
	atomic.AddInt64(&m.Errors, 1)
}

// PathCount returns the dispatch count for a path.
func (m *MetricsListener) PathCount(path string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Paths[path]
}

// LoggingListener logs each lifecycle event via the given sink (default: stdout).
type LoggingListener struct {
	Sink func(string)
}

func (l *LoggingListener) emit(s string) {
	if l.Sink != nil {
		l.Sink(s)
	} else {
		fmt.Println(s)
	}
}

func (l *LoggingListener) OnTurnStart(event Event, ctx *AgentContext) {
	l.emit(fmt.Sprintf("[turn-start] conv=%s text=%q", event.ConversationID, event.Text))
}

func (l *LoggingListener) OnRouted(path string, ctx *AgentContext) {
	l.emit(fmt.Sprintf("[routed] conv=%s path=%s", ctx.ConversationID, path))
}

func (l *LoggingListener) OnTurnEnd(result TurnResult, ctx *AgentContext) {
	l.emit(fmt.Sprintf("[turn-end] conv=%s path=%s ok=%v tools=%v",
		result.ConversationID, result.Path, result.OK, result.ToolCalls))
}

func (l *LoggingListener) OnToolCallStart(toolID string, ctx *AgentContext) {
	l.emit(fmt.Sprintf("[tool-call] conv=%s tool=%s", ctx.ConversationID, toolID))
}

func (l *LoggingListener) OnToolCallEnd(toolID string, result any, ctx *AgentContext) {
	l.emit(fmt.Sprintf("[tool-done] conv=%s tool=%s result=%v", ctx.ConversationID, toolID, result))
}

func (l *LoggingListener) OnGuardrailBlock(reason string, ctx *AgentContext) {
	l.emit(fmt.Sprintf("[guardrail-block] conv=%s reason=%q", ctx.ConversationID, reason))
}

func (l *LoggingListener) OnError(stage string, err any, ctx *AgentContext) {
	l.emit(fmt.Sprintf("[error] conv=%s stage=%s error=%v", ctx.ConversationID, stage, err))
}
