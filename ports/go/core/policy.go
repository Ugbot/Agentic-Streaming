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

// AgentListener has lifecycle hooks the RoutedGraph fires per turn: start → routed → end.
type AgentListener interface {
	OnTurnStart(event Event, ctx *AgentContext)
	OnRouted(path string, ctx *AgentContext)
	OnTurnEnd(result TurnResult, ctx *AgentContext)
}

// MetricsListener counts turns, per-path dispatches, blocked turns, and tool calls.
type MetricsListener struct {
	mu        sync.Mutex
	Turns     int64
	Blocked   int64
	ToolCalls int64
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

func (m *MetricsListener) OnTurnEnd(result TurnResult, ctx *AgentContext) {
	if !result.OK {
		atomic.AddInt64(&m.Blocked, 1)
	}
	atomic.AddInt64(&m.ToolCalls, int64(len(result.ToolCalls)))
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
