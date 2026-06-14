package core

import "sort"

// Router maps (event, ctx) -> path key.
type Router func(event Event, ctx *AgentContext) string

// Verifier maps (reply, ctx) -> (ok, possibly-annotated reply).
type Verifier func(reply string, ctx *AgentContext) (bool, string)

// Attribute keys the graph persists so a multi-turn conversation stays on its path.
const (
	PhaseAttr = "graph.phase"
	PathAttr  = "graph.path"
)

// RoutedGraph is the canonical topology: classify (router) -> dispatch to a specialized
// path agent -> validate (verifier). The chosen path + phase are persisted to the
// ConversationStore so the next turn can resume — what the Flink BankingAgentGraph does
// with routed keyed state.
type RoutedGraph struct {
	router     Router
	paths      map[string]*Agent
	fallback   string
	verifier   Verifier // may be nil
	guardrails []Guardrail
	listeners  []AgentListener
}

// WithGuardrails attaches input/output guardrails and returns the graph for chaining.
func (g *RoutedGraph) WithGuardrails(guardrails ...Guardrail) *RoutedGraph {
	g.guardrails = append(g.guardrails, guardrails...)
	return g
}

// WithListeners attaches lifecycle listeners and returns the graph for chaining.
func (g *RoutedGraph) WithListeners(listeners ...AgentListener) *RoutedGraph {
	g.listeners = append(g.listeners, listeners...)
	return g
}

// NewRoutedGraph builds a graph. It panics if paths is empty. When the router returns an
// unknown path, the graph falls back to the alphabetically-first declared path
// (deterministic, since Go maps are unordered).
func NewRoutedGraph(router Router, paths map[string]*Agent, verifier Verifier) *RoutedGraph {
	if len(paths) == 0 {
		panic("RoutedGraph requires at least one path")
	}
	keys := make([]string, 0, len(paths))
	for k := range paths {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return &RoutedGraph{router: router, paths: paths, fallback: keys[0], verifier: verifier}
}

// Paths returns the declared path keys (sorted), for inspection.
func (g *RoutedGraph) Paths() []string {
	keys := make([]string, 0, len(g.paths))
	for k := range g.paths {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Handle runs one turn through guardrails -> router -> path -> verifier -> guardrails,
// firing listeners and persisting phase/path.
func (g *RoutedGraph) Handle(event Event, ctx *AgentContext) TurnResult {
	cid := event.ConversationID
	ctx.Listeners = g.listeners // so CallTool can fire tool-call hooks
	for _, l := range g.listeners {
		l.OnTurnStart(event, ctx)
	}

	// Input guardrails: short-circuit a blocked turn before routing.
	for _, gr := range g.guardrails {
		if reason := gr.CheckInput(event.Text); reason != "" {
			ctx.Store.PutAttribute(cid, PhaseAttr, "blocked")
			blocked := TurnResult{ConversationID: cid, Reply: "[blocked] " + reason, Path: "blocked", OK: false}
			for _, l := range g.listeners {
				if gl, ok := l.(GuardrailListener); ok {
					gl.OnGuardrailBlock(reason, ctx)
				}
				l.OnTurnEnd(blocked, ctx)
			}
			return blocked
		}
	}

	ctx.Store.PutAttribute(cid, PhaseAttr, "router")
	path := g.router(event, ctx)
	if _, ok := g.paths[path]; !ok {
		path = g.fallback
	}
	ctx.Store.PutAttribute(cid, PathAttr, path)
	ctx.Store.PutAttribute(cid, PhaseAttr, "path:"+path)
	for _, l := range g.listeners {
		l.OnRouted(path, ctx)
	}

	result := g.paths[path].Turn(event, ctx)
	result.Path = path

	if g.verifier != nil {
		ctx.Store.PutAttribute(cid, PhaseAttr, "verifier")
		ok, annotated := g.verifier(result.Reply, ctx)
		result.OK = ok
		result.Reply = annotated
	} else {
		result.OK = true
	}

	// Output guardrails: redact/replace a disallowed reply.
	for _, gr := range g.guardrails {
		if reason := gr.CheckOutput(result.Reply); reason != "" {
			result.Reply = "[blocked] " + reason
			result.OK = false
			for _, l := range g.listeners {
				if gl, ok := l.(GuardrailListener); ok {
					gl.OnGuardrailBlock(reason, ctx)
				}
			}
		}
	}

	ctx.Store.PutAttribute(cid, PhaseAttr, "done")
	for _, l := range g.listeners {
		l.OnTurnEnd(result, ctx)
	}
	return result
}
