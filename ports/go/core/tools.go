package core

import "sort"

// ToolFunc executes a tool given its parameters and returns a result.
type ToolFunc func(params map[string]any) any

type registeredTool struct {
	description string
	fn          ToolFunc
}

// ToolRegistry is the central tool registry. Register tools by id; Execute invokes one.
type ToolRegistry struct {
	tools map[string]registeredTool
}

// NewToolRegistry builds an empty registry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: map[string]registeredTool{}}
}

// Register adds a tool and returns the registry for chaining.
func (r *ToolRegistry) Register(name, description string, fn ToolFunc) *ToolRegistry {
	r.tools[name] = registeredTool{description: description, fn: fn}
	return r
}

// Has reports whether a tool is registered.
func (r *ToolRegistry) Has(name string) bool {
	_, ok := r.tools[name]
	return ok
}

// Specs returns [{"name","description"}] sorted by name — what an LLM brain shows the
// model so it can pick a tool by name.
func (r *ToolRegistry) Specs() []map[string]string {
	names := make([]string, 0, len(r.tools))
	for n := range r.tools {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]map[string]string, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]string{"name": n, "description": r.tools[n].description})
	}
	return out
}

// Execute runs the named tool; it panics with a clear message if the tool is unknown,
// matching the fail-fast behaviour of the Python/Java cores.
func (r *ToolRegistry) Execute(name string, params map[string]any) any {
	t, ok := r.tools[name]
	if !ok {
		panic("unknown tool: " + name)
	}
	return t.fn(params)
}
