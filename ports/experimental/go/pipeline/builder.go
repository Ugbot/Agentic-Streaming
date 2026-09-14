// Package pipeline compiles a declarative spec (parsed from pipeline.yaml) into the
// engine-agnostic core.RoutedGraph + tools + retriever, and wires it onto a backend —
// the Go peer of pyagentic.builder / org.jagentic.core.pipeline. The same YAML schema
// builds the agentic system in every language.
package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jagentic/goagentic/core"
	"github.com/jagentic/goagentic/stores"
)

// embedFunc turns text into a query vector. Default is the deterministic FNV hashing
// embedder; an embeddings: section swaps in a real Embedder.
type embedFunc func(text string) []float64

// keywordBrain: fire a tool on a trigger keyword, else answer from retrieval, else echo.
type keywordBrain struct {
	name         string
	embed        embedFunc
	toolTriggers map[string]string
	threshold    float64
}

func (b keywordBrain) Turn(userText string, ctx *core.AgentContext) string {
	low := strings.ToLower(userText)
	for kw, tool := range b.toolTriggers {
		if strings.Contains(low, strings.ToLower(kw)) {
			r := ctx.CallTool(tool, map[string]any{"user": ctx.UserID})
			return fmt.Sprintf("[%s] %s returned %v", b.name, tool, r)
		}
	}
	if ctx.Retriever != nil {
		hits := ctx.Retriever.Retrieve(b.embed(userText), 1)
		if len(hits) > 0 && hits[0].Score > b.threshold {
			return fmt.Sprintf("[%s] %s", b.name, hits[0].Text)
		}
	}
	return fmt.Sprintf("[%s] I can help with %s questions. You said: %q", b.name, b.name, userText)
}

// Built is what a build produces.
type Built struct {
	Graph     *core.RoutedGraph
	Tools     *core.ToolRegistry
	Retriever *core.TwoTierRetriever
}

// ChatClientFactory supplies a ChatClient for an llm: spec.
type ChatClientFactory func(llm map[string]any) core.ChatClient

// Build compiles spec into a Built (graph + tools + retriever).
func Build(spec map[string]any, chatClientFactory ChatClientFactory) (Built, error) {
	agent := asMap(spec["agent"])
	pathSpecs := asMap(agent["paths"])
	if len(pathSpecs) == 0 {
		return Built{}, fmt.Errorf("pipeline spec needs agent.paths")
	}

	tools, err := buildTools(asList(spec["tools"]))
	if err != nil {
		return Built{}, err
	}
	if err := registerMcp(tools, asList(spec["mcp"])); err != nil {
		return Built{}, err
	}
	registerA2A(tools, asList(spec["a2a"]))

	embed, dim := buildEmbedder(spec["embeddings"], asMap(spec["retrieval"]))
	retriever := buildRetriever(asMap(spec["retrieval"]), embed, dim)

	// Context-window management: bound the replayed LLM transcript to a token budget.
	var ctxMgr *core.ContextWindowManager
	if cspec := asMap(spec["context"]); len(cspec) > 0 {
		budget := asInt(cspec["max_tokens"], asInt(cspec["max_items"], 12)*64)
		ctxMgr = core.NewContextWindowManager(budget)
	}

	// Skills: a path's `skills:` expand into extra tools + an appended prompt fragment.
	var skillSpecs []map[string]any
	for _, s := range asList(spec["skills"]) {
		skillSpecs = append(skillSpecs, asMap(s))
	}
	skills := core.SkillRegistryFromSpecs(skillSpecs)

	paths := map[string]*core.Agent{}
	for name, raw := range pathSpecs {
		ps := asMap(raw)
		prompt := asString(ps["prompt"], "You answer "+name+" questions.")
		var skillNames []string
		for _, s := range asList(ps["skills"]) {
			skillNames = append(skillNames, fmt.Sprint(s))
		}
		skillTools, fragment, _ := skills.Expand(skillNames)
		if fragment != "" {
			prompt = prompt + "\n" + fragment
		}
		brainKind := asString(ps["brain"], "rule")
		var brain core.Brain
		switch brainKind {
		case "llm":
			if chatClientFactory == nil {
				return Built{}, fmt.Errorf("spec uses an llm brain but no ChatClientFactory was provided")
			}
			client := chatClientFactory(asMap(spec["llm"]))
			var allowed []string
			for _, t := range asList(ps["tools"]) {
				allowed = append(allowed, fmt.Sprint(t))
			}
			allowed = append(allowed, skillTools...)
			lb := core.NewLlmBrain(client, name, prompt, allowed, asInt(ps["max_iterations"], 6))
			if osch := asMap(ps["output_schema"]); len(osch) > 0 {
				lb.WithOutputSchema(osch)
			}
			if ctxMgr != nil {
				lb.WithContextManager(ctxMgr)
			}
			brain = lb
		case "rule":
			triggers := map[string]string{}
			for k, v := range asMap(ps["tool_triggers"]) {
				triggers[k] = fmt.Sprint(v)
			}
			brain = keywordBrain{name: name, embed: embed, toolTriggers: triggers, threshold: asFloat(ps["threshold"], 0.15)}
		default:
			return Built{}, fmt.Errorf("unknown brain kind %q for path %q", brainKind, name)
		}
		paths[name] = core.NewAgent(name, prompt, brain)
	}

	router := buildRouter(asMap(agent["router"]), paths)

	var verifier core.Verifier
	vspec := asMap(agent["verifier"])
	if asString(vspec["kind"], "prefix") == "prefix" {
		verifier = func(reply string, ctx *core.AgentContext) (bool, string) {
			return strings.HasPrefix(reply, "["), reply
		}
	}

	graph := core.NewRoutedGraph(router, paths, verifier)
	for _, g := range asList(spec["guardrails"]) {
		gr, err := buildGuardrail(asMap(g))
		if err != nil {
			return Built{}, err
		}
		graph.WithGuardrails(gr)
	}

	return Built{Graph: graph, Tools: tools, Retriever: retriever}, nil
}

// buildGuardrail builds one guardrail from its spec. kind = regex (default) | classifier.
func buildGuardrail(gm map[string]any) (core.Guardrail, error) {
	switch kind := asString(gm["kind"], "regex"); kind {
	case "regex":
		var deny []string
		for _, d := range asList(gm["deny"]) {
			deny = append(deny, fmt.Sprint(d))
		}
		return core.NewRegexGuardrail(deny, asString(gm["reason"], "blocked by policy"),
			asBool(gm["check_outputs"])), nil
	case "classifier":
		var blocked []string
		for _, b := range asList(gm["blocked"]) {
			blocked = append(blocked, fmt.Sprint(b))
		}
		threshold := asFloat(gm["threshold"], 0.5)
		reason := asString(gm["reason"], "blocked by classifier policy")
		checkOutputs := asBool(gm["check_outputs"])
		switch ctype := asString(gm["classifier"], "lexicon"); ctype {
		case "lexicon":
			lexicon := map[string][]string{}
			for label, raw := range asMap(gm["lexicon"]) {
				var words []string
				for _, w := range asList(raw) {
					words = append(words, fmt.Sprint(w))
				}
				lexicon[label] = words
			}
			clf := core.NewLexiconClassifier(lexicon, asString(gm["default_label"], "other"))
			return core.NewClassifierGuardrail(clf, blocked, threshold, reason, checkOutputs), nil
		case "embedding":
			examples := map[string][]string{}
			for label, raw := range asMap(gm["examples"]) {
				var texts []string
				for _, t := range asList(raw) {
					texts = append(texts, fmt.Sprint(t))
				}
				examples[label] = texts
			}
			clf, err := core.NewEmbeddingClassifier(nil, 0).Fit(examples)
			if err != nil {
				return nil, fmt.Errorf("embedding guardrail fit: %w", err)
			}
			return core.NewClassifierGuardrail(clf, blocked, threshold, reason, checkOutputs), nil
		default:
			return nil, fmt.Errorf("unknown classifier %q; choose lexicon|embedding", ctype)
		}
	default:
		return nil, fmt.Errorf("unknown guardrail kind %q; choose regex|classifier", kind)
	}
}

func buildTools(specs []any) (*core.ToolRegistry, error) {
	reg := core.NewToolRegistry()
	for _, raw := range specs {
		t := asMap(raw)
		id := asString(t["id"], "")
		kind := asString(t["kind"], "constant")
		desc := asString(t["description"], id)
		switch kind {
		case "constant":
			value := t["value"]
			reg.Register(id, desc, func(p map[string]any) any { return value })
		case "http", "agent": // "agent" = call another agent/gateway's /agent (A2A-as-tool)
			url := resolveEnv(asString(t["url"], ""))
			reg.Register(id, desc, httpTool(url))
		default:
			return nil, fmt.Errorf("unknown tool kind %q for %q", kind, id)
		}
	}
	return reg, nil
}

func httpTool(url string) core.ToolFunc {
	client := &http.Client{Timeout: 30 * time.Second}
	return func(params map[string]any) any {
		body, _ := json.Marshal(params)
		resp, err := client.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			return map[string]any{"error": err.Error()}
		}
		defer resp.Body.Close()
		var out any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out
	}
}

// buildEmbedder resolves the embed function + dim. An embeddings: section picks a real
// provider via the Embedder SPI; otherwise the deterministic FNV hashing embedder is used
// at the retrieval dim (default 256). Mirrors Python _build_embedder.
func buildEmbedder(embSpec any, retrieval map[string]any) (embedFunc, int) {
	if em := asMap(embSpec); len(em) > 0 {
		provider := asString(em["provider"], asString(em["kind"], "hashing"))
		if provider != "" && provider != "hashing" && provider != "memory" {
			if embedder, err := core.NewEmbedder(em); err == nil {
				return embedder.Embed, embedder.Dim()
			}
			// fall through to hashing on provider error
		}
	}
	dim := asInt(retrieval["dim"], 256)
	return func(text string) []float64 { return core.Embed(text, dim) }, dim
}

// buildRetriever builds the two-tier retriever. The hot tier is always an in-memory window
// seeded with the kb; a vector_store: section adds a real cold tier (memory/hnsw/qdrant),
// also seeded with the kb so cold recall works. Mirrors Python _build_retriever.
func buildRetriever(spec map[string]any, embed embedFunc, dim int) *core.TwoTierRetriever {
	if len(spec) == 0 {
		return nil
	}
	hot := core.NewInMemoryHotVectorIndex()
	store := buildVectorStore(asMap(spec["vector_store"]), dim)
	var cold core.ColdSearch
	if store != nil {
		cold = store.ColdSearch()
	}
	for _, raw := range asList(spec["kb"]) {
		doc := asMap(raw)
		text := asString(doc["text"], "")
		vec := embed(text)
		id := asString(doc["id"], "")
		hot.Upsert(id, vec, text)
		if store != nil {
			store.Upsert(id, vec, text)
		}
	}
	return core.NewTwoTierRetriever(hot, cold, 4, 4)
}

// buildVectorStore builds the cold-tier vector store by kind. On a (qdrant) connection
// error it returns nil so the cold tier is skipped gracefully. Mirrors Python make_vector_store.
func buildVectorStore(spec map[string]any, dim int) core.VectorStore {
	if len(spec) == 0 {
		return nil
	}
	switch kind := asString(spec["kind"], "memory"); kind {
	case "memory":
		return core.NewInMemoryVectorStore()
	case "hnsw":
		m := asInt(spec["m"], 16)
		efC := asInt(spec["ef_construction"], 200)
		efS := asInt(spec["ef_search"], 50)
		seed := int64(asInt(spec["seed"], 42))
		return core.NewHnswVectorStore(m, efC, efS, seed)
	case "qdrant":
		url := resolveEnv(asString(spec["url"], ""))
		collection := asString(spec["collection"], "agentic")
		qd, err := stores.NewQdrantVectorStore(url, collection, dim)
		if err != nil {
			return nil // skip cold tier gracefully on connection error
		}
		return qd
	default:
		return nil
	}
}

// registerMcp connects to each declared MCP server (stdio only) and registers its tools
// (id-prefixed by name). Mirrors Python _register_mcp.
func registerMcp(tools *core.ToolRegistry, specs []any) error {
	for _, raw := range specs {
		m := asMap(raw)
		transport := strings.ToLower(asString(m["transport"], "stdio"))
		if transport != "stdio" {
			return fmt.Errorf("mcp transport %q not supported (use 'stdio')", transport)
		}
		var command string
		var args []string
		switch c := m["command"].(type) {
		case []any:
			if len(c) > 0 {
				command = resolveEnv(fmt.Sprint(c[0]))
				for _, a := range c[1:] {
					args = append(args, resolveEnv(fmt.Sprint(a)))
				}
			}
		default:
			command = resolveEnv(asString(m["command"], ""))
			for _, a := range asList(m["args"]) {
				args = append(args, resolveEnv(fmt.Sprint(a)))
			}
		}
		var env []string
		for k, v := range asMap(m["env"]) {
			env = append(env, k+"="+resolveEnv(fmt.Sprint(v)))
		}
		client, err := stores.NewMcpClient(command, env, args...)
		if err != nil {
			return fmt.Errorf("mcp %q: %w", asString(m["name"], "mcp"), err)
		}
		client.Register(tools, asString(m["name"], "mcp")+"_")
	}
	return nil
}

// registerA2A registers each declared peer agent as a tool (peer-as-tool over A2A HTTP).
// Mirrors Python _register_a2a.
func registerA2A(tools *core.ToolRegistry, specs []any) {
	for _, raw := range specs {
		a := asMap(raw)
		id := asString(a["id"], "")
		url := resolveEnv(asString(a["url"], ""))
		desc := asString(a["description"], "Delegate to peer agent "+id)
		tools.Register(id, desc, core.PeerTool(url, asInt(a["retries"], 2)))
	}
}

// resolveEnv expands a ${ENV} connection link (used for tool/mcp/a2a URLs).
func resolveEnv(value string) string {
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
		return os.Getenv(value[2 : len(value)-1])
	}
	return value
}

func buildRouter(spec map[string]any, paths map[string]*core.Agent) core.Router {
	def := asString(spec["default"], "")
	if def == "" {
		for k := range paths { // any path as a last resort
			def = k
		}
	}
	rules := asMap(spec["rules"])
	return func(event core.Event, ctx *core.AgentContext) string {
		low := strings.ToLower(event.Text)
		for path, kws := range rules {
			for _, kw := range asList(kws) {
				if strings.Contains(low, strings.ToLower(fmt.Sprint(kw))) {
					return path
				}
			}
		}
		return def
	}
}
