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
	"strings"
	"time"

	"github.com/jagentic/goagentic/core"
)

// keywordBrain: fire a tool on a trigger keyword, else answer from retrieval, else echo.
type keywordBrain struct {
	name         string
	dim          int
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
		hits := ctx.Retriever.Retrieve(core.Embed(userText, b.dim), 1)
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
	retrieval := asMap(spec["retrieval"])
	dim := asInt(retrieval["dim"], 256)
	retriever := buildRetriever(retrieval, dim)

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
			if os := asMap(ps["output_schema"]); len(os) > 0 {
				lb.WithOutputSchema(os)
			}
			brain = lb
		case "rule":
			triggers := map[string]string{}
			for k, v := range asMap(ps["tool_triggers"]) {
				triggers[k] = fmt.Sprint(v)
			}
			brain = keywordBrain{name: name, dim: dim, toolTriggers: triggers, threshold: asFloat(ps["threshold"], 0.15)}
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
		gm := asMap(g)
		if asString(gm["kind"], "regex") == "regex" {
			var deny []string
			for _, d := range asList(gm["deny"]) {
				deny = append(deny, fmt.Sprint(d))
			}
			graph.WithGuardrails(core.NewRegexGuardrail(deny, asString(gm["reason"], "blocked by policy"),
				asBool(gm["check_outputs"])))
		}
	}

	return Built{Graph: graph, Tools: tools, Retriever: retriever}, nil
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
			url := asString(t["url"], "")
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

func buildRetriever(spec map[string]any, dim int) *core.TwoTierRetriever {
	hot := core.NewInMemoryHotVectorIndex()
	for _, raw := range asList(spec["kb"]) {
		doc := asMap(raw)
		text := asString(doc["text"], "")
		hot.Upsert(asString(doc["id"], ""), core.Embed(text, dim), text)
	}
	return core.NewTwoTierRetriever(hot, nil, 4, 4)
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
