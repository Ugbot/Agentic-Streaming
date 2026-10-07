package pipeline

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// Rule is one agent.router.rules entry. The spec makes the order of these entries
// normative (workflow.schema.json: "first path in declaration order wins"), so the loader
// keeps them as a slice instead of a Go map.
type Rule struct {
	Path     string
	Keywords []string
}

// Trigger is one paths.<name>.tool_triggers entry, kept in declaration order for the same
// reason as Rule (primitives.md section 9: "first match in declaration order").
type Trigger struct {
	Keyword string
	Tool    string
}

// Parse decodes a workflow document into the map shape Build consumes. The two mappings
// whose key order the spec relies on, agent.router.rules and every path's tool_triggers,
// are replaced by []Rule and []Trigger read from the YAML node tree, because yaml.v3
// decodes mappings into Go maps and Go maps do not remember declaration order.
func Parse(data []byte) (map[string]any, error) {
	var spec map[string]any
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	root := &doc
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		root = root.Content[0]
	}
	agentNode := mappingChild(root, "agent")
	agent, _ := spec["agent"].(map[string]any)
	if agentNode == nil || agent == nil {
		return spec, nil
	}
	if router, ok := agent["router"].(map[string]any); ok {
		if rulesNode := mappingChild(mappingChild(agentNode, "router"), "rules"); rulesNode != nil {
			rules, err := decodeRules(rulesNode)
			if err != nil {
				return nil, err
			}
			router["rules"] = rules
		}
	}
	pathsNode := mappingChild(agentNode, "paths")
	if paths, ok := agent["paths"].(map[string]any); ok && pathsNode != nil {
		for name, raw := range paths {
			ps, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if trigNode := mappingChild(mappingChild(pathsNode, name), "tool_triggers"); trigNode != nil {
				triggers, err := decodeTriggers(trigNode)
				if err != nil {
					return nil, fmt.Errorf("agent.paths.%s.tool_triggers: %w", name, err)
				}
				ps["tool_triggers"] = triggers
			}
		}
	}
	return spec, nil
}

// mappingChild returns the value node stored under key in a mapping node, or nil.
func mappingChild(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func decodeRules(node *yaml.Node) ([]Rule, error) {
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("agent.router.rules must be a mapping of path to keyword list")
	}
	rules := make([]Rule, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		var keywords []string
		if err := node.Content[i+1].Decode(&keywords); err != nil {
			return nil, fmt.Errorf("agent.router.rules.%s: %w", node.Content[i].Value, err)
		}
		rules = append(rules, Rule{Path: node.Content[i].Value, Keywords: keywords})
	}
	return rules, nil
}

func decodeTriggers(node *yaml.Node) ([]Trigger, error) {
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("must be a mapping of keyword to tool id")
	}
	triggers := make([]Trigger, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		var tool string
		if err := node.Content[i+1].Decode(&tool); err != nil {
			return nil, fmt.Errorf("%s: %w", node.Content[i].Value, err)
		}
		triggers = append(triggers, Trigger{Keyword: node.Content[i].Value, Tool: tool})
	}
	return triggers, nil
}

// asRules accepts the []Rule Parse produces or a plain map from a spec built in Go code.
// A Go map has no declaration order, so its keys are evaluated in sorted order, which is
// at least deterministic across runs; specs that depend on rule order must come from YAML.
func asRules(v any) []Rule {
	if rules, ok := v.([]Rule); ok {
		return rules
	}
	m := asMap(v)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rules := make([]Rule, 0, len(keys))
	for _, k := range keys {
		rules = append(rules, Rule{Path: k, Keywords: asStringList(m[k])})
	}
	return rules
}

// asTriggers is asRules for tool_triggers.
func asTriggers(v any) []Trigger {
	if triggers, ok := v.([]Trigger); ok {
		return triggers
	}
	m := asMap(v)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	triggers := make([]Trigger, 0, len(keys))
	for _, k := range keys {
		triggers = append(triggers, Trigger{Keyword: k, Tool: fmt.Sprint(m[k])})
	}
	return triggers
}
