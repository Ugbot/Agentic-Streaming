package core

import "fmt"

// Skill is a named bundle of (tools + a system-prompt fragment + required facts) — the
// portable analogue of the Flink Skill. A path declares skills and the builder expands
// them into the path's tool set + prompt.
type Skill struct {
	Name           string
	Tools          []string
	PromptFragment string
	RequiredFacts  []string
}

// SkillRegistry holds named skills, looked up by the builder.
type SkillRegistry struct {
	skills map[string]Skill
}

// NewSkillRegistry builds an empty registry.
func NewSkillRegistry() *SkillRegistry { return &SkillRegistry{skills: map[string]Skill{}} }

// Register adds a skill and returns the registry for chaining.
func (r *SkillRegistry) Register(s Skill) *SkillRegistry {
	r.skills[s.Name] = s
	return r
}

// Get returns a skill (and whether it exists).
func (r *SkillRegistry) Get(name string) (Skill, bool) {
	s, ok := r.skills[name]
	return s, ok
}

// Expand resolves skill names → (extra tool ids, joined prompt fragment, required facts).
func (r *SkillRegistry) Expand(names []string) ([]string, string, []string) {
	var tools, facts []string
	var fragment string
	for _, n := range names {
		s, ok := r.skills[n]
		if !ok {
			continue
		}
		for _, t := range s.Tools {
			if !containsStr(tools, t) {
				tools = append(tools, t)
			}
		}
		if s.PromptFragment != "" {
			if fragment != "" {
				fragment += "\n"
			}
			fragment += s.PromptFragment
		}
		facts = append(facts, s.RequiredFacts...)
	}
	return tools, fragment, facts
}

// SkillRegistryFromSpecs builds from the YAML `skills:` list.
func SkillRegistryFromSpecs(specs []map[string]any) *SkillRegistry {
	reg := NewSkillRegistry()
	for _, s := range specs {
		name, _ := s["name"].(string)
		skill := Skill{Name: name, PromptFragment: fmt.Sprint(orEmpty(s["prompt"]))}
		for _, t := range toAnySlice(s["tools"]) {
			skill.Tools = append(skill.Tools, fmt.Sprint(t))
		}
		for _, f := range toAnySlice(s["facts"]) {
			skill.RequiredFacts = append(skill.RequiredFacts, fmt.Sprint(f))
		}
		reg.Register(skill)
	}
	return reg
}

func orEmpty(v any) any {
	if v == nil {
		return ""
	}
	return v
}

func toAnySlice(v any) []any {
	if l, ok := v.([]any); ok {
		return l
	}
	return nil
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
