package core

import "sort"

// Priority is the MoSCoW priority of a context item.
type Priority int

const (
	PriorityWont   Priority = 0
	PriorityCould  Priority = 1
	PriorityShould Priority = 2
	PriorityMust   Priority = 3
)

// ContextItem is a prioritized piece of context with a cheap token estimate.
type ContextItem struct {
	Text     string
	Priority Priority
}

// Tokens is a deterministic ~4-chars/token estimate.
func (c ContextItem) Tokens() int {
	t := (len(c.Text) + 3) / 4
	if t < 1 {
		return 1
	}
	return t
}

// ContextWindowManager compacts items to a token budget, MoSCoW-first.
type ContextWindowManager struct{ MaxTokens int }

// NewContextWindowManager builds a manager (maxTokens<=0 => 1).
func NewContextWindowManager(maxTokens int) *ContextWindowManager {
	if maxTokens <= 0 {
		maxTokens = 1
	}
	return &ContextWindowManager{MaxTokens: maxTokens}
}

// Compact drops WON'T items, then greedily keeps highest-priority items within budget,
// preserving the original order among kept items.
func (m *ContextWindowManager) Compact(items []ContextItem) []ContextItem {
	var cands []ContextItem
	for _, it := range items {
		if it.Priority != PriorityWont {
			cands = append(cands, it)
		}
	}
	order := make([]int, len(cands))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return cands[order[a]].Priority > cands[order[b]].Priority
	})
	budget := m.MaxTokens
	keep := make(map[int]bool)
	for _, i := range order {
		if cands[i].Tokens() <= budget {
			keep[i] = true
			budget -= cands[i].Tokens()
		}
	}
	var out []ContextItem
	for i := range cands {
		if keep[i] {
			out = append(out, cands[i])
		}
	}
	return out
}

// TotalTokens sums the token estimates.
func (m *ContextWindowManager) TotalTokens(items []ContextItem) int {
	total := 0
	for _, it := range items {
		total += it.Tokens()
	}
	return total
}
