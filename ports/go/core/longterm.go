package core

import (
	"sort"
	"sync"
)

// LongTermStore is conversation resumption + a per-user fact archive — the portable
// analogue of the Flink LongTermMemoryStore. InMemoryLongTermStore is the default; the
// stores package adds a real Postgres impl.
type LongTermStore interface {
	SaveTurn(conversationID, userID, role, content string)
	LoadHistory(conversationID string) [][2]string // (role, content) pairs
	SaveFact(userID, key, value string)
	Facts(userID string) map[string]string
	ConversationsForUser(userID string) []string
}

// InMemoryLongTermStore is the process-local default.
type InMemoryLongTermStore struct {
	mu     sync.Mutex
	turns  map[string][][2]string
	owner  map[string]string
	facts  map[string]map[string]string
}

// NewInMemoryLongTermStore builds an empty store.
func NewInMemoryLongTermStore() *InMemoryLongTermStore {
	return &InMemoryLongTermStore{
		turns: map[string][][2]string{}, owner: map[string]string{}, facts: map[string]map[string]string{}}
}

func (s *InMemoryLongTermStore) SaveTurn(conversationID, userID, role, content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turns[conversationID] = append(s.turns[conversationID], [2]string{role, content})
	s.owner[conversationID] = userID
}

func (s *InMemoryLongTermStore) LoadHistory(conversationID string) [][2]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][2]string, len(s.turns[conversationID]))
	copy(out, s.turns[conversationID])
	return out
}

func (s *InMemoryLongTermStore) SaveFact(userID, key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.facts[userID] == nil {
		s.facts[userID] = map[string]string{}
	}
	s.facts[userID][key] = value
}

func (s *InMemoryLongTermStore) Facts(userID string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for k, v := range s.facts[userID] {
		out[k] = v
	}
	return out
}

func (s *InMemoryLongTermStore) ConversationsForUser(userID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for cid, u := range s.owner {
		if u == userID {
			out = append(out, cid)
		}
	}
	sort.Strings(out)
	return out
}
