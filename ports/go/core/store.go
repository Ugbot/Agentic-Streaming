package core

import "sync"

// ConversationStore is per-conversation memory — the single most important abstraction
// in the port: a durable, per-key transcript + scalar attributes, keyed by
// conversationId and indexable by userId. Swap InMemoryConversationStore for a
// Redis/NATS-KV/Postgres-backed implementation behind this interface; agent logic is
// unchanged.
type ConversationStore interface {
	Append(conversationID string, message ChatMessage)
	History(conversationID string) []ChatMessage
	MessageCount(conversationID string) int
	PutAttribute(conversationID, key, value string)
	GetAttribute(conversationID, key string) (string, bool)
	Attributes(conversationID string) map[string]string
	AssociateUser(conversationID, userID string)
	ConversationsForUser(userID string) []string
	Clear(conversationID string)
}

type convo struct {
	messages []ChatMessage
	attrs    map[string]string
	owner    string
}

// InMemoryConversationStore is the process-local, goroutine-safe default with a bounded
// transcript.
type InMemoryConversationStore struct {
	mu          sync.Mutex
	maxMessages int
	convos      map[string]*convo
	userIndex   map[string][]string
}

// NewInMemoryConversationStore builds a store keeping at most maxMessages per
// conversation (<=0 means the default of 200).
func NewInMemoryConversationStore(maxMessages int) *InMemoryConversationStore {
	if maxMessages <= 0 {
		maxMessages = 200
	}
	return &InMemoryConversationStore{
		maxMessages: maxMessages,
		convos:      map[string]*convo{},
		userIndex:   map[string][]string{},
	}
}

func (s *InMemoryConversationStore) get(cid string) *convo {
	c := s.convos[cid]
	if c == nil {
		c = &convo{attrs: map[string]string{}}
		s.convos[cid] = c
	}
	return c
}

func (s *InMemoryConversationStore) Append(cid string, m ChatMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.get(cid)
	c.messages = append(c.messages, m)
	if len(c.messages) > s.maxMessages {
		c.messages = c.messages[len(c.messages)-s.maxMessages:]
	}
}

func (s *InMemoryConversationStore) History(cid string) []ChatMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.convos[cid]
	if c == nil {
		return nil
	}
	out := make([]ChatMessage, len(c.messages))
	copy(out, c.messages)
	return out
}

func (s *InMemoryConversationStore) MessageCount(cid string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.convos[cid]; c != nil {
		return len(c.messages)
	}
	return 0
}

func (s *InMemoryConversationStore) PutAttribute(cid, key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.get(cid).attrs[key] = value
}

func (s *InMemoryConversationStore) GetAttribute(cid, key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.convos[cid]; c != nil {
		v, ok := c.attrs[key]
		return v, ok
	}
	return "", false
}

func (s *InMemoryConversationStore) Attributes(cid string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	if c := s.convos[cid]; c != nil {
		for k, v := range c.attrs {
			out[k] = v
		}
	}
	return out
}

func (s *InMemoryConversationStore) AssociateUser(cid, userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.get(cid)
	if c.owner != "" && c.owner != userID {
		s.userIndex[c.owner] = remove(s.userIndex[c.owner], cid)
	}
	c.owner = userID
	if !contains(s.userIndex[userID], cid) {
		s.userIndex[userID] = append(s.userIndex[userID], cid)
	}
}

func (s *InMemoryConversationStore) ConversationsForUser(userID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.userIndex[userID]
	out := make([]string, len(ids))
	copy(out, ids)
	return out
}

func (s *InMemoryConversationStore) Clear(cid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.convos[cid]; c != nil && c.owner != "" {
		s.userIndex[c.owner] = remove(s.userIndex[c.owner], cid)
	}
	delete(s.convos, cid)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func remove(xs []string, x string) []string {
	out := xs[:0]
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}
