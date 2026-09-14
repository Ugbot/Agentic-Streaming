package core

import "sync"

// KeyedStateStore is the portable form of Flink keyed ValueState — a per-(key,name)
// scalar slot. Adapters back it with native keyed state (a NATS KV bucket, a Temporal
// workflow field) or Redis.
type KeyedStateStore interface {
	Get(key, name string) (any, bool)
	Put(key, name string, value any)
	Clear(key string)
}

// InMemoryKeyedStateStore is the process-local default.
type InMemoryKeyedStateStore struct {
	mu sync.Mutex
	d  map[string]map[string]any
}

// NewInMemoryKeyedStateStore builds an empty keyed state store.
func NewInMemoryKeyedStateStore() *InMemoryKeyedStateStore {
	return &InMemoryKeyedStateStore{d: map[string]map[string]any{}}
}

func (s *InMemoryKeyedStateStore) Get(key, name string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.d[key]; m != nil {
		v, ok := m[name]
		return v, ok
	}
	return nil, false
}

func (s *InMemoryKeyedStateStore) Put(key, name string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.d[key]
	if m == nil {
		m = map[string]any{}
		s.d[key] = m
	}
	m[name] = value
}

func (s *InMemoryKeyedStateStore) Clear(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.d, key)
}
