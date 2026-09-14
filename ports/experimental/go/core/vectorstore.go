package core

import "sync"

// VectorStore is the cold tier of the two-tier retriever, behind an interface.
// InMemoryVectorStore is the default; the stores package adds a real Qdrant impl.
type VectorStore interface {
	Upsert(docID string, embedding []float64, text string)
	Search(query []float64, k int) []Scored
	// ColdSearch adapts the store to the TwoTierRetriever cold-tier signature.
	ColdSearch() ColdSearch
}

type vsEntry struct {
	vec  []float64
	text string
}

// InMemoryVectorStore is a brute-force cold store (default).
type InMemoryVectorStore struct {
	mu   sync.Mutex
	docs map[string]vsEntry
}

// NewInMemoryVectorStore builds an empty in-memory vector store.
func NewInMemoryVectorStore() *InMemoryVectorStore {
	return &InMemoryVectorStore{docs: map[string]vsEntry{}}
}

func (s *InMemoryVectorStore) Upsert(docID string, embedding []float64, text string) {
	s.mu.Lock()
	s.docs[docID] = vsEntry{vec: embedding, text: text}
	s.mu.Unlock()
}

func (s *InMemoryVectorStore) Search(query []float64, k int) []Scored {
	s.mu.Lock()
	hits := make([]Scored, 0, len(s.docs))
	for id, e := range s.docs {
		hits = append(hits, Scored{ID: id, Score: Cosine(query, e.vec), Text: e.text})
	}
	s.mu.Unlock()
	sortScored(hits)
	if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits
}

func (s *InMemoryVectorStore) ColdSearch() ColdSearch {
	return func(query []float64, k int) []Scored { return s.Search(query, k) }
}
