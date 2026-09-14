package core

import (
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"
)

// Scored is a retrieval hit: id, similarity score, and the document text.
type Scored struct {
	ID    string
	Score float64
	Text  string
}

// Fnv1a32 is the FNV-1a 32-bit hash over the UTF-8 bytes of token — byte-for-byte
// identical to the Python and Go cores, so the embedder produces the same vectors.
func Fnv1a32(token string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(token))
	return h.Sum32()
}

// Embed is a deterministic, model-free bag-of-words hashing embedder: tokenize, hash
// each token into one of dim buckets (FNV-1a), count, then L2-normalize. Identical
// across the Python/Java/Go cores and stable across processes.
func Embed(text string, dim int) []float64 {
	vec := make([]float64, dim)
	for _, tok := range tokenize(text) {
		vec[Fnv1a32(tok)%uint32(dim)]++
	}
	var norm float64
	for _, v := range vec {
		norm += v * v
	}
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range vec {
			vec[i] /= norm
		}
	}
	return vec
}

// tokenize lowercases and splits on non-ASCII-alphanumeric runs, matching the Python
// `[a-z0-9]+` / Java `[^a-z0-9]+` tokenizers exactly.
func tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'))
	})
}

// Cosine is the dot product of two equal-length vectors (cosine similarity when both
// are L2-normalized, as Embed produces).
func Cosine(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}
	var dot float64
	for i := range a {
		dot += a[i] * b[i]
	}
	return dot
}

// HotVectorIndex is the in-memory (hot tier) vector store.
type HotVectorIndex interface {
	Upsert(id string, vec []float64, text string)
	Search(query []float64, k int) []Scored
}

type hotEntry struct {
	vec  []float64
	text string
}

// DefaultHotCapacity bounds the in-memory hot index (LRU eviction), matching the
// Python/Java cores so long-running jobs don't grow without limit.
const DefaultHotCapacity = 2000

// InMemoryHotVectorIndex is a capacity-bounded (LRU), goroutine-safe brute-force KNN
// index — the recent/hot tier.
type InMemoryHotVectorIndex struct {
	mu      sync.Mutex
	max     int
	entries map[string]hotEntry
	order   []string // oldest first; LRU eviction order
}

// NewInMemoryHotVectorIndex builds an empty hot index with the default capacity.
func NewInMemoryHotVectorIndex() *InMemoryHotVectorIndex {
	return NewInMemoryHotVectorIndexWithCapacity(DefaultHotCapacity)
}

// NewInMemoryHotVectorIndexWithCapacity builds a hot index keeping at most max entries
// (<=0 means the default).
func NewInMemoryHotVectorIndexWithCapacity(max int) *InMemoryHotVectorIndex {
	if max <= 0 {
		max = DefaultHotCapacity
	}
	return &InMemoryHotVectorIndex{max: max, entries: map[string]hotEntry{}}
}

func (idx *InMemoryHotVectorIndex) Upsert(id string, vec []float64, text string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if _, exists := idx.entries[id]; exists {
		idx.order = removeString(idx.order, id)
	}
	idx.entries[id] = hotEntry{vec: vec, text: text}
	idx.order = append(idx.order, id)
	for len(idx.order) > idx.max {
		oldest := idx.order[0]
		idx.order = idx.order[1:]
		delete(idx.entries, oldest)
	}
}

func (idx *InMemoryHotVectorIndex) Search(query []float64, k int) []Scored {
	idx.mu.Lock()
	hits := make([]Scored, 0, len(idx.entries))
	for id, e := range idx.entries {
		hits = append(hits, Scored{ID: id, Score: Cosine(query, e.vec), Text: e.text})
	}
	idx.mu.Unlock()
	sortScored(hits)
	if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits
}

// Size returns the number of entries currently held.
func (idx *InMemoryHotVectorIndex) Size() int {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return len(idx.entries)
}

func removeString(xs []string, x string) []string {
	out := xs[:0]
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}

// ColdSearch is the optional external (cold tier) search function.
type ColdSearch func(query []float64, k int) []Scored

// TwoTierRetriever merges hot + cold results, de-duplicating by id (hot wins) and
// returning the top-k by score.
type TwoTierRetriever struct {
	hot   HotVectorIndex
	cold  ColdSearch // may be nil
	hotK  int
	coldK int
}

// NewTwoTierRetriever builds a retriever. cold may be nil (hot-only).
func NewTwoTierRetriever(hot HotVectorIndex, cold ColdSearch, hotK, coldK int) *TwoTierRetriever {
	return &TwoTierRetriever{hot: hot, cold: cold, hotK: hotK, coldK: coldK}
}

// Retrieve returns the top-k merged hits.
func (r *TwoTierRetriever) Retrieve(query []float64, k int) []Scored {
	merged := map[string]Scored{}
	order := []string{}
	add := func(s Scored, hotTier bool) {
		if existing, ok := merged[s.ID]; ok {
			// Hot wins on text; keep the higher score.
			if hotTier {
				existing.Text = s.Text
			}
			if s.Score > existing.Score {
				existing.Score = s.Score
			}
			merged[s.ID] = existing
			return
		}
		merged[s.ID] = s
		order = append(order, s.ID)
	}
	if r.hot != nil {
		for _, s := range r.hot.Search(query, r.hotK) {
			add(s, true)
		}
	}
	if r.cold != nil {
		for _, s := range r.cold(query, r.coldK) {
			add(s, false)
		}
	}
	out := make([]Scored, 0, len(order))
	for _, id := range order {
		out = append(out, merged[id])
	}
	sortScored(out)
	if k > 0 && len(out) > k {
		out = out[:k]
	}
	return out
}

func sortScored(hits []Scored) {
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
}
