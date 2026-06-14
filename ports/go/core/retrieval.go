package core

import (
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"unicode"
)

// Scored is a retrieval hit: id, similarity score, and the document text.
type Scored struct {
	ID    string
	Score float64
	Text  string
}

// Embed is a deterministic, model-free bag-of-words hashing embedder: tokenize, hash
// each token into one of dim buckets, count, then L2-normalize. Good enough for the
// model-free demo and identical across runs (no randomness).
func Embed(text string, dim int) []float64 {
	vec := make([]float64, dim)
	for _, tok := range tokenize(text) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		vec[h.Sum32()%uint32(dim)]++
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

func tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
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

// InMemoryHotVectorIndex is a brute-force KNN index.
type InMemoryHotVectorIndex struct {
	entries map[string]hotEntry
}

// NewInMemoryHotVectorIndex builds an empty hot index.
func NewInMemoryHotVectorIndex() *InMemoryHotVectorIndex {
	return &InMemoryHotVectorIndex{entries: map[string]hotEntry{}}
}

func (idx *InMemoryHotVectorIndex) Upsert(id string, vec []float64, text string) {
	idx.entries[id] = hotEntry{vec: vec, text: text}
}

func (idx *InMemoryHotVectorIndex) Search(query []float64, k int) []Scored {
	hits := make([]Scored, 0, len(idx.entries))
	for id, e := range idx.entries {
		hits = append(hits, Scored{ID: id, Score: Cosine(query, e.vec), Text: e.text})
	}
	sortScored(hits)
	if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits
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
