package core

import (
	"container/heap"
	"math"
	"math/rand"
	"sort"
	"sync"
)

// A hand-rolled HNSW (Hierarchical Navigable Small World) index — a real approximate
// nearest-neighbour graph that runs in-process, so the cores don't need an external vector
// database. Implements Malkov & Yashunin (2016): a multi-layer navigable small-world graph
// with greedy descent through the upper layers and an ef-bounded best-first search at
// layer 0. Distances are 1 - cosine (smaller = nearer); scores returned are cosine
// similarity. The level-assignment RNG is seeded so recall is reproducible. Mirrors the
// Python/Java cores and the Flink in-JVM FlinkStateHnswVectorMemory.

// HnswIndex is an in-memory HNSW graph.
type HnswIndex struct {
	mu             sync.Mutex
	m              int
	m0             int
	efConstruction int
	efSearch       int
	ml             float64
	rng            *rand.Rand

	vecs  map[string][]float64
	text  map[string]string
	level map[string]int
	// graph[layer][nodeID] -> neighbour ids
	graph []map[string][]string
	entry string
	top   int
	count int
}

// NewHnswIndex builds an index. m neighbours per node per layer (2*m at layer 0),
// efConstruction candidate width while inserting, efSearch while querying. seed makes
// level assignment reproducible. Zero values fall back to sensible defaults.
func NewHnswIndex(m, efConstruction, efSearch int, seed int64) *HnswIndex {
	if m < 2 {
		m = 16
	}
	if efConstruction <= 0 {
		efConstruction = 200
	}
	if efSearch <= 0 {
		efSearch = 50
	}
	return &HnswIndex{
		m:              m,
		m0:             2 * m,
		efConstruction: efConstruction,
		efSearch:       efSearch,
		ml:             1.0 / math.Log(float64(m)),
		rng:            rand.New(rand.NewSource(seed)),
		vecs:           map[string][]float64{},
		text:           map[string]string{},
		level:          map[string]int{},
		entry:          "",
		top:            -1,
	}
}

func (h *HnswIndex) distance(a []float64, id string) float64 {
	return 1.0 - Cosine(a, h.vecs[id])
}

func (h *HnswIndex) neighbors(id string, layer int) []string {
	if layer >= len(h.graph) {
		return nil
	}
	return h.graph[layer][id]
}

func (h *HnswIndex) ensureLayers(level int) {
	for len(h.graph) <= level {
		h.graph = append(h.graph, map[string][]string{})
	}
}

func (h *HnswIndex) randomLevel() int {
	r := h.rng.Float64()
	for r <= 0.0 {
		r = h.rng.Float64()
	}
	return int(-math.Log(r) * h.ml)
}

// distItem is a (distance, id) pair used in the search heaps.
type distItem struct {
	dist float64
	id   string
}

// minHeap pops the nearest (smallest distance) first.
type minHeap []distItem

func (m minHeap) Len() int           { return len(m) }
func (m minHeap) Less(i, j int) bool { return m[i].dist < m[j].dist }
func (m minHeap) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m *minHeap) Push(x any)        { *m = append(*m, x.(distItem)) }
func (m *minHeap) Pop() any          { old := *m; n := len(old); it := old[n-1]; *m = old[:n-1]; return it }

// maxHeap pops the farthest (largest distance) first.
type maxHeap []distItem

func (m maxHeap) Len() int           { return len(m) }
func (m maxHeap) Less(i, j int) bool { return m[i].dist > m[j].dist }
func (m maxHeap) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m *maxHeap) Push(x any)        { *m = append(*m, x.(distItem)) }
func (m *maxHeap) Pop() any          { old := *m; n := len(old); it := old[n-1]; *m = old[:n-1]; return it }

// searchLayer does a best-first search within one layer, returning up to ef (distance, id)
// pairs sorted by ascending distance.
func (h *HnswIndex) searchLayer(query []float64, entryPoints []string, ef, layer int) []distItem {
	visited := make(map[string]bool, ef*2)
	candidates := &minHeap{}
	results := &maxHeap{}
	heap.Init(candidates)
	heap.Init(results)
	for _, ep := range entryPoints {
		d := h.distance(query, ep)
		visited[ep] = true
		heap.Push(candidates, distItem{d, ep})
		heap.Push(results, distItem{d, ep})
	}
	for candidates.Len() > 0 {
		c := heap.Pop(candidates).(distItem)
		worst := (*results)[0].dist
		if c.dist > worst && results.Len() >= ef {
			break
		}
		for _, e := range h.neighbors(c.id, layer) {
			if visited[e] {
				continue
			}
			visited[e] = true
			d := h.distance(query, e)
			worst = (*results)[0].dist
			if results.Len() < ef || d < worst {
				heap.Push(candidates, distItem{d, e})
				heap.Push(results, distItem{d, e})
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}
	out := make([]distItem, results.Len())
	copy(out, *results)
	sort.Slice(out, func(i, j int) bool { return out[i].dist < out[j].dist })
	return out
}

func (h *HnswIndex) selectNeighbors(candidates []distItem, m int) []string {
	sorted := make([]distItem, len(candidates))
	copy(sorted, candidates)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].dist < sorted[j].dist })
	if len(sorted) > m {
		sorted = sorted[:m]
	}
	ids := make([]string, len(sorted))
	for i, it := range sorted {
		ids[i] = it.id
	}
	return ids
}

func (h *HnswIndex) prune(id string, layer int) {
	mMax := h.m
	if layer == 0 {
		mMax = h.m0
	}
	neigh := h.graph[layer][id]
	if len(neigh) <= mMax {
		return
	}
	items := make([]distItem, len(neigh))
	for i, n := range neigh {
		items[i] = distItem{h.distance(h.vecs[id], n), n}
	}
	h.graph[layer][id] = h.selectNeighbors(items, mMax)
}

func (h *HnswIndex) greedy(query []float64, entry string, layer int) string {
	cur := entry
	curD := h.distance(query, cur)
	changed := true
	for changed {
		changed = false
		for _, e := range h.neighbors(cur, layer) {
			d := h.distance(query, e)
			if d < curD {
				cur, curD, changed = e, d, true
			}
		}
	}
	return cur
}

// Add inserts (or refreshes) a vector. Refreshing an existing id updates its vector/text
// and keeps existing links (approximate); typical KB usage inserts unique ids once.
func (h *HnswIndex) Add(id string, vector []float64, text string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, update := h.vecs[id]
	v := make([]float64, len(vector))
	copy(v, vector)
	h.vecs[id] = v
	h.text[id] = text
	if update {
		return
	}
	h.count++

	level := h.randomLevel()
	h.level[id] = level
	h.ensureLayers(level)
	for lc := 0; lc <= level; lc++ {
		if _, ok := h.graph[lc][id]; !ok {
			h.graph[lc][id] = []string{}
		}
	}

	if h.entry == "" {
		h.entry = id
		h.top = level
		return
	}

	cur := h.entry
	for lc := h.top; lc > level; lc-- {
		cur = h.greedy(v, cur, lc)
	}
	start := level
	if h.top < start {
		start = h.top
	}
	for lc := start; lc >= 0; lc-- {
		found := h.searchLayer(v, []string{cur}, h.efConstruction, lc)
		m := h.m
		if lc == 0 {
			m = h.m0
		}
		for _, nb := range h.selectNeighbors(found, m) {
			h.graph[lc][id] = appendUnique(h.graph[lc][id], nb)
			h.graph[lc][nb] = appendUnique(h.graph[lc][nb], id)
			h.prune(nb, lc)
		}
		if len(found) > 0 {
			cur = found[0].id
		}
	}

	if level > h.top {
		h.top = level
		h.entry = id
	}
}

func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}

// Search returns the k nearest neighbours as Scored (score = cosine similarity).
func (h *HnswIndex) Search(query []float64, k int) []Scored {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.entry == "" {
		return nil
	}
	cur := h.entry
	for lc := h.top; lc > 0; lc-- {
		cur = h.greedy(query, cur, lc)
	}
	ef := h.efSearch
	if k > ef {
		ef = k
	}
	found := h.searchLayer(query, []string{cur}, ef, 0)
	if k > 0 && len(found) > k {
		found = found[:k]
	}
	out := make([]Scored, len(found))
	for i, it := range found {
		out[i] = Scored{ID: it.id, Score: 1.0 - it.dist, Text: h.text[it.id]}
	}
	return out
}

// Len reports the number of indexed vectors.
func (h *HnswIndex) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// HnswVectorStore adapts HnswIndex to the VectorStore SPI (in-process cold tier).
type HnswVectorStore struct {
	index *HnswIndex
}

// NewHnswVectorStore builds an in-process HNSW-backed vector store.
func NewHnswVectorStore(m, efConstruction, efSearch int, seed int64) *HnswVectorStore {
	return &HnswVectorStore{index: NewHnswIndex(m, efConstruction, efSearch, seed)}
}

func (s *HnswVectorStore) Upsert(docID string, embedding []float64, text string) {
	s.index.Add(docID, embedding, text)
}

func (s *HnswVectorStore) Search(query []float64, k int) []Scored { return s.index.Search(query, k) }

func (s *HnswVectorStore) ColdSearch() ColdSearch {
	return func(query []float64, k int) []Scored { return s.index.Search(query, k) }
}
