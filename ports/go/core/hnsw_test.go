package core

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func randVec(rng *rand.Rand, dim int) []float64 {
	v := make([]float64, dim)
	for i := range v {
		v[i] = rng.NormFloat64()
	}
	return v
}

func bruteTopK(vecs map[string][]float64, query []float64, k int) []string {
	type sc struct {
		id    string
		score float64
	}
	all := make([]sc, 0, len(vecs))
	for id, v := range vecs {
		all = append(all, sc{id, Cosine(query, v)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].score > all[j].score })
	if len(all) > k {
		all = all[:k]
	}
	out := make([]string, len(all))
	for i, s := range all {
		out[i] = s.id
	}
	return out
}

func TestHnswRecallMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	dim, n, k := 48, 400, 10
	vecs := map[string][]float64{}
	index := NewHnswIndex(16, 200, 64, 42)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("d%d", i)
		v := randVec(rng, dim)
		vecs[id] = v
		index.Add(id, v, id)
	}
	if index.Len() != n {
		t.Fatalf("len = %d, want %d", index.Len(), n)
	}
	hits, total := 0, 0
	for q := 0; q < 30; q++ {
		query := randVec(rng, dim)
		truth := map[string]bool{}
		for _, id := range bruteTopK(vecs, query, k) {
			truth[id] = true
		}
		for _, s := range index.Search(query, k) {
			if truth[s.ID] {
				hits++
			}
		}
		total += k
	}
	recall := float64(hits) / float64(total)
	if recall < 0.85 {
		t.Fatalf("recall@%d = %.3f, want >= 0.85", k, recall)
	}
}

func TestHnswTop1ExactForPlantedQuery(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	dim := 64
	store := NewHnswVectorStore(16, 200, 50, 1)
	planted := randVec(rng, dim)
	store.Upsert("target", planted, "the answer")
	for i := 0; i < 200; i++ {
		store.Upsert(fmt.Sprintf("noise%d", i), randVec(rng, dim), "noise")
	}
	perturbed := make([]float64, dim)
	for i, x := range planted {
		perturbed[i] = x + rng.NormFloat64()*0.01
	}
	top := store.Search(perturbed, 1)
	if len(top) == 0 || top[0].ID != "target" {
		t.Fatalf("top = %v, want target", top)
	}
	if top[0].Score < 0.99 {
		t.Fatalf("score = %.4f, want > 0.99", top[0].Score)
	}
}

func TestHnswAgreesWithInMemoryOnSmallSet(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	dim := 32
	mem := NewInMemoryVectorStore()
	hnsw := NewHnswVectorStore(16, 200, 50, 5)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("k%d", i)
		v := randVec(rng, dim)
		mem.Upsert(id, v, id)
		hnsw.Upsert(id, v, id)
	}
	q := randVec(rng, dim)
	if mem.Search(q, 1)[0].ID != hnsw.Search(q, 1)[0].ID {
		t.Fatal("hnsw top-1 disagrees with brute force on small set")
	}
}
