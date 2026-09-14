// Package stores holds the real external store implementations (Qdrant vector store,
// Postgres long-term store, Redis conversation store) behind the goagentic core SPIs.
// They live here, with their own deps, so the core stays dependency-light.
package stores

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jagentic/goagentic/core"
)

// QdrantVectorStore is a real core.VectorStore backed by a Qdrant server (REST API) —
// the reference cold-tier impl. No heavy client dep; just the stable REST surface.
type QdrantVectorStore struct {
	baseURL    string
	collection string
	http       *http.Client
}

// NewQdrantVectorStore connects to Qdrant and ensures the collection exists.
func NewQdrantVectorStore(baseURL, collection string, dim int) (*QdrantVectorStore, error) {
	if baseURL == "" {
		baseURL = "http://localhost:6333"
	}
	q := &QdrantVectorStore{baseURL: strings.TrimRight(baseURL, "/"), collection: collection,
		http: &http.Client{Timeout: 10 * time.Second}}
	err := q.do("PUT", "/collections/"+collection, map[string]any{
		"vectors": map[string]any{"size": dim, "distance": "Cosine"}}, nil)
	if err != nil && !strings.Contains(err.Error(), "409") { // 409 = collection already exists
		return nil, err
	}
	return q, nil
}

func (q *QdrantVectorStore) do(method, path string, body any, out any) error {
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, q.baseURL+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("qdrant %s %s -> %d", method, path, resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (q *QdrantVectorStore) Upsert(docID string, embedding []float64, text string) {
	_ = q.do("PUT", "/collections/"+q.collection+"/points?wait=true", map[string]any{
		"points": []map[string]any{{
			"id":      uint64(core.Fnv1a32(docID)),
			"vector":  embedding,
			"payload": map[string]any{"doc_id": docID, "text": text},
		}}}, nil)
}

func (q *QdrantVectorStore) Search(query []float64, k int) []core.Scored {
	var resp struct {
		Result struct {
			Points []struct {
				Score   float64        `json:"score"`
				Payload map[string]any `json:"payload"`
			} `json:"points"`
		} `json:"result"`
	}
	if err := q.do("POST", "/collections/"+q.collection+"/points/query", map[string]any{
		"query": query, "limit": k, "with_payload": true}, &resp); err != nil {
		return nil
	}
	out := make([]core.Scored, 0, len(resp.Result.Points))
	for _, p := range resp.Result.Points {
		id, _ := p.Payload["doc_id"].(string)
		text, _ := p.Payload["text"].(string)
		out = append(out, core.Scored{ID: id, Score: p.Score, Text: text})
	}
	return out
}

func (q *QdrantVectorStore) ColdSearch() core.ColdSearch {
	return func(query []float64, k int) []core.Scored { return q.Search(query, k) }
}
