package core

import (
	"net/http"
	"testing"
	"time"
)

func TestHashingEmbedderDefault(t *testing.T) {
	e, err := NewEmbedder(map[string]any{"kind": "hashing", "dim": 256})
	if err != nil {
		t.Fatal(err)
	}
	if e.Dim() != 256 {
		t.Fatalf("dim = %d", e.Dim())
	}
	v1, v2 := e.Embed("crypto cash-back"), e.Embed("crypto cash-back")
	if len(v1) != 256 || Cosine(v1, v2) < 0.999 {
		t.Fatalf("hashing embedder not deterministic")
	}
	near := Cosine(e.Embed("crypto cash-back redemption"), v1)
	far := Cosine(e.Embed("the weather is sunny today"), v1)
	if near <= far {
		t.Fatalf("related text should be nearer: near=%f far=%f", near, far)
	}
}

func TestLangChainGoEmbedderAgainstOllamaIfAvailable(t *testing.T) {
	client := &http.Client{Timeout: 2 * time.Second}
	if _, err := client.Get("http://localhost:11434/api/tags"); err != nil {
		t.Skipf("Ollama not reachable: %v", err)
	}
	e, err := NewEmbedder(map[string]any{"kind": "ollama", "model": "nomic-embed-text"})
	if err != nil {
		t.Skipf("embedder build failed: %v", err)
	}
	v := e.Embed("hello world")
	if v == nil {
		t.Skip("Ollama embed returned nil (model not pulled?)")
	}
	if e.Dim() != len(v) {
		t.Fatalf("dim mismatch: Dim()=%d len=%d", e.Dim(), len(v))
	}
	if Cosine(v, e.Embed("hello world")) < 0.99 {
		t.Fatalf("same text should embed (near-)identically")
	}
}
