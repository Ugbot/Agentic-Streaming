package core

import (
	"context"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/ollama"
	"github.com/tmc/langchaingo/llms/openai"
)

// Embedder turns text into a vector. The model-free HashingEmbedder is the default;
// LangChainGoEmbedder calls a real provider (Ollama, OpenAI, …). The retriever takes any
// Embedder, so swapping a hashing embedder for a real one is a one-line config change.
type Embedder interface {
	Embed(text string) []float64
	EmbedBatch(texts []string) [][]float64
	Dim() int
}

// HashingEmbedder is the deterministic FNV bag-of-words embedder (the default).
type HashingEmbedder struct{ dim int }

// NewHashingEmbedder builds a hashing embedder (dim<=0 => 256).
func NewHashingEmbedder(dim int) *HashingEmbedder {
	if dim <= 0 {
		dim = 256
	}
	return &HashingEmbedder{dim: dim}
}

func (e *HashingEmbedder) Embed(text string) []float64 { return Embed(text, e.dim) }

func (e *HashingEmbedder) EmbedBatch(texts []string) [][]float64 {
	out := make([][]float64, len(texts))
	for i, t := range texts {
		out[i] = Embed(t, e.dim)
	}
	return out
}

func (e *HashingEmbedder) Dim() int { return e.dim }

// LangChainGoEmbedder is a real embedder via langchaingo (Ollama / OpenAI).
type LangChainGoEmbedder struct {
	emb *embeddings.EmbedderImpl
	dim int
}

// NewLangChainGoEmbedder builds a real embedder. provider is "ollama" | "openai".
func NewLangChainGoEmbedder(provider, model, baseURL string) (*LangChainGoEmbedder, error) {
	var client embeddings.EmbedderClient
	var err error
	switch provider {
	case "openai":
		opts := []openai.Option{}
		if model != "" {
			opts = append(opts, openai.WithEmbeddingModel(model))
		}
		if baseURL != "" {
			opts = append(opts, openai.WithBaseURL(baseURL))
		}
		client, err = openai.New(opts...)
	default: // ollama
		if model == "" {
			model = "nomic-embed-text"
		}
		opts := []ollama.Option{ollama.WithModel(model)}
		if baseURL != "" {
			opts = append(opts, ollama.WithServerURL(baseURL))
		}
		client, err = ollama.New(opts...)
	}
	if err != nil {
		return nil, err
	}
	emb, err := embeddings.NewEmbedder(client)
	if err != nil {
		return nil, err
	}
	return &LangChainGoEmbedder{emb: emb}, nil
}

func (e *LangChainGoEmbedder) Embed(text string) []float64 {
	v, err := e.emb.EmbedQuery(context.Background(), text)
	if err != nil {
		return nil
	}
	return f32to64(v)
}

func (e *LangChainGoEmbedder) EmbedBatch(texts []string) [][]float64 {
	vs, err := e.emb.EmbedDocuments(context.Background(), texts)
	if err != nil {
		return nil
	}
	out := make([][]float64, len(vs))
	for i, v := range vs {
		out[i] = f32to64(v)
	}
	return out
}

func (e *LangChainGoEmbedder) Dim() int {
	if e.dim == 0 {
		if v := e.Embed("dimension probe"); v != nil {
			e.dim = len(v)
		}
	}
	return e.dim
}

func f32to64(v []float32) []float64 {
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = float64(x)
	}
	return out
}

// NewEmbedder builds an Embedder from a {kind|provider, dim, model, base_url} spec.
func NewEmbedder(spec map[string]any) (Embedder, error) {
	kind, _ := spec["kind"].(string)
	if kind == "" {
		kind, _ = spec["provider"].(string)
	}
	if kind == "" {
		kind = "hashing"
	}
	switch kind {
	case "hashing", "memory":
		dim := 256
		if d, ok := spec["dim"].(int); ok {
			dim = d
		}
		return NewHashingEmbedder(dim), nil
	case "ollama", "openai", "langchaingo":
		provider := kind
		if kind == "langchaingo" {
			provider, _ = spec["provider"].(string)
		}
		model, _ := spec["model"].(string)
		baseURL, _ := spec["base_url"].(string)
		return NewLangChainGoEmbedder(provider, model, baseURL)
	}
	return NewHashingEmbedder(256), nil
}
