package core

import (
	"fmt"
	"math"
	"regexp"
	"strings"
)

// Traditional ML / DL inference SPI (Tier 4) — Classifier and Scorer interfaces plus two
// working, deterministic-by-default implementations and a ClassifierGuardrail that screens
// turns using a classifier instead of a regex.
//
//   - LexiconClassifier: keyword-weighted bag-of-words classifier — no model, fully offline.
//   - EmbeddingClassifier: nearest-centroid classifier over an Embedder — works offline with
//     the hashing embedder and becomes a real semantic classifier with a real embedder
//     (Ollama/OpenAI). This is the "real model opt-in" path: swap the embedder, keep the SPI.
//
// Mirrors the Flink inference/ SPI (Classifier/Scorer/guardrails) at a portable subset.

var wordRe = regexp.MustCompile(`[a-z0-9']+`)

func tokens(text string) []string {
	return wordRe.FindAllString(strings.ToLower(text), -1)
}

// Classification is a label + its confidence in [0,1], with the full per-label score map.
type Classification struct {
	Label  string
	Score  float64
	Scores map[string]float64
}

// Classifier assigns a label (with confidence) to a piece of text.
type Classifier interface {
	Classify(text string) Classification
}

// Scorer maps text to a single score in [0,1].
type Scorer interface {
	Score(text string) float64
}

// LexiconClassifier scores each label by the fraction of the text's tokens that hit that
// label's keyword set, normalised so the per-label scores sum to 1. Deterministic, offline.
type LexiconClassifier struct {
	lexicon      map[string]map[string]bool
	defaultLabel string
}

// NewLexiconClassifier builds a classifier from label -> keywords.
func NewLexiconClassifier(lexicon map[string][]string, defaultLabel string) *LexiconClassifier {
	lex := make(map[string]map[string]bool, len(lexicon))
	for label, words := range lexicon {
		set := make(map[string]bool, len(words))
		for _, w := range words {
			set[strings.ToLower(w)] = true
		}
		lex[label] = set
	}
	if defaultLabel == "" {
		defaultLabel = "other"
	}
	return &LexiconClassifier{lexicon: lex, defaultLabel: defaultLabel}
}

// Classify returns the best-matching label and a normalised score distribution.
func (c *LexiconClassifier) Classify(text string) Classification {
	toks := tokens(text)
	scores := make(map[string]float64, len(c.lexicon))
	for label := range c.lexicon {
		scores[label] = 0.0
	}
	if len(toks) == 0 {
		return Classification{Label: c.defaultLabel, Score: 0.0, Scores: scores}
	}
	n := float64(len(toks))
	total := 0.0
	for label, words := range c.lexicon {
		hits := 0
		for _, t := range toks {
			if words[t] {
				hits++
			}
		}
		scores[label] = float64(hits) / n
		total += scores[label]
	}
	if total <= 0.0 {
		return Classification{Label: c.defaultLabel, Score: 0.0, Scores: scores}
	}
	best, bestScore := c.defaultLabel, -1.0
	for label := range scores {
		scores[label] /= total
		if scores[label] > bestScore {
			best, bestScore = label, scores[label]
		}
	}
	return Classification{Label: best, Score: bestScore, Scores: scores}
}

// EmbeddingClassifier is a nearest-centroid classifier over an Embedder. Fit averages each
// label's example embeddings into a centroid; Classify returns the label whose centroid is
// most cosine-similar, mapped to a softmax distribution.
type EmbeddingClassifier struct {
	embedder    Embedder
	temperature float64
	centroids   map[string][]float64
}

// NewEmbeddingClassifier builds a classifier (nil embedder => hashing default; temp<=0 => 10).
func NewEmbeddingClassifier(embedder Embedder, temperature float64) *EmbeddingClassifier {
	if embedder == nil {
		embedder = NewHashingEmbedder(256)
	}
	if temperature <= 0 {
		temperature = 10.0
	}
	return &EmbeddingClassifier{embedder: embedder, temperature: temperature, centroids: map[string][]float64{}}
}

// Fit averages each label's example embeddings into a centroid.
func (c *EmbeddingClassifier) Fit(examples map[string][]string) (*EmbeddingClassifier, error) {
	for label, texts := range examples {
		if len(texts) == 0 {
			continue
		}
		vecs := c.embedder.EmbedBatch(texts)
		if len(vecs) == 0 || len(vecs[0]) == 0 {
			return nil, fmt.Errorf("embedder returned no vectors for label %q", label)
		}
		dim := len(vecs[0])
		centroid := make([]float64, dim)
		for _, v := range vecs {
			for i := 0; i < dim && i < len(v); i++ {
				centroid[i] += v[i]
			}
		}
		for i := range centroid {
			centroid[i] /= float64(len(vecs))
		}
		c.centroids[label] = centroid
	}
	if len(c.centroids) == 0 {
		return nil, fmt.Errorf("no non-empty example sets")
	}
	return c, nil
}

// Classify returns the nearest-centroid label with a softmax score distribution.
func (c *EmbeddingClassifier) Classify(text string) Classification {
	if len(c.centroids) == 0 {
		return Classification{Label: "other", Score: 0.0, Scores: map[string]float64{}}
	}
	vec := c.embedder.Embed(text)
	sims := make(map[string]float64, len(c.centroids))
	denom := 0.0
	for label, centroid := range c.centroids {
		sims[label] = math.Exp(c.temperature * Cosine(vec, centroid))
		denom += sims[label]
	}
	if denom == 0 {
		denom = 1.0
	}
	best, bestScore := "other", -1.0
	scores := make(map[string]float64, len(sims))
	for label, e := range sims {
		scores[label] = e / denom
		if scores[label] > bestScore {
			best, bestScore = label, scores[label]
		}
	}
	return Classification{Label: best, Score: bestScore, Scores: scores}
}

// ClassifierScorer adapts a Classifier into a Scorer for one target label.
type ClassifierScorer struct {
	classifier Classifier
	label      string
}

// NewClassifierScorer builds a scorer over a classifier for the given target label.
func NewClassifierScorer(classifier Classifier, label string) *ClassifierScorer {
	return &ClassifierScorer{classifier: classifier, label: label}
}

// Score returns the probability the classifier assigns to the target label.
func (s *ClassifierScorer) Score(text string) float64 {
	c := s.classifier.Classify(text)
	if v, ok := c.Scores[s.label]; ok {
		return v
	}
	if c.Label == s.label {
		return c.Score
	}
	return 0.0
}

// ClassifierGuardrail blocks when a classifier assigns a blocked label with confidence at
// or above threshold. A learned/lexicon alternative to RegexGuardrail.
type ClassifierGuardrail struct {
	classifier   Classifier
	blocked      map[string]bool
	threshold    float64
	reason       string
	checkOutputs bool
}

// NewClassifierGuardrail builds a classifier-backed guardrail.
func NewClassifierGuardrail(classifier Classifier, blockedLabels []string, threshold float64, reason string, checkOutputs bool) *ClassifierGuardrail {
	blocked := make(map[string]bool, len(blockedLabels))
	for _, l := range blockedLabels {
		blocked[strings.ToLower(l)] = true
	}
	if reason == "" {
		reason = "blocked by classifier policy"
	}
	return &ClassifierGuardrail{classifier: classifier, blocked: blocked, threshold: threshold, reason: reason, checkOutputs: checkOutputs}
}

func (g *ClassifierGuardrail) hit(text string) string {
	if text == "" {
		return ""
	}
	c := g.classifier.Classify(text)
	if g.blocked[strings.ToLower(c.Label)] && c.Score >= g.threshold {
		return fmt.Sprintf("%s (%s=%.2f)", g.reason, c.Label, c.Score)
	}
	return ""
}

// CheckInput screens the inbound text.
func (g *ClassifierGuardrail) CheckInput(text string) string { return g.hit(text) }

// CheckOutput screens the outbound reply when checkOutputs is set.
func (g *ClassifierGuardrail) CheckOutput(reply string) string {
	if g.checkOutputs {
		return g.hit(reply)
	}
	return ""
}
