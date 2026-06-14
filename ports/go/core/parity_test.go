package core

import (
	"math"
	"sort"
	"testing"
)

// Cross-core parity guard (Go side). The Python `test_parity.py` and Java `ParityTest`
// assert these exact same golden values, so any core diverging is caught.

var goldenFNV = map[string]uint32{
	"crypto": 1712156752, "balance": 2560266987, "card": 2284280159, "dispute": 3025431163,
}
var goldenBucket256 = map[string]uint32{
	"crypto": 80, "balance": 235, "card": 95, "dispute": 123,
}

func TestFnv1aGolden(t *testing.T) {
	for tok, want := range goldenFNV {
		if got := Fnv1a32(tok); got != want {
			t.Fatalf("Fnv1a32(%q)=%d want %d", tok, got, want)
		}
		if got := Fnv1a32(tok) % 256; got != goldenBucket256[tok] {
			t.Fatalf("bucket(%q)=%d want %d", tok, got, goldenBucket256[tok])
		}
	}
}

func TestEmbedGoldenVector(t *testing.T) {
	v := Embed("crypto cash", 8)
	nonzero := []int{}
	for i, x := range v {
		if x != 0.0 {
			nonzero = append(nonzero, i)
		}
	}
	sort.Ints(nonzero)
	if len(nonzero) != 2 || nonzero[0] != 0 || nonzero[1] != 2 {
		t.Fatalf("nonzero buckets = %v, want [0 2]", nonzero)
	}
	for _, i := range nonzero {
		if math.Abs(v[i]-1.0/math.Sqrt(2)) > 1e-9 {
			t.Fatalf("v[%d]=%f want %f", i, v[i], 1.0/math.Sqrt(2))
		}
	}
}

func TestRetrievalRanksCryptoFirst(t *testing.T) {
	retr := BankingRetriever()
	hits := retr.Retrieve(Embed("tell me about crypto cash-back redemption", BankingDim), 4)
	if len(hits) == 0 || hits[0].ID != "kb_cards_crypto" {
		t.Fatalf("top hit = %+v, want kb_cards_crypto", hits)
	}
	if hits[0].Text != BankingKB["kb_cards_crypto"] {
		t.Fatalf("top text mismatch: %q", hits[0].Text)
	}
}

func TestRoutingParity(t *testing.T) {
	rt := NewBankingRuntime()
	cases := []struct{ text, want string }{
		{"what card types do you offer?", "cards"},
		{"what is my balance?", "payments"},
		{"how do I dispute a charge?", "payments"},
		{"hello there", "general"},
		{"tell me about crypto cash-back", "cards"},
	}
	for _, c := range cases {
		res := rt.Submit(NewEvent("c-"+c.text[:4], "demo", c.text))
		if res.Path != c.want {
			t.Fatalf("route(%q)=%s want %s", c.text, res.Path, c.want)
		}
	}
}
