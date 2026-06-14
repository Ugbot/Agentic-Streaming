package core

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const webPage = `<html><head><title>Acme Bank</title><style>.x{color:red}</style></head>` +
	`<body><h1>Welcome</h1><p>Open a savings account today.</p>` +
	`<script>var x=1;</script>` +
	`<a href="/cards">Cards</a> <a href="https://example.org/help">Help</a></body></html>`

func webServer(robots string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(robots))
		default:
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(webPage))
		}
	}))
}

func TestWebFetchExtractsTextAndLinks(t *testing.T) {
	srv := webServer("User-agent: *\nAllow: /\n")
	defer srv.Close()

	f := NewWebFetcher()
	res, err := f.Fetch(srv.URL + "/page")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if res.Title != "Acme Bank" {
		t.Fatalf("title = %q", res.Title)
	}
	if !strings.Contains(res.Text, "Open a savings account today.") {
		t.Fatalf("text missing body: %q", res.Text)
	}
	if strings.Contains(res.Text, "var x") || strings.Contains(res.Text, "color:red") {
		t.Fatalf("script/style leaked into text: %q", res.Text)
	}
	foundRel, foundAbs := false, false
	for _, l := range res.Links {
		if l == srv.URL+"/cards" {
			foundRel = true
		}
		if l == "https://example.org/help" {
			foundAbs = true
		}
	}
	if !foundRel || !foundAbs {
		t.Fatalf("links = %v", res.Links)
	}
}

func TestWebToolsRegisterAndRun(t *testing.T) {
	srv := webServer("User-agent: *\nAllow: /\n")
	defer srv.Close()

	reg := NewToolRegistry()
	ids := RegisterWebTools(reg)
	if len(ids) != 2 || !reg.Has("web_fetch") || !reg.Has("web_links") {
		t.Fatalf("register ids = %v", ids)
	}
	text := reg.Execute("web_fetch", map[string]any{"url": srv.URL + "/page"}).(string)
	if !strings.Contains(text, "Acme Bank") || !strings.Contains(text, "savings account") {
		t.Fatalf("web_fetch = %q", text)
	}
	links := reg.Execute("web_links", map[string]any{"url": srv.URL + "/page"}).(string)
	if !strings.Contains(links, "https://example.org/help") {
		t.Fatalf("web_links = %q", links)
	}
}

func TestRobotsDisallowBlocksFetch(t *testing.T) {
	srv := webServer("User-agent: *\nDisallow: /blocked\n")
	defer srv.Close()

	f := NewWebFetcher()
	if _, err := f.Fetch(srv.URL + "/blocked"); err == nil {
		t.Fatal("expected robots disallow error")
	}
	if _, err := f.Fetch(srv.URL + "/page"); err != nil {
		t.Fatalf("allowed path should fetch: %v", err)
	}
}

func TestLexiconClassifier(t *testing.T) {
	clf := NewLexiconClassifier(map[string][]string{
		"billing": {"invoice", "charge", "refund", "payment"},
		"tech":    {"error", "crash", "bug", "login"},
	}, "other")
	if got := clf.Classify("I want a refund for this charge").Label; got != "billing" {
		t.Fatalf("billing => %q", got)
	}
	if got := clf.Classify("the app keeps showing a login error").Label; got != "tech" {
		t.Fatalf("tech => %q", got)
	}
	if got := clf.Classify("hello there").Label; got != "other" {
		t.Fatalf("other => %q", got)
	}
	c := clf.Classify("refund my payment please")
	sum := 0.0
	for _, v := range c.Scores {
		sum += v
	}
	if sum < 0.999 || sum > 1.001 {
		t.Fatalf("scores not a distribution: %v (sum %f)", c.Scores, sum)
	}
}

func TestEmbeddingClassifierNearestCentroid(t *testing.T) {
	clf, err := NewEmbeddingClassifier(nil, 0).Fit(map[string][]string{
		"billing": {"refund my invoice", "dispute a charge", "payment failed"},
		"tech":    {"app crashed on login", "error message bug", "cannot sign in"},
	})
	if err != nil {
		t.Fatalf("fit: %v", err)
	}
	if got := clf.Classify("please refund this invoice charge").Label; got != "billing" {
		t.Fatalf("billing => %q", got)
	}
	if got := clf.Classify("login error keeps crashing").Label; got != "tech" {
		t.Fatalf("tech => %q", got)
	}
}

func TestClassifierGuardrailBlocksLabel(t *testing.T) {
	clf := NewLexiconClassifier(map[string][]string{
		"toxic": {"idiot", "stupid", "hate"},
		"ok":    {"please", "thanks", "help"},
	}, "ok")
	guard := NewClassifierGuardrail(clf, []string{"toxic"}, 0.3, "", false)
	if guard.CheckInput("you stupid idiot") == "" {
		t.Fatal("expected toxic block")
	}
	if guard.CheckInput("please help, thanks") != "" {
		t.Fatal("benign text should pass")
	}
	scorer := NewClassifierScorer(clf, "toxic")
	if scorer.Score("idiot") <= scorer.Score("thanks please help") {
		t.Fatal("toxic score should exceed benign")
	}
}
