package core

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// Web toolkit (Tier 4) — a robots-aware fetch + text/link extraction tool built on the
// standard library plus golang.org/x/net/html. Ships as an opt-in tool, default off: call
// RegisterWebTools(registry) to expose web_fetch / web_links to an agent. Mirrors the
// Flink web/ toolkit (Jsoup + crawler-commons robots) at a portable subset.

const defaultUserAgent = "jagentic-webfetch/0.1 (+https://github.com/jagentic)"

var skipTags = map[string]bool{"script": true, "style": true, "noscript": true, "template": true}

// FetchResult is the outcome of a web fetch.
type FetchResult struct {
	URL    string
	Status int
	Title  string
	Text   string
	Links  []string
}

// WebFetcher fetches and extracts pages, honouring robots.txt unless disabled.
type WebFetcher struct {
	UserAgent     string
	Timeout       time.Duration
	MaxBytes      int64
	RespectRobots bool
	client        *http.Client
}

// NewWebFetcher builds a fetcher with sane defaults (robots respected).
func NewWebFetcher() *WebFetcher {
	return &WebFetcher{
		UserAgent:     defaultUserAgent,
		Timeout:       15 * time.Second,
		MaxBytes:      2_000_000,
		RespectRobots: true,
		client:        &http.Client{Timeout: 15 * time.Second},
	}
}

// Fetch retrieves url and extracts its title, text and absolute links.
func (w *WebFetcher) Fetch(rawURL string) (*FetchResult, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("unsupported URL: %q", rawURL)
	}
	if w.RespectRobots {
		allowed, _ := w.robotsAllows(u)
		if !allowed {
			return nil, fmt.Errorf("robots.txt disallows fetching %s", rawURL)
		}
	}
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", w.UserAgent)
	req.Header.Set("Accept", "text/html,*/*")
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, w.MaxBytes))
	if err != nil {
		return nil, err
	}
	doc, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	res := &FetchResult{URL: resp.Request.URL.String(), Status: resp.StatusCode}
	extract(doc, resp.Request.URL, res, false)
	res.Text = strings.TrimSpace(res.Text)
	res.Title = strings.TrimSpace(res.Title)
	return res, nil
}

// extract walks the parse tree collecting visible text, the title, and absolute links.
func extract(n *html.Node, base *url.URL, res *FetchResult, inTitle bool) {
	if n.Type == html.ElementNode {
		tag := strings.ToLower(n.Data)
		if skipTags[tag] {
			return
		}
		if tag == "title" {
			inTitle = true
		}
		if tag == "a" {
			for _, a := range n.Attr {
				if strings.ToLower(a.Key) == "href" && a.Val != "" {
					if ref, err := base.Parse(strings.TrimSpace(a.Val)); err == nil {
						ref.Fragment = ""
						if ref.Scheme == "http" || ref.Scheme == "https" {
							res.appendLink(ref.String())
						}
					}
				}
			}
		}
	}
	if n.Type == html.TextNode {
		t := strings.TrimSpace(n.Data)
		if t != "" {
			if inTitle {
				if res.Title != "" {
					res.Title += " "
				}
				res.Title += t
			} else {
				if res.Text != "" {
					res.Text += "\n"
				}
				res.Text += t
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		extract(c, base, res, inTitle)
	}
}

func (r *FetchResult) appendLink(link string) {
	for _, l := range r.Links {
		if l == link {
			return
		}
	}
	r.Links = append(r.Links, link)
}

// robotsAllows checks the site's robots.txt; fail-open if unreachable, fail-closed only on
// an explicit Disallow match for our user-agent (or *).
func (w *WebFetcher) robotsAllows(u *url.URL) (bool, error) {
	robotsURL := u.Scheme + "://" + u.Host + "/robots.txt"
	req, err := http.NewRequest(http.MethodGet, robotsURL, nil)
	if err != nil {
		return true, nil
	}
	req.Header.Set("User-Agent", w.UserAgent)
	resp, err := w.client.Do(req)
	if err != nil {
		return true, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return true, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512_000))
	if err != nil {
		return true, nil
	}
	return robotsCanFetch(string(body), u.Path), nil
}

// robotsCanFetch is a minimal robots.txt evaluator: it gathers Disallow rules under the
// "*" user-agent group and denies if the path has a matching prefix. Allow rules override.
func robotsCanFetch(robots, path string) bool {
	if path == "" {
		path = "/"
	}
	var disallow, allow []string
	applies := false
	for _, line := range strings.Split(robots, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)
		switch key {
		case "user-agent":
			applies = val == "*"
		case "disallow":
			if applies && val != "" {
				disallow = append(disallow, val)
			}
		case "allow":
			if applies && val != "" {
				allow = append(allow, val)
			}
		}
	}
	for _, a := range allow {
		if strings.HasPrefix(path, a) {
			return true
		}
	}
	for _, d := range disallow {
		if strings.HasPrefix(path, d) {
			return false
		}
	}
	return true
}

// RegisterWebTools registers web_fetch (title+text) and web_links (link list) into the
// registry. Opt-in. Returns the registered tool ids.
func RegisterWebTools(reg *ToolRegistry) []string {
	fetcher := NewWebFetcher()
	reg.Register("web_fetch", "Fetch a web page (robots-aware) and return its title + text.",
		func(params map[string]any) any {
			res, err := fetcher.Fetch(toStr(params["url"]))
			if err != nil {
				return map[string]any{"error": err.Error()}
			}
			if res.Title != "" {
				return res.Title + "\n\n" + res.Text
			}
			return res.Text
		})
	reg.Register("web_links", "Fetch a web page and return its outbound links, one per line.",
		func(params map[string]any) any {
			res, err := fetcher.Fetch(toStr(params["url"]))
			if err != nil {
				return map[string]any{"error": err.Error()}
			}
			return strings.Join(res.Links, "\n")
		})
	return []string{"web_fetch", "web_links"}
}

func toStr(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
