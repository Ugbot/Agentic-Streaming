package pipeline

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Ugbot/Agentic-Streaming/ports/experimental/go/core"
)

// overlappingSpec declares two routes whose keywords both appear in the probe text. The
// declaration order of first and second is the only thing that decides the route.
func overlappingSpec(first, second, sharedKeyword string) string {
	return fmt.Sprintf(`backend: local
agent:
  router:
    kind: keyword
    default: general
    rules:
      %[1]s: [%[3]s]
      %[2]s: [%[3]s]
  paths:
    %[1]s: {brain: rule, prompt: You answer %[1]s questions.}
    %[2]s: {brain: rule, prompt: You answer %[2]s questions.}
    general: {brain: rule, prompt: You answer general questions.}
  verifier: {kind: prefix}
`, first, second, sharedKeyword)
}

func randomWord(r *rand.Rand, prefix string) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	var b strings.Builder
	b.WriteString(prefix)
	for i := 0; i < 6; i++ {
		b.WriteByte(letters[r.Intn(len(letters))])
	}
	return b.String()
}

func buildFromYAML(t *testing.T, doc string) *System {
	t.Helper()
	spec, err := Parse([]byte(doc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sys, err := BuildSystem(spec, "local")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return sys
}

// TestRouterFirstDeclaredRuleWins builds the same two-route spec in both declaration
// orders. Both routes match the text, so a router that iterated a Go map would pick
// either; the spec (workflow.schema.json router.rules) requires the first declared path.
func TestRouterFirstDeclaredRuleWins(t *testing.T) {
	r := rand.New(rand.NewSource(rand.Int63()))
	a, b, kw := randomWord(r, "a"), randomWord(r, "b"), randomWord(r, "kw")
	text := fmt.Sprintf("please handle %s today", strings.ToUpper(kw))
	for i := 0; i < 20; i++ {
		if got := buildFromYAML(t, overlappingSpec(a, b, kw)).Submit(core.Event{ConversationID: "c1", UserID: "u", Text: text}).Path; got != a {
			t.Fatalf("iteration %d: rules declared [%s, %s], routed to %s", i, a, b, got)
		}
		if got := buildFromYAML(t, overlappingSpec(b, a, kw)).Submit(core.Event{ConversationID: "c1", UserID: "u", Text: text}).Path; got != b {
			t.Fatalf("iteration %d: rules declared [%s, %s], routed to %s", i, b, a, got)
		}
	}
}

// TestToolTriggersFirstDeclaredWins covers the second ordered mapping of the spec: a rule
// brain with two triggers on the same keyword invokes the first declared tool.
func TestToolTriggersFirstDeclaredWins(t *testing.T) {
	r := rand.New(rand.NewSource(rand.Int63()))
	kw := randomWord(r, "kw")
	doc := fmt.Sprintf(`backend: local
agent:
  router: {kind: keyword, default: only}
  paths:
    only:
      brain: rule
      tool_triggers:
        %[1]s: second_tool
        %[1]s-more: first_tool
  verifier: {kind: prefix}
tools:
  - {id: first_tool, kind: constant, description: first, value: 1}
  - {id: second_tool, kind: constant, description: second, value: 2}
`, kw)
	res := buildFromYAML(t, doc).Submit(core.Event{ConversationID: "c1", UserID: "u", Text: "run " + kw + "-more now"})
	if len(res.ToolCalls) != 1 || res.ToolCalls[0] != "second_tool" {
		t.Fatalf("expected the first declared trigger (second_tool) to fire once, got %v", res.ToolCalls)
	}
}

// bankingProbes are turns whose expected route the Python reference runtime (the golden
// oracle for spec/v1) produces for examples/pipelines/banking.yaml. The second probe
// matches both the cards and the payments rules; cards is declared first.
var bankingProbes = []struct{ text, path string }{
	{"what is my balance?", "payments"},
	{"dispute a charge on my card", "cards"},
	{"tell me about crypto cash-back", "cards"},
	{"hello there", "general"},
	{"raise my transfer limit", "payments"},
}

func TestBankingYamlRoutesOverlappingTurnToFirstDeclaredPath(t *testing.T) {
	sys := loadShared(t, banking, "local")
	for i, probe := range bankingProbes {
		res := sys.Submit(core.Event{ConversationID: fmt.Sprintf("c%d", i), UserID: "demo", Text: probe.text})
		if res.Path != probe.path {
			t.Fatalf("%q routed to %s, spec order requires %s", probe.text, res.Path, probe.path)
		}
	}
}

// TestBankingYamlMatchesReferenceRuntime runs the same probes through
// spec/tools/reference_runtime.py and compares the chosen path turn by turn. It needs a
// python3 with PyYAML; when that is missing it skips and prints why.
func TestBankingYamlMatchesReferenceRuntime(t *testing.T) {
	sys := loadShared(t, banking, "local")
	repoRoot, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skipf("python3 not on PATH; reference runtime comparison not run: %v", err)
	}
	script := `import sys, yaml
sys.path.insert(0, sys.argv[1])
from reference_runtime import ReferenceRuntime, Turn
rt = ReferenceRuntime(yaml.safe_load(open(sys.argv[2])))
for i, text in enumerate(sys.argv[3:]):
    print(rt.submit(Turn(conversation_id=f"c{i}", turn_id=f"t{i}", text=text, user_id="demo"))["path"])
`
	args := []string{"-c", script, filepath.Join(repoRoot, "spec", "tools"), filepath.Join(repoRoot, "examples", "pipelines", "banking.yaml")}
	for _, probe := range bankingProbes {
		args = append(args, probe.text)
	}
	cmd := exec.Command(python, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if strings.Contains(stderr.String(), "No module named 'yaml'") {
			t.Skipf("python3 has no PyYAML (pip install pyyaml); reference runtime comparison not run")
		}
		t.Fatalf("reference runtime failed: %v\n%s", err, stderr.String())
	}
	got := strings.Fields(stdout.String())
	if len(got) != len(bankingProbes) {
		t.Fatalf("reference runtime printed %d paths for %d probes: %q", len(got), len(bankingProbes), stdout.String())
	}
	for i, probe := range bankingProbes {
		res := sys.Submit(core.Event{ConversationID: fmt.Sprintf("c%d", i), UserID: "demo", Text: probe.text})
		if res.Path != got[i] {
			t.Fatalf("%q: go routed to %s, reference runtime routed to %s", probe.text, res.Path, got[i])
		}
	}
}

// TestKeyedEventConstructionCarriesUserIdToTools guards the Event argument-order finding:
// adapter call sites build events with keyed fields, and this proves the user id (not the
// text) is what the rule brain hands to the tool as {"user": ...}.
func TestKeyedEventConstructionCarriesUserIdToTools(t *testing.T) {
	sys := loadShared(t, banking, "local")
	r := rand.New(rand.NewSource(rand.Int63()))
	userID := randomWord(r, "user-")
	var seenUser any
	sys.Built.Tools.Register("get_balance", "records the user argument", func(p map[string]any) any {
		seenUser = p["user"]
		return 1
	})
	res := sys.Submit(core.Event{ConversationID: "c1", UserID: userID, Text: "what is my balance?"})
	if !contains(res.ToolCalls, "get_balance") {
		t.Fatalf("expected get_balance call, got %+v", res)
	}
	if seenUser != userID {
		t.Fatalf("tool received user=%v, want %s", seenUser, userID)
	}
}

// TestParseRejectsMalformedRules checks the ordered decoder reports the offending path.
func TestParseRejectsMalformedRules(t *testing.T) {
	_, err := Parse([]byte("agent:\n  router:\n    rules:\n      cards: not-a-list\n  paths:\n    cards: {}\n"))
	if err == nil || !strings.Contains(err.Error(), "agent.router.rules.cards") {
		t.Fatalf("expected an error naming agent.router.rules.cards, got %v", err)
	}
	data, err := os.ReadFile(banking)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	rules := asRules(asMap(asMap(spec["agent"])["router"])["rules"])
	if len(rules) != 2 || rules[0].Path != "cards" || rules[1].Path != "payments" {
		t.Fatalf("banking.yaml rules decoded out of order: %+v", rules)
	}
}
