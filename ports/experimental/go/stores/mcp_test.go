package stores

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jagentic/goagentic/core"
)

const stubMCPServer = `
from mcp.server.fastmcp import FastMCP
mcp = FastMCP("stub")

@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b

if __name__ == "__main__":
    mcp.run()
`

func TestMcpClientRegistersAndCallsToolsIfAvailable(t *testing.T) {
	py := os.Getenv("AGENTIC_TEST_PYTHON")
	if py == "" {
		py = "/tmp/af-venv/bin/python"
	}
	if _, err := os.Stat(py); err != nil {
		t.Skipf("python with mcp SDK not available at %s", py)
	}
	stub := filepath.Join(t.TempDir(), "stub_mcp_server.py")
	if err := os.WriteFile(stub, []byte(stubMCPServer), 0o644); err != nil {
		t.Fatal(err)
	}
	client, err := NewMcpClient(py, nil, stub)
	if err != nil {
		t.Skipf("MCP stub server didn't start (mcp SDK missing?): %v", err)
	}
	defer client.Close()

	names := map[string]bool{}
	for _, s := range client.Tools() {
		names[s["name"]] = true
	}
	if !names["add"] {
		t.Fatalf("expected 'add' tool, got %v", client.Tools())
	}
	reg := core.NewToolRegistry()
	ids := client.Register(reg, "mcp_")
	if len(ids) == 0 || !reg.Has("mcp_add") {
		t.Fatalf("register ids = %v", ids)
	}
	got := strings.TrimSpace(reg.Execute("mcp_add", map[string]any{"a": 2, "b": 3}).(string))
	if got != "5" {
		t.Fatalf("mcp_add(2,3) = %q, want 5", got)
	}
}
