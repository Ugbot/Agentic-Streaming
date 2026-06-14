package stores

import (
	"context"
	"strings"
	"time"

	mcpclient "github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/jagentic/goagentic/core"
)

// McpClient connects to a stdio MCP server and registers its tools into a core
// ToolRegistry, so an agent can call external MCP tool servers (mark3labs/mcp-go).
type McpClient struct {
	client *mcpclient.Client
	specs  []map[string]string
}

// NewMcpClient launches a stdio MCP server (command + args) and initializes the session.
func NewMcpClient(command string, env []string, args ...string) (*McpClient, error) {
	c, err := mcpclient.NewStdioMCPClient(command, env, args...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "goagentic", Version: "0.1.0"}
	if _, err := c.Initialize(ctx, initReq); err != nil {
		_ = c.Close()
		return nil, err
	}
	listed, err := c.ListTools(ctx, mcp.ListToolsRequest{})
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	mc := &McpClient{client: c}
	for _, t := range listed.Tools {
		mc.specs = append(mc.specs, map[string]string{"name": t.Name, "description": t.Description})
	}
	return mc, nil
}

// Tools returns the discovered MCP tool specs.
func (m *McpClient) Tools() []map[string]string { return m.specs }

func (m *McpClient) call(name string, args map[string]any) any {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := mcp.CallToolRequest{}
	req.Params.Name = name
	req.Params.Arguments = args
	res, err := m.client.CallTool(ctx, req)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	var parts []string
	for _, c := range res.Content {
		if tc, ok := mcp.AsTextContent(c); ok {
			parts = append(parts, tc.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// Register registers every MCP tool into the registry (optionally id-prefixed); returns ids.
func (m *McpClient) Register(reg *core.ToolRegistry, prefix string) []string {
	var ids []string
	for _, spec := range m.specs {
		name := spec["name"]
		id := prefix + name
		reg.Register(id, spec["description"], func(params map[string]any) any { return m.call(name, params) })
		ids = append(ids, id)
	}
	return ids
}

// Close shuts the MCP server down.
func (m *McpClient) Close() error { return m.client.Close() }
