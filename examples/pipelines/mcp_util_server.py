"""A tiny stdio MCP server for tools-mcp.yaml and banking-mcp.yaml. Needs the MCP SDK: pip install "mcp<2"."""
from mcp.server.fastmcp import FastMCP

server = FastMCP("util")


@server.tool()
def util_add(a: float, b: float) -> float:
    """Add two numbers."""
    return a + b


@server.tool()
def util_toUpperCase(text: str) -> str:
    """Upper-case a string."""
    return text.upper()


if __name__ == "__main__":
    server.run()
