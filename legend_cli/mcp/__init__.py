"""Legend CLI MCP (Model Context Protocol) server.

This module provides an MCP server that enables Claude Desktop to interact
with Legend for model generation and modification.

Usage:
    legend-cli mcp serve

Configuration in Claude Desktop:
    {
        "mcpServers": {
            "legend-cli": {
                "command": "legend-cli",
                "args": ["mcp", "serve"],
                "env": {
                    "LEGEND_SDLC_URL": "http://localhost:6900/sdlc/api",
                    "LEGEND_PAT": "your-personal-access-token"
                }
            }
        }
    }

Note: Requires the 'mcp' optional dependency:
    pip install 'legend-cli[mcp]'
"""

# Always import context and errors (no external dependencies)
from .context import MCPContext, get_context, reset_context, DatabaseType
from .errors import (
    MCPError,
    ConnectionError,
    DatabaseError,
    IntrospectionError,
    GenerationError,
    SDLCError,
    EngineError,
    ValidationError,
    EntityNotFoundError,
    WorkspaceNotFoundError,
    ProjectNotFoundError,
    ModificationError,
)

# Lazy imports for MCP server (requires mcp package)
_server = None
_run_server = None
_main = None


def _ensure_mcp_installed():
    """Ensure MCP package is installed."""
    global _server, _run_server, _main
    if _server is None:
        try:
            from .server import server, run_server, main
            _server = server
            _run_server = run_server
            _main = main
        except ImportError as e:
            raise ImportError(
                "MCP dependencies not installed. Install with: pip install 'legend-cli[mcp]'"
            ) from e
    return _server, _run_server, _main


def __getattr__(name):
    """Lazy loading for MCP server components."""
    if name == "server":
        server, _, _ = _ensure_mcp_installed()
        return server
    elif name == "run_server":
        _, run_server, _ = _ensure_mcp_installed()
        return run_server
    elif name == "main":
        _, _, main = _ensure_mcp_installed()
        return main
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Server (lazy loaded)
    "server",
    "run_server",
    "main",
    # Context
    "MCPContext",
    "get_context",
    "reset_context",
    "DatabaseType",
    # Errors
    "MCPError",
    "ConnectionError",
    "DatabaseError",
    "IntrospectionError",
    "GenerationError",
    "SDLCError",
    "EngineError",
    "ValidationError",
    "EntityNotFoundError",
    "WorkspaceNotFoundError",
    "ProjectNotFoundError",
    "ModificationError",
]
