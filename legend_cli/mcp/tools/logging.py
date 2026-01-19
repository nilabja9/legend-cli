"""Logging MCP tools for Legend CLI.

Provides tools for querying MCP tool call logs for debugging and monitoring.
"""

import json
import logging
from typing import Any, List, Optional

from mcp.types import Tool

from ..context import MCPContext

logger = logging.getLogger(__name__)


def get_tools() -> List[Tool]:
    """Return all logging-related tools."""
    return [
        Tool(
            name="query_mcp_logs",
            description="Query MCP tool call logs for debugging. Returns recent tool calls with optional filtering by tool name, status, or time range.",
            inputSchema={
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "description": "Filter by tool name (e.g., 'push_artifacts', 'validate_pure_code')"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["success", "error"],
                        "description": "Filter by status"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 20,
                        "description": "Maximum number of results to return (default 20)"
                    },
                    "since_hours": {
                        "type": "integer",
                        "default": 24,
                        "description": "Look back N hours (default 24)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_mcp_log_stats",
            description="Get statistics about MCP tool call usage, including success/error rates and performance metrics.",
            inputSchema={
                "type": "object",
                "properties": {
                    "since_hours": {
                        "type": "integer",
                        "default": 24,
                        "description": "Look back N hours (default 24)"
                    }
                },
                "required": []
            }
        ),
    ]


async def query_mcp_logs(
    ctx: MCPContext,
    tool_name: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20,
    since_hours: int = 24,
) -> str:
    """Query MCP tool call logs."""
    try:
        from ..server import get_log_service

        log_service = get_log_service()
        if not log_service or not log_service.enabled:
            return json.dumps({
                "status": "disabled",
                "message": "MCP logging is not enabled. Set MCP_LOGGING_ENABLED=true to enable."
            })

        logs = await log_service.query_logs(
            tool_name=tool_name,
            status=status,
            since_hours=since_hours,
            limit=limit,
        )

        # Format logs for display
        formatted_logs = []
        for log in logs:
            formatted_log = {
                "id": log.get("id"),
                "timestamp": log.get("timestamp"),
                "tool_name": log.get("tool_name"),
                "status": log.get("status"),
                "duration_ms": log.get("duration_ms"),
            }

            if log.get("error_message"):
                formatted_log["error"] = {
                    "message": log.get("error_message"),
                    "type": log.get("error_type"),
                }

            # Include truncated parameters if present
            if log.get("parameters"):
                params = log.get("parameters")
                if len(params) > 200:
                    params = params[:200] + "..."
                formatted_log["parameters"] = params

            formatted_logs.append(formatted_log)

        return json.dumps({
            "status": "success",
            "count": len(formatted_logs),
            "since_hours": since_hours,
            "filters": {
                "tool_name": tool_name,
                "status": status,
            },
            "logs": formatted_logs,
            "message": f"Found {len(formatted_logs)} log entries"
        }, indent=2)

    except Exception as e:
        logger.exception("Error querying MCP logs")
        return json.dumps({
            "status": "error",
            "message": f"Failed to query logs: {str(e)}"
        })


async def get_mcp_log_stats(
    ctx: MCPContext,
    since_hours: int = 24,
) -> str:
    """Get MCP tool call statistics."""
    try:
        from ..server import get_log_service

        log_service = get_log_service()
        if not log_service or not log_service.enabled:
            return json.dumps({
                "status": "disabled",
                "message": "MCP logging is not enabled. Set MCP_LOGGING_ENABLED=true to enable."
            })

        stats = await log_service.get_stats(since_hours=since_hours)

        return json.dumps({
            "status": "success",
            **stats,
            "message": f"Statistics for the last {since_hours} hours"
        }, indent=2)

    except Exception as e:
        logger.exception("Error getting MCP log stats")
        return json.dumps({
            "status": "error",
            "message": f"Failed to get stats: {str(e)}"
        })
