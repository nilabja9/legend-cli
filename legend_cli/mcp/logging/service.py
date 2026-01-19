"""MCP tool call logging service."""

import json
import logging
import time
import uuid
from typing import Optional, Any, Dict, List

from .db import MCPLogDatabase

logger = logging.getLogger(__name__)


class MCPLogService:
    """Service for logging MCP tool calls to database.

    Provides async-compatible methods for logging tool invocations,
    results, and errors.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        retention_days: int = 30,
        max_result_size: int = 10000,
        enabled: bool = True,
    ):
        """Initialize the logging service.

        Args:
            db_path: Path to SQLite database. If None, uses default.
            retention_days: Number of days to retain logs.
            max_result_size: Maximum size of result/parameter strings to store.
            enabled: Whether logging is enabled.
        """
        self.db = MCPLogDatabase(db_path) if enabled else None
        self.retention_days = retention_days
        self.max_result_size = max_result_size
        self.enabled = enabled
        self._session_id: Optional[str] = None

    @property
    def session_id(self) -> str:
        """Get or generate session ID."""
        if self._session_id is None:
            self._session_id = str(uuid.uuid4())[:8]
        return self._session_id

    def set_session_id(self, session_id: str) -> None:
        """Set the session ID."""
        self._session_id = session_id

    def _truncate(self, text: str) -> str:
        """Truncate text to max_result_size."""
        if len(text) > self.max_result_size:
            return text[: self.max_result_size - 20] + "...[TRUNCATED]"
        return text

    def _safe_json(self, obj: Any) -> str:
        """Safely convert object to JSON string."""
        try:
            return json.dumps(obj, default=str)
        except Exception:
            return str(obj)

    async def log_tool_start(
        self,
        tool_name: str,
        params: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Log the start of a tool call.

        Args:
            tool_name: Name of the MCP tool
            params: Tool parameters
            context: Additional context (project_id, workspace_id, etc.)

        Returns:
            Log entry ID for tracking
        """
        if not self.enabled or self.db is None:
            return -1

        try:
            params_json = self._truncate(self._safe_json(params)) if params else None
            context_json = self._safe_json(context) if context else None

            log_id = self.db.insert_log(
                tool_name=tool_name,
                session_id=self.session_id,
                parameters=params_json,
                status="started",
                context_data=context_json,
            )

            logger.debug("Logged tool start: %s (id=%d)", tool_name, log_id)
            return log_id

        except Exception as e:
            logger.warning("Failed to log tool start: %s", e)
            return -1

    async def log_tool_success(
        self,
        log_id: int,
        result: Any,
        duration_ms: int,
    ) -> None:
        """Log successful tool completion.

        Args:
            log_id: ID from log_tool_start
            result: Tool result
            duration_ms: Execution duration in milliseconds
        """
        if not self.enabled or self.db is None or log_id < 0:
            return

        try:
            result_json = self._truncate(self._safe_json(result))
            self.db.update_log_success(
                log_id=log_id,
                result=result_json,
                duration_ms=duration_ms,
            )
            logger.debug("Logged tool success: id=%d, duration=%dms", log_id, duration_ms)

        except Exception as e:
            logger.warning("Failed to log tool success: %s", e)

    async def log_tool_error(
        self,
        log_id: int,
        error: Exception,
        duration_ms: int,
    ) -> None:
        """Log tool error.

        Args:
            log_id: ID from log_tool_start
            error: Exception that occurred
            duration_ms: Execution duration in milliseconds
        """
        if not self.enabled or self.db is None or log_id < 0:
            return

        try:
            error_message = str(error)[:1000]  # Truncate error message
            error_type = type(error).__name__

            self.db.update_log_error(
                log_id=log_id,
                error_message=error_message,
                error_type=error_type,
                duration_ms=duration_ms,
            )
            logger.debug("Logged tool error: id=%d, type=%s", log_id, error_type)

        except Exception as e:
            logger.warning("Failed to log tool error: %s", e)

    async def query_logs(
        self,
        tool_name: Optional[str] = None,
        status: Optional[str] = None,
        since_hours: int = 24,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Query recent tool call logs.

        Args:
            tool_name: Filter by tool name
            status: Filter by status ('success', 'error')
            since_hours: Look back N hours
            limit: Maximum results to return

        Returns:
            List of log entries
        """
        if not self.enabled or self.db is None:
            return []

        try:
            return self.db.query_logs(
                tool_name=tool_name,
                status=status,
                since_hours=since_hours,
                limit=limit,
            )
        except Exception as e:
            logger.warning("Failed to query logs: %s", e)
            return []

    async def get_stats(self, since_hours: int = 24) -> Dict[str, Any]:
        """Get tool call statistics.

        Args:
            since_hours: Look back N hours

        Returns:
            Statistics dictionary
        """
        if not self.enabled or self.db is None:
            return {"enabled": False}

        try:
            stats = self.db.get_stats(since_hours=since_hours)
            stats["enabled"] = True
            stats["session_id"] = self.session_id
            return stats
        except Exception as e:
            logger.warning("Failed to get stats: %s", e)
            return {"enabled": True, "error": str(e)}

    async def cleanup_old_logs(self) -> int:
        """Delete logs older than retention period.

        Returns:
            Number of deleted logs
        """
        if not self.enabled or self.db is None:
            return 0

        try:
            return self.db.cleanup_old_logs(retention_days=self.retention_days)
        except Exception as e:
            logger.warning("Failed to cleanup logs: %s", e)
            return 0

    def close(self) -> None:
        """Close database connection."""
        if self.db:
            self.db.close()


class ToolCallLogger:
    """Context manager for logging a single tool call."""

    def __init__(
        self,
        service: MCPLogService,
        tool_name: str,
        params: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the logger.

        Args:
            service: MCPLogService instance
            tool_name: Name of the tool being called
            params: Tool parameters
            context: Additional context
        """
        self.service = service
        self.tool_name = tool_name
        self.params = params
        self.context = context
        self.log_id = -1
        self.start_time: Optional[float] = None

    async def __aenter__(self) -> "ToolCallLogger":
        """Start logging the tool call."""
        self.start_time = time.time()
        self.log_id = await self.service.log_tool_start(
            tool_name=self.tool_name,
            params=self.params,
            context=self.context,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Complete logging based on outcome."""
        if self.start_time is None:
            return

        duration_ms = int((time.time() - self.start_time) * 1000)

        if exc_val is not None:
            await self.service.log_tool_error(
                log_id=self.log_id,
                error=exc_val,
                duration_ms=duration_ms,
            )
        # Note: success is logged separately with the result

    async def log_success(self, result: Any) -> None:
        """Log successful completion with result.

        Args:
            result: The tool result
        """
        if self.start_time is None:
            return

        duration_ms = int((time.time() - self.start_time) * 1000)
        await self.service.log_tool_success(
            log_id=self.log_id,
            result=result,
            duration_ms=duration_ms,
        )
