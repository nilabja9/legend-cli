"""Database operations for MCP tool call logging."""

import sqlite3
import logging
import os
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# SQL schema for MCP tool call logging
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS mcp_tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    session_id TEXT,
    tool_name TEXT NOT NULL,
    parameters TEXT,  -- JSON
    status TEXT,  -- 'started', 'success', 'error'
    result TEXT,  -- JSON (truncated)
    error_message TEXT,
    error_type TEXT,
    duration_ms INTEGER,
    context_data TEXT  -- JSON: project_id, workspace_id
);

CREATE INDEX IF NOT EXISTS idx_tool_calls_timestamp ON mcp_tool_calls(timestamp);
CREATE INDEX IF NOT EXISTS idx_tool_calls_tool_name ON mcp_tool_calls(tool_name);
CREATE INDEX IF NOT EXISTS idx_tool_calls_status ON mcp_tool_calls(status);
CREATE INDEX IF NOT EXISTS idx_tool_calls_session_id ON mcp_tool_calls(session_id);
"""


def get_default_db_path() -> str:
    """Get the default database path (~/.legend-cli/mcp_logs.db)."""
    home = Path.home()
    legend_dir = home / ".legend-cli"
    legend_dir.mkdir(exist_ok=True)
    return str(legend_dir / "mcp_logs.db")


class MCPLogDatabase:
    """SQLite database for MCP tool call logging."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file. If None, uses default.
        """
        self.db_path = db_path or get_default_db_path()
        self._connection: Optional[sqlite3.Connection] = None
        self._initialized = False

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level=None,  # Auto-commit mode
            )
            self._connection.row_factory = sqlite3.Row
        return self._connection

    def initialize(self) -> None:
        """Initialize database schema."""
        if self._initialized:
            return

        try:
            conn = self._get_connection()
            conn.executescript(SCHEMA_SQL)
            self._initialized = True
            logger.debug("MCP logging database initialized at %s", self.db_path)
        except Exception as e:
            logger.error("Failed to initialize MCP logging database: %s", e)
            raise

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def insert_log(
        self,
        tool_name: str,
        session_id: Optional[str] = None,
        parameters: Optional[str] = None,
        status: str = "started",
        context_data: Optional[str] = None,
    ) -> int:
        """Insert a new log entry.

        Args:
            tool_name: Name of the MCP tool being called
            session_id: Optional session identifier
            parameters: JSON string of tool parameters
            status: Initial status ('started')
            context_data: JSON string of context (project_id, workspace_id)

        Returns:
            The row ID of the inserted log entry
        """
        self.initialize()
        conn = self._get_connection()
        cursor = conn.execute(
            """
            INSERT INTO mcp_tool_calls (tool_name, session_id, parameters, status, context_data)
            VALUES (?, ?, ?, ?, ?)
            """,
            (tool_name, session_id, parameters, status, context_data),
        )
        return cursor.lastrowid

    def update_log_success(
        self,
        log_id: int,
        result: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> None:
        """Update log entry with successful result.

        Args:
            log_id: ID of the log entry to update
            result: JSON string of the result (may be truncated)
            duration_ms: Execution duration in milliseconds
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE mcp_tool_calls
            SET status = 'success', result = ?, duration_ms = ?
            WHERE id = ?
            """,
            (result, duration_ms, log_id),
        )

    def update_log_error(
        self,
        log_id: int,
        error_message: str,
        error_type: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> None:
        """Update log entry with error information.

        Args:
            log_id: ID of the log entry to update
            error_message: Error message
            error_type: Type/class of the error
            duration_ms: Execution duration in milliseconds
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE mcp_tool_calls
            SET status = 'error', error_message = ?, error_type = ?, duration_ms = ?
            WHERE id = ?
            """,
            (error_message, error_type, duration_ms, log_id),
        )

    def query_logs(
        self,
        tool_name: Optional[str] = None,
        status: Optional[str] = None,
        session_id: Optional[str] = None,
        since_hours: int = 24,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Query log entries with optional filters.

        Args:
            tool_name: Filter by tool name
            status: Filter by status ('success', 'error')
            session_id: Filter by session ID
            since_hours: Look back N hours (default 24)
            limit: Maximum number of results (default 100)
            offset: Offset for pagination

        Returns:
            List of log entries as dictionaries
        """
        self.initialize()
        conn = self._get_connection()

        conditions = []
        params = []

        # Time filter
        since_time = datetime.utcnow() - timedelta(hours=since_hours)
        conditions.append("timestamp >= ?")
        params.append(since_time.isoformat())

        if tool_name:
            conditions.append("tool_name = ?")
            params.append(tool_name)

        if status:
            conditions.append("status = ?")
            params.append(status)

        if session_id:
            conditions.append("session_id = ?")
            params.append(session_id)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.extend([limit, offset])

        query = f"""
            SELECT * FROM mcp_tool_calls
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def get_stats(self, since_hours: int = 24) -> Dict[str, Any]:
        """Get statistics about tool calls.

        Args:
            since_hours: Look back N hours

        Returns:
            Dict with statistics
        """
        self.initialize()
        conn = self._get_connection()

        since_time = datetime.utcnow() - timedelta(hours=since_hours)

        # Total counts
        cursor = conn.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
                AVG(duration_ms) as avg_duration_ms
            FROM mcp_tool_calls
            WHERE timestamp >= ?
            """,
            (since_time.isoformat(),),
        )
        row = cursor.fetchone()

        # Counts by tool
        cursor = conn.execute(
            """
            SELECT tool_name, COUNT(*) as count,
                   SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
                   SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors
            FROM mcp_tool_calls
            WHERE timestamp >= ?
            GROUP BY tool_name
            ORDER BY count DESC
            """,
            (since_time.isoformat(),),
        )
        tool_stats = [dict(r) for r in cursor.fetchall()]

        return {
            "total_calls": row["total"] or 0,
            "success_count": row["success_count"] or 0,
            "error_count": row["error_count"] or 0,
            "avg_duration_ms": round(row["avg_duration_ms"] or 0, 2),
            "since_hours": since_hours,
            "by_tool": tool_stats,
        }

    def cleanup_old_logs(self, retention_days: int = 30) -> int:
        """Delete logs older than retention period.

        Args:
            retention_days: Number of days to retain logs

        Returns:
            Number of deleted rows
        """
        self.initialize()
        conn = self._get_connection()

        cutoff_time = datetime.utcnow() - timedelta(days=retention_days)
        cursor = conn.execute(
            """
            DELETE FROM mcp_tool_calls
            WHERE timestamp < ?
            """,
            (cutoff_time.isoformat(),),
        )

        deleted = cursor.rowcount
        if deleted > 0:
            logger.info("Cleaned up %d old log entries", deleted)

        return deleted

    def __enter__(self):
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
