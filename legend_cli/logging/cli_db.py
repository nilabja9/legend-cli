"""Database operations for CLI run logging."""

import sqlite3
import logging
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# SQL schema for CLI run logging
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cli_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    command TEXT NOT NULL,
    subcommand TEXT,
    database_type TEXT,
    database_path TEXT,
    schema_filter TEXT,
    doc_sources TEXT,  -- JSON array
    arguments TEXT,  -- JSON of all arguments
    status TEXT DEFAULT 'started',  -- 'started', 'success', 'error', 'interrupted'
    duration_ms INTEGER,

    -- Introspection results
    schemas_count INTEGER,
    tables_count INTEGER,
    columns_count INTEGER,

    -- Relationship results
    pattern_relationships INTEGER,
    document_relationships INTEGER,
    total_relationships INTEGER,

    -- Generation results
    artifacts_generated TEXT,  -- JSON array of artifact types
    classes_generated INTEGER,
    associations_generated INTEGER,

    -- SDLC results
    project_id INTEGER,
    project_name TEXT,
    workspace_id TEXT,
    push_status TEXT,  -- 'success', 'skipped', 'error'

    -- Enhanced analysis results
    enhanced_mode BOOLEAN DEFAULT FALSE,
    enums_detected INTEGER,
    hierarchies_detected INTEGER,

    -- Error information
    error_message TEXT,
    error_type TEXT,
    error_traceback TEXT,

    -- Environment info
    python_version TEXT,
    package_version TEXT,
    working_directory TEXT
);

CREATE INDEX IF NOT EXISTS idx_cli_runs_timestamp ON cli_runs(timestamp);
CREATE INDEX IF NOT EXISTS idx_cli_runs_run_id ON cli_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_cli_runs_status ON cli_runs(status);
CREATE INDEX IF NOT EXISTS idx_cli_runs_command ON cli_runs(command);
CREATE INDEX IF NOT EXISTS idx_cli_runs_database_type ON cli_runs(database_type);
"""


def get_default_cli_db_path() -> str:
    """Get the default database path (~/.legend-cli/cli_runs.db)."""
    home = Path.home()
    legend_dir = home / ".legend-cli"
    legend_dir.mkdir(exist_ok=True)
    return str(legend_dir / "cli_runs.db")


class CLIRunDatabase:
    """SQLite database for CLI run logging."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file. If None, uses default.
        """
        self.db_path = db_path or get_default_cli_db_path()
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
            logger.debug("CLI logging database initialized at %s", self.db_path)
        except Exception as e:
            logger.error("Failed to initialize CLI logging database: %s", e)
            raise

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def insert_run(
        self,
        run_id: str,
        command: str,
        subcommand: Optional[str] = None,
        database_type: Optional[str] = None,
        database_path: Optional[str] = None,
        schema_filter: Optional[str] = None,
        doc_sources: Optional[List[str]] = None,
        arguments: Optional[Dict[str, Any]] = None,
        python_version: Optional[str] = None,
        package_version: Optional[str] = None,
        working_directory: Optional[str] = None,
    ) -> int:
        """Insert a new CLI run entry.

        Args:
            run_id: Unique identifier for this run
            command: Main command (e.g., 'model')
            subcommand: Subcommand (e.g., 'from-duckdb')
            database_type: Type of database ('duckdb', 'snowflake')
            database_path: Path or identifier of database
            schema_filter: Schema filter if specified
            doc_sources: List of documentation source paths
            arguments: Dictionary of all command arguments
            python_version: Python version
            package_version: Legend CLI version
            working_directory: Current working directory

        Returns:
            The row ID of the inserted entry
        """
        self.initialize()
        conn = self._get_connection()

        doc_sources_json = json.dumps(doc_sources) if doc_sources else None
        arguments_json = json.dumps(arguments) if arguments else None

        cursor = conn.execute(
            """
            INSERT INTO cli_runs (
                run_id, command, subcommand, database_type, database_path,
                schema_filter, doc_sources, arguments, status,
                python_version, package_version, working_directory
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'started', ?, ?, ?)
            """,
            (
                run_id, command, subcommand, database_type, database_path,
                schema_filter, doc_sources_json, arguments_json,
                python_version, package_version, working_directory,
            ),
        )
        return cursor.lastrowid

    def update_introspection_results(
        self,
        run_id: str,
        schemas_count: int,
        tables_count: int,
        columns_count: int,
        pattern_relationships: int,
    ) -> None:
        """Update run with introspection results.

        Args:
            run_id: Run identifier
            schemas_count: Number of schemas found
            tables_count: Number of tables found
            columns_count: Total number of columns
            pattern_relationships: Relationships from pattern detection
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE cli_runs
            SET schemas_count = ?, tables_count = ?, columns_count = ?,
                pattern_relationships = ?
            WHERE run_id = ?
            """,
            (schemas_count, tables_count, columns_count, pattern_relationships, run_id),
        )

    def update_document_relationships(
        self,
        run_id: str,
        document_relationships: int,
        total_relationships: int,
    ) -> None:
        """Update run with document relationship results.

        Args:
            run_id: Run identifier
            document_relationships: Relationships from document analysis
            total_relationships: Total relationships after merge
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE cli_runs
            SET document_relationships = ?, total_relationships = ?
            WHERE run_id = ?
            """,
            (document_relationships, total_relationships, run_id),
        )

    def update_enhanced_analysis(
        self,
        run_id: str,
        enhanced_mode: bool,
        enums_detected: int = 0,
        hierarchies_detected: int = 0,
    ) -> None:
        """Update run with enhanced analysis results.

        Args:
            run_id: Run identifier
            enhanced_mode: Whether enhanced mode was used
            enums_detected: Number of enums detected
            hierarchies_detected: Number of hierarchies detected
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE cli_runs
            SET enhanced_mode = ?, enums_detected = ?, hierarchies_detected = ?
            WHERE run_id = ?
            """,
            (enhanced_mode, enums_detected, hierarchies_detected, run_id),
        )

    def update_generation_results(
        self,
        run_id: str,
        artifacts_generated: List[str],
        classes_generated: int = 0,
        associations_generated: int = 0,
    ) -> None:
        """Update run with code generation results.

        Args:
            run_id: Run identifier
            artifacts_generated: List of artifact types generated
            classes_generated: Number of classes generated
            associations_generated: Number of associations generated
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE cli_runs
            SET artifacts_generated = ?, classes_generated = ?, associations_generated = ?
            WHERE run_id = ?
            """,
            (json.dumps(artifacts_generated), classes_generated, associations_generated, run_id),
        )

    def update_sdlc_results(
        self,
        run_id: str,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        workspace_id: Optional[str] = None,
        push_status: str = "success",
    ) -> None:
        """Update run with SDLC push results.

        Args:
            run_id: Run identifier
            project_id: Legend SDLC project ID
            project_name: Project name
            workspace_id: Workspace ID
            push_status: Push status ('success', 'skipped', 'error')
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE cli_runs
            SET project_id = ?, project_name = ?, workspace_id = ?, push_status = ?
            WHERE run_id = ?
            """,
            (project_id, project_name, workspace_id, push_status, run_id),
        )

    def update_success(
        self,
        run_id: str,
        duration_ms: int,
    ) -> None:
        """Mark run as successful.

        Args:
            run_id: Run identifier
            duration_ms: Total duration in milliseconds
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE cli_runs
            SET status = 'success', duration_ms = ?
            WHERE run_id = ?
            """,
            (duration_ms, run_id),
        )

    def update_error(
        self,
        run_id: str,
        error_message: str,
        error_type: Optional[str] = None,
        error_traceback: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> None:
        """Mark run as failed with error details.

        Args:
            run_id: Run identifier
            error_message: Error message
            error_type: Exception type
            error_traceback: Full traceback
            duration_ms: Duration until error
        """
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE cli_runs
            SET status = 'error', error_message = ?, error_type = ?,
                error_traceback = ?, duration_ms = ?
            WHERE run_id = ?
            """,
            (error_message, error_type, error_traceback, duration_ms, run_id),
        )

    def query_runs(
        self,
        command: Optional[str] = None,
        status: Optional[str] = None,
        database_type: Optional[str] = None,
        since_hours: int = 24,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Query run entries with optional filters.

        Args:
            command: Filter by command
            status: Filter by status
            database_type: Filter by database type
            since_hours: Look back N hours (default 24)
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of run entries as dictionaries
        """
        self.initialize()
        conn = self._get_connection()

        conditions = []
        params = []

        # Time filter
        since_time = datetime.utcnow() - timedelta(hours=since_hours)
        conditions.append("timestamp >= ?")
        params.append(since_time.isoformat())

        if command:
            conditions.append("command = ?")
            params.append(command)

        if status:
            conditions.append("status = ?")
            params.append(status)

        if database_type:
            conditions.append("database_type = ?")
            params.append(database_type)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.extend([limit, offset])

        query = f"""
            SELECT * FROM cli_runs
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def get_run_by_id(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific run by ID.

        Args:
            run_id: Run identifier

        Returns:
            Run entry as dictionary or None
        """
        self.initialize()
        conn = self._get_connection()

        cursor = conn.execute(
            "SELECT * FROM cli_runs WHERE run_id = ?",
            (run_id,),
        )
        row = cursor.fetchone()

        return dict(row) if row else None

    def get_stats(self, since_hours: int = 24) -> Dict[str, Any]:
        """Get statistics about CLI runs.

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
                AVG(duration_ms) as avg_duration_ms,
                SUM(tables_count) as total_tables,
                SUM(total_relationships) as total_relationships
            FROM cli_runs
            WHERE timestamp >= ?
            """,
            (since_time.isoformat(),),
        )
        row = cursor.fetchone()

        # Counts by database type
        cursor = conn.execute(
            """
            SELECT database_type, COUNT(*) as count,
                   SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
                   SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors
            FROM cli_runs
            WHERE timestamp >= ? AND database_type IS NOT NULL
            GROUP BY database_type
            ORDER BY count DESC
            """,
            (since_time.isoformat(),),
        )
        db_stats = [dict(r) for r in cursor.fetchall()]

        # Recent errors
        cursor = conn.execute(
            """
            SELECT run_id, timestamp, command, subcommand, error_message, error_type
            FROM cli_runs
            WHERE timestamp >= ? AND status = 'error'
            ORDER BY timestamp DESC
            LIMIT 5
            """,
            (since_time.isoformat(),),
        )
        recent_errors = [dict(r) for r in cursor.fetchall()]

        return {
            "total_runs": row["total"] or 0,
            "success_count": row["success_count"] or 0,
            "error_count": row["error_count"] or 0,
            "avg_duration_ms": round(row["avg_duration_ms"] or 0, 2),
            "total_tables_processed": row["total_tables"] or 0,
            "total_relationships_found": row["total_relationships"] or 0,
            "since_hours": since_hours,
            "by_database_type": db_stats,
            "recent_errors": recent_errors,
        }

    def cleanup_old_runs(self, retention_days: int = 30) -> int:
        """Delete runs older than retention period.

        Args:
            retention_days: Number of days to retain runs

        Returns:
            Number of deleted rows
        """
        self.initialize()
        conn = self._get_connection()

        cutoff_time = datetime.utcnow() - timedelta(days=retention_days)
        cursor = conn.execute(
            """
            DELETE FROM cli_runs
            WHERE timestamp < ?
            """,
            (cutoff_time.isoformat(),),
        )

        deleted = cursor.rowcount
        if deleted > 0:
            logger.info("Cleaned up %d old CLI run entries", deleted)

        return deleted

    def __enter__(self):
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
