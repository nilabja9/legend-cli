"""CLI run logging service for Legend CLI.

Provides a high-level interface for logging CLI command runs,
including automatic context capture and error handling.
"""

import logging
import os
import sys
import time
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from legend_cli.logging.cli_db import CLIRunDatabase

logger = logging.getLogger(__name__)

# Global logger instance
_cli_logger: Optional["CLIRunLogger"] = None


def get_cli_logger() -> "CLIRunLogger":
    """Get or create the global CLI logger instance."""
    global _cli_logger
    if _cli_logger is None:
        _cli_logger = CLIRunLogger()
    return _cli_logger


@dataclass
class RunContext:
    """Context for a CLI run."""

    run_id: str
    command: str
    subcommand: Optional[str] = None
    database_type: Optional[str] = None
    database_path: Optional[str] = None
    schema_filter: Optional[str] = None
    doc_sources: Optional[List[str]] = None
    arguments: Dict[str, Any] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)

    # Results that get populated during the run
    schemas_count: int = 0
    tables_count: int = 0
    columns_count: int = 0
    pattern_relationships: int = 0
    document_relationships: int = 0
    total_relationships: int = 0
    enhanced_mode: bool = False
    enums_detected: int = 0
    hierarchies_detected: int = 0
    artifacts_generated: List[str] = field(default_factory=list)
    classes_generated: int = 0
    associations_generated: int = 0
    project_id: Optional[int] = None
    project_name: Optional[str] = None
    workspace_id: Optional[str] = None
    push_status: Optional[str] = None


class CLIRunLogger:
    """High-level logger for CLI runs.

    Example usage:
        logger = get_cli_logger()

        with logger.log_run(
            command="model",
            subcommand="from-duckdb",
            database_type="duckdb",
            database_path="/path/to/db.duckdb",
        ) as ctx:
            # Do introspection
            ctx.tables_count = 17
            ctx.pattern_relationships = 19

            # Generate code
            ctx.artifacts_generated = ["store", "classes", "mapping"]

            # If error occurs, it's automatically logged
    """

    def __init__(self, db_path: Optional[str] = None, enabled: bool = True):
        """Initialize the CLI run logger.

        Args:
            db_path: Path to the SQLite database. If None, uses default.
            enabled: Whether logging is enabled.
        """
        self.enabled = enabled
        self._db: Optional[CLIRunDatabase] = None
        self._db_path = db_path

        if self.enabled:
            try:
                self._db = CLIRunDatabase(db_path)
                self._db.initialize()
                # Clean up old logs on initialization
                self._db.cleanup_old_runs()
            except Exception as e:
                logger.warning("Failed to initialize CLI logging: %s", e)
                self.enabled = False

    @property
    def db(self) -> Optional[CLIRunDatabase]:
        """Get the database instance."""
        return self._db

    def _get_environment_info(self) -> Dict[str, str]:
        """Get environment information for logging."""
        return {
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "package_version": self._get_package_version(),
            "working_directory": os.getcwd(),
        }

    def _get_package_version(self) -> str:
        """Get the legend-cli package version."""
        try:
            from importlib.metadata import version
            return version("legend-cli")
        except Exception:
            return "unknown"

    @contextmanager
    def log_run(
        self,
        command: str,
        subcommand: Optional[str] = None,
        database_type: Optional[str] = None,
        database_path: Optional[str] = None,
        schema_filter: Optional[str] = None,
        doc_sources: Optional[List[str]] = None,
        arguments: Optional[Dict[str, Any]] = None,
    ):
        """Context manager for logging a CLI run.

        Args:
            command: Main command (e.g., 'model')
            subcommand: Subcommand (e.g., 'from-duckdb')
            database_type: Database type
            database_path: Database path
            schema_filter: Schema filter
            doc_sources: Documentation sources
            arguments: All command arguments

        Yields:
            RunContext that can be updated during the run
        """
        run_id = str(uuid.uuid4())[:8]
        ctx = RunContext(
            run_id=run_id,
            command=command,
            subcommand=subcommand,
            database_type=database_type,
            database_path=database_path,
            schema_filter=schema_filter,
            doc_sources=doc_sources,
            arguments=arguments or {},
        )

        if not self.enabled or self._db is None:
            # If logging disabled, just yield context and return
            yield ctx
            return

        # Insert initial run entry
        try:
            env_info = self._get_environment_info()
            self._db.insert_run(
                run_id=run_id,
                command=command,
                subcommand=subcommand,
                database_type=database_type,
                database_path=database_path,
                schema_filter=schema_filter,
                doc_sources=doc_sources,
                arguments=arguments,
                python_version=env_info["python_version"],
                package_version=env_info["package_version"],
                working_directory=env_info["working_directory"],
            )
        except Exception as e:
            logger.warning("Failed to log run start: %s", e)

        try:
            yield ctx

            # Update with final results
            self._update_run_results(ctx)

            # Mark as success
            duration_ms = int((time.time() - ctx.start_time) * 1000)
            self._db.update_success(run_id, duration_ms)

            logger.debug(
                "CLI run %s completed successfully in %dms",
                run_id,
                duration_ms,
            )

        except Exception as e:
            # Log the error
            duration_ms = int((time.time() - ctx.start_time) * 1000)
            try:
                self._db.update_error(
                    run_id=run_id,
                    error_message=str(e),
                    error_type=type(e).__name__,
                    error_traceback=traceback.format_exc(),
                    duration_ms=duration_ms,
                )
            except Exception as log_err:
                logger.warning("Failed to log run error: %s", log_err)

            logger.debug(
                "CLI run %s failed after %dms: %s",
                run_id,
                duration_ms,
                str(e),
            )

            # Re-raise the original exception
            raise

    def _update_run_results(self, ctx: RunContext) -> None:
        """Update the run entry with collected results."""
        if not self._db:
            return

        try:
            # Update introspection results
            if ctx.tables_count > 0:
                self._db.update_introspection_results(
                    run_id=ctx.run_id,
                    schemas_count=ctx.schemas_count,
                    tables_count=ctx.tables_count,
                    columns_count=ctx.columns_count,
                    pattern_relationships=ctx.pattern_relationships,
                )

            # Update document relationships
            if ctx.document_relationships > 0 or ctx.total_relationships > 0:
                self._db.update_document_relationships(
                    run_id=ctx.run_id,
                    document_relationships=ctx.document_relationships,
                    total_relationships=ctx.total_relationships,
                )

            # Update enhanced analysis
            if ctx.enhanced_mode:
                self._db.update_enhanced_analysis(
                    run_id=ctx.run_id,
                    enhanced_mode=ctx.enhanced_mode,
                    enums_detected=ctx.enums_detected,
                    hierarchies_detected=ctx.hierarchies_detected,
                )

            # Update generation results
            if ctx.artifacts_generated:
                self._db.update_generation_results(
                    run_id=ctx.run_id,
                    artifacts_generated=ctx.artifacts_generated,
                    classes_generated=ctx.classes_generated,
                    associations_generated=ctx.associations_generated,
                )

            # Update SDLC results
            if ctx.project_id or ctx.push_status:
                self._db.update_sdlc_results(
                    run_id=ctx.run_id,
                    project_id=ctx.project_id,
                    project_name=ctx.project_name,
                    workspace_id=ctx.workspace_id,
                    push_status=ctx.push_status or "skipped",
                )

        except Exception as e:
            logger.warning("Failed to update run results: %s", e)

    def query_runs(
        self,
        command: Optional[str] = None,
        status: Optional[str] = None,
        database_type: Optional[str] = None,
        since_hours: int = 24,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query CLI runs with optional filters."""
        if not self.enabled or not self._db:
            return []

        return self._db.query_runs(
            command=command,
            status=status,
            database_type=database_type,
            since_hours=since_hours,
            limit=limit,
        )

    def get_stats(self, since_hours: int = 24) -> Dict[str, Any]:
        """Get statistics about CLI runs."""
        if not self.enabled or not self._db:
            return {"error": "Logging not enabled"}

        return self._db.get_stats(since_hours=since_hours)

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific run by ID."""
        if not self.enabled or not self._db:
            return None

        return self._db.get_run_by_id(run_id)


def log_cli_run(
    command: str,
    subcommand: Optional[str] = None,
    database_type: Optional[str] = None,
    database_path: Optional[str] = None,
    schema_filter: Optional[str] = None,
    doc_sources: Optional[List[str]] = None,
    arguments: Optional[Dict[str, Any]] = None,
):
    """Convenience function to get a logging context manager.

    Example:
        with log_cli_run("model", "from-duckdb", "duckdb", "/path/to/db") as ctx:
            # Do work
            ctx.tables_count = 17
    """
    return get_cli_logger().log_run(
        command=command,
        subcommand=subcommand,
        database_type=database_type,
        database_path=database_path,
        schema_filter=schema_filter,
        doc_sources=doc_sources,
        arguments=arguments,
    )
