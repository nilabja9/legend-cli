"""Session state management for Legend MCP server."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from enum import Enum

from legend_cli.database.models import Database


class DatabaseType(str, Enum):
    """Supported database types."""
    SNOWFLAKE = "snowflake"
    DUCKDB = "duckdb"


@dataclass
class DatabaseConnection:
    """Represents an active database connection."""
    db_type: DatabaseType
    database_name: str
    introspector: Any  # DatabaseIntrospector
    is_connected: bool = False
    connection_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.db_type, str):
            self.db_type = DatabaseType(self.db_type.lower())


@dataclass
class PendingArtifact:
    """Represents a generated artifact pending for SDLC push."""
    artifact_type: str  # 'store', 'classes', 'connection', 'mapping', 'runtime', 'associations'
    pure_code: str
    path: Optional[str] = None  # Full Pure path like 'model::domain::Person'
    classifier_path: Optional[str] = None


@dataclass
class MCPContext:
    """Session context for MCP server.

    Maintains state across tool calls within a session, including:
    - Active database connections
    - Introspected database schemas
    - Pending artifacts for review/push
    - Current SDLC project/workspace context
    """

    # Database connections keyed by identifier (e.g., "snowflake:MYDB" or "duckdb:path/to/db")
    connections: Dict[str, DatabaseConnection] = field(default_factory=dict)

    # Introspected database schemas keyed by database identifier
    introspected_schemas: Dict[str, Database] = field(default_factory=dict)

    # Pending artifacts awaiting push to SDLC
    pending_artifacts: List[PendingArtifact] = field(default_factory=list)

    # Current SDLC context
    current_project_id: Optional[str] = None
    current_workspace_id: Optional[str] = None

    # Package prefix for generated Pure code
    package_prefix: str = "model"

    # Generation options
    enhanced_analysis: bool = True
    detect_relationships: bool = True
    generate_docs: bool = True

    def get_connection_key(self, db_type: DatabaseType, database: str) -> str:
        """Generate a unique key for a database connection."""
        return f"{db_type.value}:{database}"

    def add_connection(
        self,
        db_type: DatabaseType,
        database: str,
        introspector: Any,
        connection_params: Dict[str, Any] = None
    ) -> DatabaseConnection:
        """Add or update a database connection."""
        key = self.get_connection_key(db_type, database)
        conn = DatabaseConnection(
            db_type=db_type,
            database_name=database,
            introspector=introspector,
            is_connected=True,
            connection_params=connection_params or {}
        )
        self.connections[key] = conn
        return conn

    def get_connection(self, db_type: DatabaseType, database: str) -> Optional[DatabaseConnection]:
        """Get an active database connection."""
        key = self.get_connection_key(db_type, database)
        return self.connections.get(key)

    def remove_connection(self, db_type: DatabaseType, database: str) -> bool:
        """Remove a database connection."""
        key = self.get_connection_key(db_type, database)
        if key in self.connections:
            conn = self.connections[key]
            if conn.introspector:
                try:
                    conn.introspector.close()
                except Exception:
                    pass
            del self.connections[key]
            return True
        return False

    def store_schema(self, db_type: DatabaseType, database: str, schema: Database):
        """Store an introspected database schema."""
        key = self.get_connection_key(db_type, database)
        self.introspected_schemas[key] = schema

    def get_schema(self, db_type: DatabaseType, database: str) -> Optional[Database]:
        """Get a stored database schema."""
        key = self.get_connection_key(db_type, database)
        return self.introspected_schemas.get(key)

    def add_pending_artifact(
        self,
        artifact_type: str,
        pure_code: str,
        path: Optional[str] = None,
        classifier_path: Optional[str] = None
    ):
        """Add an artifact to the pending list."""
        self.pending_artifacts.append(PendingArtifact(
            artifact_type=artifact_type,
            pure_code=pure_code,
            path=path,
            classifier_path=classifier_path
        ))

    def clear_pending_artifacts(self):
        """Clear all pending artifacts."""
        self.pending_artifacts.clear()

    def get_pending_artifacts_summary(self) -> Dict[str, int]:
        """Get a summary of pending artifacts by type."""
        summary = {}
        for artifact in self.pending_artifacts:
            summary[artifact.artifact_type] = summary.get(artifact.artifact_type, 0) + 1
        return summary

    def set_sdlc_context(self, project_id: str, workspace_id: Optional[str] = None):
        """Set the current SDLC project and workspace context."""
        self.current_project_id = project_id
        if workspace_id:
            self.current_workspace_id = workspace_id

    def close_all_connections(self):
        """Close all active database connections."""
        for key, conn in list(self.connections.items()):
            if conn.introspector:
                try:
                    conn.introspector.close()
                except Exception:
                    pass
        self.connections.clear()

    def reset(self):
        """Reset all session state."""
        self.close_all_connections()
        self.introspected_schemas.clear()
        self.pending_artifacts.clear()
        self.current_project_id = None
        self.current_workspace_id = None


# Global context instance for the MCP server session
_context: Optional[MCPContext] = None


def get_context() -> MCPContext:
    """Get or create the global MCP context."""
    global _context
    if _context is None:
        _context = MCPContext()
    return _context


def reset_context():
    """Reset the global MCP context."""
    global _context
    if _context:
        _context.reset()
    _context = MCPContext()
