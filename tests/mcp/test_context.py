"""Tests for MCP context management."""

import pytest
from legend_cli.mcp.context import (
    MCPContext,
    PendingArtifact,
    DatabaseConnection,
    DatabaseType,
    sanitize_pure_identifier,
)


class TestMCPContextCreation:
    """Test MCPContext initialization."""

    def test_empty_context_creation(self, mcp_context):
        """Test that a fresh context has empty collections."""
        assert mcp_context.connections == {}
        assert mcp_context.introspected_schemas == {}
        assert mcp_context.pending_artifacts == []
        assert mcp_context.current_project_id is None
        assert mcp_context.current_workspace_id is None

    def test_default_package_prefix(self, mcp_context):
        """Test default package prefix is 'model'."""
        assert mcp_context.package_prefix == "model"

    def test_default_options(self, mcp_context):
        """Test default generation options."""
        assert mcp_context.enhanced_analysis is True
        assert mcp_context.detect_relationships is True
        assert mcp_context.generate_docs is True


class TestPendingArtifacts:
    """Test pending artifact management."""

    def test_add_pending_artifact(self, mcp_context):
        """Test adding a pending artifact."""
        mcp_context.add_pending_artifact(
            artifact_type="store",
            pure_code="###Relational\nDatabase test::DB ()",
            path="test::DB",
            classifier_path="meta::relational::metamodel::Database"
        )

        assert len(mcp_context.pending_artifacts) == 1
        artifact = mcp_context.pending_artifacts[0]
        assert artifact.artifact_type == "store"
        assert artifact.path == "test::DB"

    def test_add_multiple_artifacts(self, mcp_context):
        """Test adding multiple pending artifacts."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="store code")
        mcp_context.add_pending_artifact(artifact_type="classes", pure_code="class code")
        mcp_context.add_pending_artifact(artifact_type="mapping", pure_code="mapping code")

        assert len(mcp_context.pending_artifacts) == 3

    def test_clear_pending_artifacts(self, mcp_context):
        """Test clearing pending artifacts."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="code")
        mcp_context.add_pending_artifact(artifact_type="classes", pure_code="code")

        mcp_context.clear_pending_artifacts()

        assert len(mcp_context.pending_artifacts) == 0

    def test_get_pending_artifacts_summary(self, mcp_context):
        """Test getting summary of pending artifacts."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="code")
        mcp_context.add_pending_artifact(artifact_type="classes", pure_code="code1")
        mcp_context.add_pending_artifact(artifact_type="classes", pure_code="code2")
        mcp_context.add_pending_artifact(artifact_type="mapping", pure_code="code")

        summary = mcp_context.get_pending_artifacts_summary()

        assert summary["store"] == 1
        assert summary["classes"] == 2
        assert summary["mapping"] == 1


class TestSDLCContext:
    """Test SDLC context management."""

    def test_set_sdlc_context(self, mcp_context):
        """Test setting SDLC project and workspace context."""
        mcp_context.set_sdlc_context("PROJ-1", "workspace-dev")

        assert mcp_context.current_project_id == "PROJ-1"
        assert mcp_context.current_workspace_id == "workspace-dev"

    def test_set_sdlc_context_project_only(self, mcp_context):
        """Test setting only project ID."""
        mcp_context.set_sdlc_context("PROJ-1")

        assert mcp_context.current_project_id == "PROJ-1"
        assert mcp_context.current_workspace_id is None


class TestDatabaseConnections:
    """Test database connection management."""

    def test_add_connection(self, mcp_context):
        """Test adding a database connection."""
        mock_introspector = object()  # Mock introspector
        conn = mcp_context.add_connection(
            db_type=DatabaseType.DUCKDB,
            database="test.db",
            introspector=mock_introspector,
            connection_params={"path": "/tmp/test.db"}
        )

        assert conn.db_type == DatabaseType.DUCKDB
        assert conn.database_name == "test.db"
        assert conn.is_connected is True

    def test_get_connection(self, mcp_context):
        """Test getting an existing connection."""
        mock_introspector = object()
        mcp_context.add_connection(
            db_type=DatabaseType.SNOWFLAKE,
            database="MYDB",
            introspector=mock_introspector
        )

        conn = mcp_context.get_connection(DatabaseType.SNOWFLAKE, "MYDB")

        assert conn is not None
        assert conn.database_name == "MYDB"

    def test_get_nonexistent_connection(self, mcp_context):
        """Test getting a connection that doesn't exist."""
        conn = mcp_context.get_connection(DatabaseType.DUCKDB, "nonexistent")

        assert conn is None

    def test_connection_key_generation(self, mcp_context):
        """Test connection key format."""
        key = mcp_context.get_connection_key(DatabaseType.SNOWFLAKE, "MYDB")

        assert key == "snowflake:MYDB"


class TestContextReset:
    """Test context reset functionality."""

    def test_reset_clears_all_state(self, mcp_context):
        """Test that reset clears all context state."""
        # Add some state
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="code")
        mcp_context.set_sdlc_context("PROJ-1", "workspace-1")

        mcp_context.reset()

        assert len(mcp_context.pending_artifacts) == 0
        assert len(mcp_context.connections) == 0
        assert len(mcp_context.introspected_schemas) == 0
        assert mcp_context.current_project_id is None
        assert mcp_context.current_workspace_id is None


class TestSanitizePureIdentifier:
    """Test Pure identifier sanitization."""

    def test_simple_name(self):
        """Test simple valid name."""
        assert sanitize_pure_identifier("TestDB") == "Testdb"

    def test_name_with_spaces(self):
        """Test name with spaces converted to PascalCase."""
        assert sanitize_pure_identifier("my database") == "MyDatabase"

    def test_name_with_hyphens(self):
        """Test name with hyphens converted to PascalCase."""
        assert sanitize_pure_identifier("my-database") == "MyDatabase"

    def test_file_path(self):
        """Test file path extracts filename."""
        result = sanitize_pure_identifier("/path/to/my_database.db")
        assert result == "MyDatabase"

    def test_name_starting_with_number(self):
        """Test name starting with number gets 'Db' prefix."""
        result = sanitize_pure_identifier("123database")
        assert result == "Db123database"

    def test_empty_name(self):
        """Test empty name returns 'Unknown'."""
        assert sanitize_pure_identifier("") == "Unknown"

    def test_special_characters_removed(self):
        """Test special characters are removed."""
        result = sanitize_pure_identifier("test@db#123")
        assert "Testdb123" in result or "TestDb123" in result


class TestPendingArtifactDataclass:
    """Test PendingArtifact dataclass."""

    def test_create_minimal_artifact(self):
        """Test creating artifact with minimal fields."""
        artifact = PendingArtifact(
            artifact_type="store",
            pure_code="some code"
        )

        assert artifact.artifact_type == "store"
        assert artifact.pure_code == "some code"
        assert artifact.path is None
        assert artifact.classifier_path is None

    def test_create_full_artifact(self):
        """Test creating artifact with all fields."""
        artifact = PendingArtifact(
            artifact_type="classes",
            pure_code="class code",
            path="model::domain::User",
            classifier_path="meta::pure::metamodel::type::Class"
        )

        assert artifact.artifact_type == "classes"
        assert artifact.path == "model::domain::User"
        assert artifact.classifier_path == "meta::pure::metamodel::type::Class"


class TestDatabaseConnectionDataclass:
    """Test DatabaseConnection dataclass."""

    def test_create_connection(self):
        """Test creating a database connection."""
        conn = DatabaseConnection(
            db_type=DatabaseType.DUCKDB,
            database_name="test.db",
            introspector=None
        )

        assert conn.db_type == DatabaseType.DUCKDB
        assert conn.database_name == "test.db"
        assert conn.is_connected is False
        assert conn.connection_params == {}

    def test_connection_with_string_db_type(self):
        """Test that string db_type is converted to enum."""
        conn = DatabaseConnection(
            db_type="snowflake",
            database_name="MYDB",
            introspector=None
        )

        assert conn.db_type == DatabaseType.SNOWFLAKE
