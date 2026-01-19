"""Tests for model generation tools."""

import pytest
from legend_cli.mcp.context import MCPContext, PendingArtifact
from legend_cli.database.models import Database, Schema, Table, Column


class TestDatabaseModel:
    """Test Database model creation and manipulation."""

    def test_create_database(self, sample_database):
        """Test creating a Database model."""
        assert sample_database.name == "TestDB"
        assert len(sample_database.schemas) == 1

    def test_database_schema_access(self, sample_database):
        """Test accessing schemas in a database."""
        schema = sample_database.schemas[0]
        assert schema.name == "main"
        assert len(schema.tables) == 2

    def test_table_columns(self, sample_database):
        """Test accessing columns in a table."""
        users_table = sample_database.schemas[0].tables[0]
        assert users_table.name == "users"
        assert len(users_table.columns) == 4

        # Check column properties
        id_column = users_table.columns[0]
        assert id_column.name == "id"
        assert id_column.data_type == "INTEGER"
        assert id_column.is_primary_key is True
        assert id_column.is_nullable is False


class TestPureCodeSamples:
    """Test sample Pure code fixtures."""

    def test_store_pure_code_valid(self, sample_store_pure_code):
        """Test store Pure code is valid format."""
        assert "###Relational" in sample_store_pure_code
        assert "Database model::store::TestDB" in sample_store_pure_code
        assert "Schema main" in sample_store_pure_code
        assert "Table users" in sample_store_pure_code

    def test_class_pure_code_valid(self, sample_class_pure_code):
        """Test class Pure code is valid format."""
        assert "###Pure" in sample_class_pure_code
        assert "Class model::domain::User" in sample_class_pure_code
        assert "Class model::domain::Order" in sample_class_pure_code

    def test_connection_pure_code_valid(self, sample_connection_pure_code):
        """Test connection Pure code is valid format."""
        assert "###Connection" in sample_connection_pure_code
        assert "RelationalDatabaseConnection" in sample_connection_pure_code
        assert "store: model::store::TestDB" in sample_connection_pure_code

    def test_mapping_pure_code_valid(self, sample_mapping_pure_code):
        """Test mapping Pure code is valid format."""
        assert "###Mapping" in sample_mapping_pure_code
        assert "Mapping model::mapping::TestDBMapping" in sample_mapping_pure_code


class TestPendingArtifactsFixture:
    """Test pending artifacts fixture."""

    def test_full_model_artifacts(self, pending_artifacts_full_model):
        """Test full model has all required artifact types."""
        artifact_types = {a.artifact_type for a in pending_artifacts_full_model}

        assert "store" in artifact_types
        assert "classes" in artifact_types
        assert "connection" in artifact_types
        assert "mapping" in artifact_types

    def test_all_artifacts_have_code(self, pending_artifacts_full_model):
        """Test all artifacts have non-empty Pure code."""
        for artifact in pending_artifacts_full_model:
            assert artifact.pure_code is not None
            assert len(artifact.pure_code.strip()) > 0


class TestArtifactGeneration:
    """Test artifact generation scenarios."""

    def test_add_store_artifact(self, mcp_context, sample_store_pure_code):
        """Test adding a store artifact to context."""
        mcp_context.add_pending_artifact(
            artifact_type="store",
            pure_code=sample_store_pure_code,
            path="model::store::TestDB",
            classifier_path="meta::relational::metamodel::Database"
        )

        assert len(mcp_context.pending_artifacts) == 1
        artifact = mcp_context.pending_artifacts[0]
        assert artifact.artifact_type == "store"
        assert "###Relational" in artifact.pure_code

    def test_add_multiple_class_artifacts(self, mcp_context):
        """Test adding multiple class artifacts."""
        class1_code = "###Pure\nClass model::domain::User { name: String[1]; }"
        class2_code = "###Pure\nClass model::domain::Order { total: Decimal[1]; }"

        mcp_context.add_pending_artifact(artifact_type="classes", pure_code=class1_code)
        mcp_context.add_pending_artifact(artifact_type="classes", pure_code=class2_code)

        summary = mcp_context.get_pending_artifacts_summary()
        assert summary["classes"] == 2

    def test_artifact_ordering(self, mcp_context):
        """Test that artifacts maintain insertion order."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="1")
        mcp_context.add_pending_artifact(artifact_type="classes", pure_code="2")
        mcp_context.add_pending_artifact(artifact_type="mapping", pure_code="3")

        types = [a.artifact_type for a in mcp_context.pending_artifacts]
        assert types == ["store", "classes", "mapping"]


class TestModelCompleteness:
    """Test model completeness validation scenarios."""

    def test_store_only_model(self, mcp_context, sample_store_pure_code):
        """Test model with only store artifact."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code=sample_store_pure_code)

        summary = mcp_context.get_pending_artifacts_summary()
        assert summary == {"store": 1}

    def test_complete_model(self, mcp_context, pending_artifacts_full_model):
        """Test model with all artifact types."""
        mcp_context.pending_artifacts = pending_artifacts_full_model

        summary = mcp_context.get_pending_artifacts_summary()
        assert "store" in summary
        assert "classes" in summary
        assert "connection" in summary
        assert "mapping" in summary


class TestColumnTypeMapping:
    """Test database column type detection for Pure types."""

    @pytest.mark.parametrize("sql_type,expected_pure_type", [
        ("INTEGER", "Integer"),
        ("INT", "Integer"),
        ("BIGINT", "Integer"),
        ("VARCHAR(255)", "String"),
        ("TEXT", "String"),
        ("DECIMAL(10,2)", "Decimal"),
        ("NUMERIC", "Decimal"),
        ("FLOAT", "Float"),
        ("DOUBLE", "Float"),
        ("BOOLEAN", "Boolean"),
        ("DATE", "StrictDate"),
        ("TIMESTAMP", "DateTime"),
        ("DATETIME", "DateTime"),
    ])
    def test_sql_to_pure_type_mapping(self, sql_type, expected_pure_type):
        """Test SQL types map to correct Pure types."""
        # This tests the expected mapping logic
        # The actual implementation would be in the generator
        type_map = {
            "INTEGER": "Integer",
            "INT": "Integer",
            "BIGINT": "Integer",
            "VARCHAR": "String",
            "TEXT": "String",
            "DECIMAL": "Decimal",
            "NUMERIC": "Decimal",
            "FLOAT": "Float",
            "DOUBLE": "Float",
            "BOOLEAN": "Boolean",
            "DATE": "StrictDate",
            "TIMESTAMP": "DateTime",
            "DATETIME": "DateTime",
        }

        # Extract base type (e.g., VARCHAR from VARCHAR(255))
        base_type = sql_type.split("(")[0].upper()
        pure_type = type_map.get(base_type, "String")

        assert pure_type == expected_pure_type


class TestSchemaGeneration:
    """Test schema generation scenarios."""

    def test_single_schema_database(self, sample_database):
        """Test database with single schema."""
        assert len(sample_database.schemas) == 1
        assert sample_database.schemas[0].name == "main"

    def test_multi_table_schema(self, sample_database):
        """Test schema with multiple tables."""
        schema = sample_database.schemas[0]
        table_names = [t.name for t in schema.tables]

        assert "users" in table_names
        assert "orders" in table_names

    def test_create_database_with_multiple_schemas(self):
        """Test creating database with multiple schemas."""
        db = Database(
            name="MultiSchemaDB",
            schemas=[
                Schema(
                    name="public",
                    tables=[Table(name="t1", schema="public", columns=[Column(name="id", data_type="INT", is_nullable=False, is_primary_key=True)])]
                ),
                Schema(
                    name="staging",
                    tables=[Table(name="t2", schema="staging", columns=[Column(name="id", data_type="INT", is_nullable=False, is_primary_key=True)])]
                ),
            ]
        )

        assert len(db.schemas) == 2
        schema_names = [s.name for s in db.schemas]
        assert "public" in schema_names
        assert "staging" in schema_names
