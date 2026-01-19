"""Tests for push_artifacts workflow."""

import pytest
import json
from unittest.mock import patch, MagicMock

from legend_cli.mcp.context import MCPContext, PendingArtifact
from legend_cli.mcp.tools.sdlc import (
    push_artifacts,
    _validate_artifacts,
    _validate_parsed_entities,
)
from .fixtures import MockEngineClient, MockSDLCClient, create_mock_engine_with_store_response


class TestValidateArtifacts:
    """Test artifact validation before push."""

    def test_valid_full_model(self, mcp_context, pending_artifacts_full_model):
        """Test validation passes for a complete model."""
        mcp_context.pending_artifacts = pending_artifacts_full_model

        result = _validate_artifacts(mcp_context)

        assert result["valid"] is True
        assert len(result["errors"]) == 0
        assert "store" in result["artifact_types"]
        assert "classes" in result["artifact_types"]

    def test_missing_store_with_mapping(self, mcp_context):
        """Test validation fails when mapping is present but store is missing."""
        mcp_context.add_pending_artifact(artifact_type="mapping", pure_code="mapping code")
        mcp_context.add_pending_artifact(artifact_type="classes", pure_code="class code")

        result = _validate_artifacts(mcp_context)

        assert result["valid"] is False
        assert any("Store is missing" in e for e in result["errors"])

    def test_missing_classes_with_mapping(self, mcp_context):
        """Test validation fails when mapping is present but classes are missing."""
        mcp_context.add_pending_artifact(artifact_type="mapping", pure_code="mapping code")
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="store code")

        result = _validate_artifacts(mcp_context)

        assert result["valid"] is False
        assert any("Classes are missing" in e for e in result["errors"])

    def test_empty_pure_code_error(self, mcp_context):
        """Test validation fails for empty Pure code."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="")

        result = _validate_artifacts(mcp_context)

        assert result["valid"] is False
        assert any("empty Pure code" in e for e in result["errors"])

    def test_warnings_for_incomplete_model(self, mcp_context):
        """Test warnings are generated for incomplete models."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="store code")

        result = _validate_artifacts(mcp_context)

        assert result["valid"] is True  # Still valid, just warnings
        assert len(result["warnings"]) > 0
        assert any("no Classes" in w for w in result["warnings"])


class TestValidateParsedEntities:
    """Test validation of parsed entities."""

    def test_valid_entities(self):
        """Test validation passes when all expected entities are present."""
        entities = [
            {"path": "model::store::DB", "classifierPath": "meta::relational::metamodel::Database"},
            {"path": "model::domain::User", "classifierPath": "meta::pure::metamodel::type::Class"},
        ]
        artifact_types = {"store", "classes"}

        result = _validate_parsed_entities(entities, artifact_types)

        assert result["valid"] is True
        assert len(result["missing"]) == 0

    def test_missing_entity_type(self):
        """Test validation fails when expected entity type is missing."""
        entities = [
            {"path": "model::domain::User", "classifierPath": "meta::pure::metamodel::type::Class"},
        ]
        artifact_types = {"store", "classes"}

        result = _validate_parsed_entities(entities, artifact_types)

        assert result["valid"] is False
        assert "store" in result["missing"]

    def test_entity_type_counts(self):
        """Test entity type counting."""
        entities = [
            {"classifierPath": "meta::pure::metamodel::type::Class"},
            {"classifierPath": "meta::pure::metamodel::type::Class"},
            {"classifierPath": "meta::relational::metamodel::Database"},
        ]
        artifact_types = {"classes", "store"}

        result = _validate_parsed_entities(entities, artifact_types)

        assert result["entity_types"]["meta::pure::metamodel::type::Class"] == 2
        assert result["entity_types"]["meta::relational::metamodel::Database"] == 1

    def test_diagnostics_included(self):
        """Test diagnostics are included in result when provided."""
        entities = [{"classifierPath": "meta::pure::metamodel::type::Class"}]
        artifact_types = {"store", "classes"}
        diagnostics = {
            "store": {"diagnostic": "No elements found", "error": "Parse failed"}
        }

        result = _validate_parsed_entities(entities, artifact_types, diagnostics)

        assert result["diagnostics"] == diagnostics


class TestPushArtifactsNoArtifacts:
    """Test push_artifacts with no pending artifacts."""

    @pytest.mark.asyncio
    async def test_no_artifacts_returns_message(self, mcp_context):
        """Test that pushing with no artifacts returns appropriate message."""
        result = await push_artifacts(
            ctx=mcp_context,
            project_id="PROJ-1",
            workspace_id="workspace-1"
        )

        result_data = json.loads(result)
        assert result_data["status"] == "no_artifacts"
        assert "No pending artifacts" in result_data["message"]


class TestPushArtifactsValidationError:
    """Test push_artifacts with validation errors."""

    @pytest.mark.asyncio
    async def test_validation_error_missing_dependencies(self, mcp_context):
        """Test validation error when dependencies are missing."""
        # Add mapping without store and classes
        mcp_context.add_pending_artifact(artifact_type="mapping", pure_code="mapping code")

        result = await push_artifacts(
            ctx=mcp_context,
            project_id="PROJ-1",
            workspace_id="workspace-1"
        )

        result_data = json.loads(result)
        assert result_data["status"] == "validation_error"
        assert len(result_data["errors"]) > 0


class TestPushArtifactsWithMocks:
    """Test push_artifacts with mocked Engine and SDLC clients."""

    @pytest.mark.asyncio
    async def test_push_success_with_mocks(self, mcp_context, sample_store_pure_code):
        """Test successful push with mocked clients."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code=sample_store_pure_code)

        mock_engine = create_mock_engine_with_store_response()
        mock_sdlc = MockSDLCClient()
        mock_sdlc.add_project("PROJ-1", "Test Project")
        mock_sdlc.add_workspace("PROJ-1", "workspace-1")

        with patch("legend_cli.engine_client.EngineClient") as MockEngineClass, \
             patch("legend_cli.sdlc_client.SDLCClient") as MockSDLCClass:

            MockEngineClass.return_value.__enter__ = MagicMock(return_value=mock_engine)
            MockEngineClass.return_value.__exit__ = MagicMock(return_value=None)
            MockSDLCClass.return_value.__enter__ = MagicMock(return_value=mock_sdlc)
            MockSDLCClass.return_value.__exit__ = MagicMock(return_value=None)

            result = await push_artifacts(
                ctx=mcp_context,
                project_id="PROJ-1",
                workspace_id="workspace-1",
                commit_message="Test push"
            )

            result_data = json.loads(result)
            assert result_data["status"] == "success"
            assert result_data["entity_count"] == 1

    @pytest.mark.asyncio
    async def test_parse_error_returns_diagnostics(self, mcp_context):
        """Test that parse errors include diagnostics."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code="invalid code")

        mock_engine = MockEngineClient()
        mock_engine.set_default_response({"modelDataContext": {"elements": []}})

        with patch("legend_cli.engine_client.EngineClient") as MockEngineClass:
            MockEngineClass.return_value.__enter__ = MagicMock(return_value=mock_engine)
            MockEngineClass.return_value.__exit__ = MagicMock(return_value=None)

            result = await push_artifacts(
                ctx=mcp_context,
                project_id="PROJ-1",
                workspace_id="workspace-1"
            )

            result_data = json.loads(result)
            assert result_data["status"] == "parse_error"
            assert "diagnostics" in result_data or "errors" in result_data


class TestPushArtifactsClearsPending:
    """Test that push_artifacts clears pending artifacts on success."""

    @pytest.mark.asyncio
    async def test_clear_pending_on_success(self, mcp_context, sample_store_pure_code):
        """Test pending artifacts are cleared after successful push."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code=sample_store_pure_code)

        mock_engine = create_mock_engine_with_store_response()
        mock_sdlc = MockSDLCClient()
        mock_sdlc.add_project("PROJ-1", "Test Project")
        mock_sdlc.add_workspace("PROJ-1", "workspace-1")

        with patch("legend_cli.engine_client.EngineClient") as MockEngineClass, \
             patch("legend_cli.sdlc_client.SDLCClient") as MockSDLCClass:

            MockEngineClass.return_value.__enter__ = MagicMock(return_value=mock_engine)
            MockEngineClass.return_value.__exit__ = MagicMock(return_value=None)
            MockSDLCClass.return_value.__enter__ = MagicMock(return_value=mock_sdlc)
            MockSDLCClass.return_value.__exit__ = MagicMock(return_value=None)

            await push_artifacts(
                ctx=mcp_context,
                project_id="PROJ-1",
                workspace_id="workspace-1",
                clear_pending=True
            )

            assert len(mcp_context.pending_artifacts) == 0

    @pytest.mark.asyncio
    async def test_keep_pending_when_disabled(self, mcp_context, sample_store_pure_code):
        """Test pending artifacts are kept when clear_pending=False."""
        mcp_context.add_pending_artifact(artifact_type="store", pure_code=sample_store_pure_code)

        mock_engine = create_mock_engine_with_store_response()
        mock_sdlc = MockSDLCClient()
        mock_sdlc.add_project("PROJ-1", "Test Project")
        mock_sdlc.add_workspace("PROJ-1", "workspace-1")

        with patch("legend_cli.engine_client.EngineClient") as MockEngineClass, \
             patch("legend_cli.sdlc_client.SDLCClient") as MockSDLCClass:

            MockEngineClass.return_value.__enter__ = MagicMock(return_value=mock_engine)
            MockEngineClass.return_value.__exit__ = MagicMock(return_value=None)
            MockSDLCClass.return_value.__enter__ = MagicMock(return_value=mock_sdlc)
            MockSDLCClass.return_value.__exit__ = MagicMock(return_value=None)

            await push_artifacts(
                ctx=mcp_context,
                project_id="PROJ-1",
                workspace_id="workspace-1",
                clear_pending=False
            )

            assert len(mcp_context.pending_artifacts) == 1
