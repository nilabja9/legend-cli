"""Integration tests simulating Claude Desktop workflows.

These tests replicate the exact workflows that would be executed
through Claude Desktop, ensuring end-to-end functionality.
"""

import pytest
import json
from unittest.mock import patch, MagicMock

from legend_cli.mcp.context import MCPContext, PendingArtifact
from legend_cli.mcp.tools.sdlc import push_artifacts, _validate_artifacts, _validate_parsed_entities
from .fixtures import MockEngineClient, MockSDLCClient, create_mock_engine_with_store_response, create_mock_engine_with_bug


class TestStoreGenerationToParsingFlow:
    """Test the store generation -> parsing -> push flow.

    This replicates the exact workflow that caused the '0 entities' bug.
    """

    def test_relational_store_parsing_flow(self):
        """Test that relational store code parses correctly through the full flow."""
        # Step 1: Generate store Pure code (simulated)
        store_code = '''###Relational
Database trading_simulation::store::TradingDB
(
  Schema main
  (
    Table trades
    (
      id INTEGER PRIMARY KEY,
      symbol VARCHAR(10) NOT NULL,
      quantity INTEGER NOT NULL,
      price DECIMAL(10,2) NOT NULL
    )
  )
)'''

        # Step 2: Parse through mock Engine
        mock_engine = MockEngineClient()
        mock_engine.set_default_response({
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "relational",
                        "package": "trading_simulation::store",
                        "name": "TradingDB",
                        "schemas": []
                    }
                ]
            }
        })

        entities = mock_engine.parse_pure_code(store_code)

        # Step 3: Verify entities were extracted
        assert len(entities) == 1
        assert entities[0]["path"] == "trading_simulation::store::TradingDB"
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"

    def test_stores_key_parsing_flow(self):
        """Test parsing when Engine returns elements in 'stores' key."""
        store_code = '''###Relational
Database model::store::TestDB ()'''

        # Simulate the bug: Engine returns in 'stores' instead of 'elements'
        mock_engine = create_mock_engine_with_bug()

        entities = mock_engine.parse_pure_code(store_code)

        # With the fix, entities should still be extracted
        assert len(entities) == 1
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"


class TestFullModelGenerationWorkflow:
    """Test complete model generation workflow (store -> classes -> connection -> mapping -> runtime)."""

    @pytest.fixture
    def full_workflow_context(self):
        """Create context with full model artifacts."""
        ctx = MCPContext()

        # Add store
        ctx.add_pending_artifact(
            artifact_type="store",
            pure_code='''###Relational
Database model::store::TestDB
(
  Schema main
  (
    Table users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL)
  )
)'''
        )

        # Add classes
        ctx.add_pending_artifact(
            artifact_type="classes",
            pure_code='''###Pure
Class model::domain::User
{
  id: Integer[1];
  name: String[1];
}'''
        )

        # Add connection
        ctx.add_pending_artifact(
            artifact_type="connection",
            pure_code='''###Connection
RelationalDatabaseConnection model::connection::TestDBConnection
{
  store: model::store::TestDB;
  type: DuckDB;
  specification: LocalDuckDB { path: '/tmp/test.db'; };
  auth: DefaultH2;
}'''
        )

        # Add mapping
        ctx.add_pending_artifact(
            artifact_type="mapping",
            pure_code='''###Mapping
Mapping model::mapping::TestDBMapping
(
  model::domain::User: Relational
  {
    ~mainTable [model::store::TestDB]main.users
    id: [model::store::TestDB]main.users.id,
    name: [model::store::TestDB]main.users.name
  }
)'''
        )

        return ctx

    def test_validation_passes_for_full_model(self, full_workflow_context):
        """Test that validation passes for a complete model."""
        result = _validate_artifacts(full_workflow_context)

        assert result["valid"] is True
        assert len(result["errors"]) == 0
        assert "store" in result["artifact_types"]
        assert "classes" in result["artifact_types"]
        assert "connection" in result["artifact_types"]
        assert "mapping" in result["artifact_types"]

    @pytest.mark.asyncio
    async def test_full_push_workflow(self, full_workflow_context):
        """Test complete push workflow with mocked clients."""
        # Configure mock Engine to return appropriate responses
        mock_engine = MockEngineClient()

        # Add responses for each artifact type
        mock_engine.add_response(
            r"###Relational\s+Database",
            {
                "modelDataContext": {
                    "elements": [
                        {"_type": "relational", "package": "model::store", "name": "TestDB"}
                    ]
                }
            }
        )
        mock_engine.add_response(
            r"###Pure\s+Class",
            {
                "modelDataContext": {
                    "elements": [
                        {"_type": "class", "package": "model::domain", "name": "User"}
                    ]
                }
            }
        )
        mock_engine.add_response(
            r"###Connection",
            {
                "modelDataContext": {
                    "elements": [
                        {"_type": "relationalDatabaseConnection", "package": "model::connection", "name": "TestDBConnection"}
                    ]
                }
            }
        )
        mock_engine.add_response(
            r"###Mapping",
            {
                "modelDataContext": {
                    "elements": [
                        {"_type": "mapping", "package": "model::mapping", "name": "TestDBMapping"}
                    ]
                }
            }
        )

        # Configure mock SDLC
        mock_sdlc = MockSDLCClient()
        mock_sdlc.add_project("PROJ-1", "Test Project")
        mock_sdlc.add_workspace("PROJ-1", "dev")

        with patch("legend_cli.engine_client.EngineClient") as MockEngineClass, \
             patch("legend_cli.sdlc_client.SDLCClient") as MockSDLCClass:

            MockEngineClass.return_value.__enter__ = MagicMock(return_value=mock_engine)
            MockEngineClass.return_value.__exit__ = MagicMock(return_value=None)
            MockSDLCClass.return_value.__enter__ = MagicMock(return_value=mock_sdlc)
            MockSDLCClass.return_value.__exit__ = MagicMock(return_value=None)

            result = await push_artifacts(
                ctx=full_workflow_context,
                project_id="PROJ-1",
                workspace_id="dev",
                commit_message="Initial model push"
            )

            result_data = json.loads(result)
            assert result_data["status"] == "success"
            assert result_data["entity_count"] == 4


class TestClaudeDesktopWorkflowSimulation:
    """Simulate the exact Claude Desktop workflow from debug_issue.md."""

    @pytest.fixture
    def trading_simulation_context(self):
        """Create context simulating trading_simulation project."""
        ctx = MCPContext()
        ctx.package_prefix = "trading_simulation"
        return ctx

    def test_trading_simulation_store_parsing(self, trading_simulation_context):
        """Test parsing trading simulation store."""
        # Simulate the store that was generated
        store_code = '''###Relational
Database trading_simulation::store::TradingDB
(
  Schema main
  (
    Table trades
    (
      id INTEGER PRIMARY KEY,
      symbol VARCHAR(10) NOT NULL,
      quantity INTEGER NOT NULL,
      price DECIMAL(10,2) NOT NULL,
      trade_date TIMESTAMP NOT NULL
    )
    Table positions
    (
      id INTEGER PRIMARY KEY,
      symbol VARCHAR(10) NOT NULL,
      quantity INTEGER NOT NULL,
      avg_price DECIMAL(10,2) NOT NULL
    )
  )
)'''

        trading_simulation_context.add_pending_artifact(
            artifact_type="store",
            pure_code=store_code
        )

        # Validate artifacts
        validation = _validate_artifacts(trading_simulation_context)

        # Store-only model should be valid (just with warnings)
        assert validation["valid"] is True
        assert "store" in validation["artifact_types"]

    def test_parse_entities_from_various_response_structures(self):
        """Test that our fix handles all known Engine response structures."""
        mock_engine = MockEngineClient()

        # Test 1: Standard modelDataContext.elements
        response1 = {
            "modelDataContext": {
                "elements": [{"_type": "relational", "package": "test", "name": "DB1"}]
            }
        }
        entities = mock_engine.extract_entities(response1)
        assert len(entities) == 1

        # Test 2: modelDataContext.stores (the bug case)
        response2 = {
            "modelDataContext": {
                "elements": [],
                "stores": [{"_type": "relational", "package": "test", "name": "DB2"}]
            }
        }
        entities = mock_engine.extract_entities(response2)
        assert len(entities) == 1

        # Test 3: pureModelContextData.elements
        response3 = {
            "pureModelContextData": {
                "elements": [{"_type": "relational", "package": "test", "name": "DB3"}]
            }
        }
        entities = mock_engine.extract_entities(response3)
        assert len(entities) == 1

        # Test 4: Combined (all locations)
        response4 = {
            "modelDataContext": {
                "elements": [{"_type": "class", "package": "a", "name": "C1"}],
                "stores": [{"_type": "relational", "package": "b", "name": "DB"}]
            },
            "pureModelContextData": {
                "elements": [{"_type": "mapping", "package": "c", "name": "M1"}]
            }
        }
        entities = mock_engine.extract_entities(response4)
        assert len(entities) == 3


class TestErrorDiagnosticsWorkflow:
    """Test error diagnostics workflow."""

    def test_debug_parse_response_provides_diagnostics(self):
        """Test that debug_parse_response provides useful diagnostics."""
        mock_engine = MockEngineClient()
        mock_engine.set_default_response({
            "modelDataContext": {
                "elements": [],
                "unknownKey": "some value"
            }
        })

        result = mock_engine.debug_parse_response("###Relational\nDatabase test::DB ()")

        assert "diagnostic" in result
        assert "modelDataContext" in result["diagnostic"]
        assert "elements count: 0" in result["diagnostic"]
        assert result["error"] == "No entities extracted from response"

    @pytest.mark.asyncio
    async def test_push_with_parse_error_includes_diagnostics(self, mcp_context):
        """Test that push errors include diagnostic information."""
        mcp_context.add_pending_artifact(
            artifact_type="store",
            pure_code="invalid pure code that won't parse"
        )

        mock_engine = MockEngineClient()
        mock_engine.set_default_response({"modelDataContext": {"elements": []}})

        with patch("legend_cli.engine_client.EngineClient") as MockEngineClass:
            MockEngineClass.return_value.__enter__ = MagicMock(return_value=mock_engine)
            MockEngineClass.return_value.__exit__ = MagicMock(return_value=None)

            result = await push_artifacts(
                ctx=mcp_context,
                project_id="PROJ-1",
                workspace_id="dev"
            )

            result_data = json.loads(result)
            assert result_data["status"] == "parse_error"
            # Should have diagnostic info
            assert "diagnostics" in result_data or "errors" in result_data


class TestEntityVerificationWorkflow:
    """Test entity verification after push."""

    @pytest.mark.asyncio
    async def test_verification_success(self, mcp_context):
        """Test successful entity verification after push."""
        mcp_context.add_pending_artifact(
            artifact_type="store",
            pure_code="###Relational\nDatabase test::DB ()"
        )

        mock_engine = MockEngineClient()
        mock_engine.set_default_response({
            "modelDataContext": {
                "elements": [{"_type": "relational", "package": "test", "name": "DB"}]
            }
        })

        mock_sdlc = MockSDLCClient()
        mock_sdlc.add_project("PROJ-1", "Test")
        mock_sdlc.add_workspace("PROJ-1", "dev")

        with patch("legend_cli.engine_client.EngineClient") as MockEngineClass, \
             patch("legend_cli.sdlc_client.SDLCClient") as MockSDLCClass:

            MockEngineClass.return_value.__enter__ = MagicMock(return_value=mock_engine)
            MockEngineClass.return_value.__exit__ = MagicMock(return_value=None)
            MockSDLCClass.return_value.__enter__ = MagicMock(return_value=mock_sdlc)
            MockSDLCClass.return_value.__exit__ = MagicMock(return_value=None)

            result = await push_artifacts(
                ctx=mcp_context,
                project_id="PROJ-1",
                workspace_id="dev",
                verify_push=True
            )

            result_data = json.loads(result)
            assert result_data["status"] == "success"
            assert "verification" in result_data
            assert result_data["verification"]["verified"] is True
