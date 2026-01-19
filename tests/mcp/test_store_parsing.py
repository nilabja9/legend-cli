"""Tests for store parsing bug fixes.

These tests verify that the EngineClient correctly extracts entities from
various Engine API response structures, including edge cases that caused
the '0 entities found' bug.
"""

import pytest
from legend_cli.engine_client import EngineClient
from .fixtures import MockEngineClient, create_mock_engine_with_store_response, create_mock_engine_with_bug


class TestExtractEntitiesWithRelationalType:
    """Test that 'relational' type maps correctly to Database classifier."""

    def test_relational_type_maps_to_database(self, mock_engine_relational_response):
        """Verify 'relational' _type maps to meta::relational::metamodel::Database."""
        mock = MockEngineClient()
        entities = mock.extract_entities(mock_engine_relational_response)

        assert len(entities) == 1
        assert entities[0]["path"] == "model::store::TestDB"
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"

    def test_database_type_maps_correctly(self):
        """Verify 'database' _type also maps to Database classifier (case variant)."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "database",
                        "package": "model::store",
                        "name": "AnotherDB"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert len(entities) == 1
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"

    def test_relational_database_type_maps_correctly(self):
        """Verify 'relationalDatabase' _type maps correctly."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "relationalDatabase",
                        "package": "model::store",
                        "name": "TestDB"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert len(entities) == 1
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"


class TestExtractEntitiesEmptyElements:
    """Test handling of responses with empty elements array (the bug case)."""

    def test_empty_elements_array(self, mock_engine_empty_response):
        """Handle empty elements array gracefully."""
        mock = MockEngineClient()
        entities = mock.extract_entities(mock_engine_empty_response)

        assert entities == []

    def test_no_model_data_context(self):
        """Handle response with no modelDataContext."""
        mock = MockEngineClient()
        response = {"someOtherKey": {}}
        entities = mock.extract_entities(response)

        assert entities == []

    def test_none_elements(self):
        """Handle None elements value."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": None
            }
        }
        entities = mock.extract_entities(response)

        assert entities == []


class TestExtractEntitiesStoresKey:
    """Test extraction from 'stores' key instead of 'elements'."""

    def test_extracts_from_stores_key(self, mock_engine_stores_response):
        """Verify entities are extracted from stores key when elements is empty."""
        mock = MockEngineClient()
        entities = mock.extract_entities(mock_engine_stores_response)

        assert len(entities) == 1
        assert entities[0]["path"] == "model::store::TestDB"
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"

    def test_combines_elements_and_stores(self):
        """Verify both elements and stores are combined."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "class",
                        "package": "model::domain",
                        "name": "User"
                    }
                ],
                "stores": [
                    {
                        "_type": "relational",
                        "package": "model::store",
                        "name": "TestDB"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert len(entities) == 2
        paths = [e["path"] for e in entities]
        assert "model::domain::User" in paths
        assert "model::store::TestDB" in paths


class TestExtractEntitiesPureModelContextData:
    """Test extraction from pureModelContextData alternative structure."""

    def test_extracts_from_pure_model_context_data(self, mock_engine_pure_model_context_response):
        """Verify entities are extracted from pureModelContextData."""
        mock = MockEngineClient()
        entities = mock.extract_entities(mock_engine_pure_model_context_response)

        assert len(entities) == 1
        assert entities[0]["path"] == "model::store::TestDB"

    def test_extracts_from_pure_model_context_stores(self):
        """Verify extraction from pureModelContextData.stores."""
        mock = MockEngineClient()
        response = {
            "pureModelContextData": {
                "elements": [],
                "stores": [
                    {
                        "_type": "relational",
                        "package": "model::store",
                        "name": "TestDB"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert len(entities) == 1
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"


class TestClassifierMapping:
    """Parametrized tests for all type-to-classifier mappings."""

    @pytest.mark.parametrize("element_type,expected_classifier", [
        # Class types
        ("class", "meta::pure::metamodel::type::Class"),
        ("Class", "meta::pure::metamodel::type::Class"),
        # Database types
        ("relational", "meta::relational::metamodel::Database"),
        ("Relational", "meta::relational::metamodel::Database"),
        ("database", "meta::relational::metamodel::Database"),
        ("Database", "meta::relational::metamodel::Database"),
        ("relationalDatabase", "meta::relational::metamodel::Database"),
        ("RelationalDatabase", "meta::relational::metamodel::Database"),
        # Connection types
        ("relationalDatabaseConnection", "meta::pure::runtime::PackageableConnection"),
        ("RelationalDatabaseConnection", "meta::pure::runtime::PackageableConnection"),
        ("connection", "meta::pure::runtime::PackageableConnection"),
        ("Connection", "meta::pure::runtime::PackageableConnection"),
        # Mapping types
        ("mapping", "meta::pure::mapping::Mapping"),
        ("Mapping", "meta::pure::mapping::Mapping"),
        # Runtime types
        ("packageableRuntime", "meta::pure::runtime::PackageableRuntime"),
        ("PackageableRuntime", "meta::pure::runtime::PackageableRuntime"),
        ("runtime", "meta::pure::runtime::PackageableRuntime"),
        # Association types
        ("association", "meta::pure::metamodel::relationship::Association"),
        ("Association", "meta::pure::metamodel::relationship::Association"),
        # Other types
        ("profile", "meta::pure::metamodel::extension::Profile"),
        ("enumeration", "meta::pure::metamodel::type::Enumeration"),
        ("function", "meta::pure::metamodel::function::ConcreteFunctionDefinition"),
    ])
    def test_classifier_mapping(self, element_type, expected_classifier):
        """Test that element types map to correct classifier paths."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": element_type,
                        "package": "test::pkg",
                        "name": "TestEntity"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert len(entities) == 1
        assert entities[0]["classifierPath"] == expected_classifier

    def test_unknown_type_uses_fallback(self):
        """Test that unknown types use fallback classifier."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "unknownCustomType",
                        "package": "test::pkg",
                        "name": "TestEntity"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert len(entities) == 1
        assert entities[0]["classifierPath"] == "meta::pure::metamodel::unknownCustomType"


class TestSectionIndexFiltering:
    """Test that sectionIndex elements are filtered out."""

    def test_filters_section_index(self):
        """Verify sectionIndex elements are not included in extracted entities."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "sectionIndex",
                        "sections": []
                    },
                    {
                        "_type": "class",
                        "package": "model::domain",
                        "name": "User"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert len(entities) == 1
        assert entities[0]["path"] == "model::domain::User"


class TestMockEngineWithBugRepro:
    """Test that the mock can reproduce the bug scenario."""

    def test_mock_with_bug_extracts_from_stores(self):
        """The fixed code should extract entities even with the 'bug' response."""
        mock = create_mock_engine_with_bug()
        entities = mock.parse_pure_code("###Relational\nDatabase test::DB ()")

        # With the fix, entities should be found in 'stores'
        assert len(entities) == 1
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"

    def test_mock_with_store_response_works(self):
        """Test the standard mock returns expected responses."""
        mock = create_mock_engine_with_store_response()

        # Test relational database
        entities = mock.parse_pure_code("###Relational\nDatabase model::store::TestDB ()")
        assert len(entities) == 1
        assert entities[0]["classifierPath"] == "meta::relational::metamodel::Database"

        # Test class
        entities = mock.parse_pure_code("###Pure\nClass model::domain::User {}")
        assert len(entities) == 1
        assert entities[0]["classifierPath"] == "meta::pure::metamodel::type::Class"


class TestDebugParseResponse:
    """Test the debug_parse_response diagnostic method."""

    def test_debug_returns_diagnostic_info(self):
        """Verify debug_parse_response returns useful diagnostic information."""
        mock = MockEngineClient()
        mock.set_default_response({
            "modelDataContext": {
                "elements": [
                    {"_type": "class", "package": "test", "name": "Foo"}
                ]
            }
        })

        result = mock.debug_parse_response("###Pure\nClass test::Foo {}")

        assert "raw_response" in result
        assert "diagnostic" in result
        assert "entities" in result
        assert result["raw_response"] is not None
        assert "modelDataContext" in result["diagnostic"]
        assert len(result["entities"]) == 1

    def test_debug_reports_no_entities_error(self, mock_engine_empty_response):
        """Verify debug reports error when no entities found."""
        mock = MockEngineClient()
        mock.set_default_response(mock_engine_empty_response)

        result = mock.debug_parse_response("some code")

        assert result["error"] == "No entities extracted from response"
        assert len(result["entities"]) == 0


class TestEntityPathGeneration:
    """Test that entity paths are generated correctly."""

    def test_path_with_package(self):
        """Entity path should be package::name when package exists."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "class",
                        "package": "model::domain",
                        "name": "User"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert entities[0]["path"] == "model::domain::User"

    def test_path_without_package(self):
        """Entity path should be just name when package is empty."""
        mock = MockEngineClient()
        response = {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "class",
                        "package": "",
                        "name": "RootClass"
                    }
                ]
            }
        }
        entities = mock.extract_entities(response)

        assert entities[0]["path"] == "RootClass"
