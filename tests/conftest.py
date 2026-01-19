"""Shared pytest fixtures for Legend CLI tests."""

import pytest
from typing import Dict, Any, List

from legend_cli.mcp.context import MCPContext, PendingArtifact
from legend_cli.database.models import Database, Schema, Table, Column


@pytest.fixture
def mcp_context():
    """Create a fresh MCPContext for each test."""
    return MCPContext()


@pytest.fixture
def sample_database():
    """Create a sample Database model with tables and columns."""
    return Database(
        name="TestDB",
        schemas=[
            Schema(
                name="main",
                tables=[
                    Table(
                        name="users",
                        schema="main",
                        columns=[
                            Column(name="id", data_type="INTEGER", is_nullable=False, is_primary_key=True),
                            Column(name="name", data_type="VARCHAR(255)", is_nullable=False),
                            Column(name="email", data_type="VARCHAR(255)", is_nullable=True),
                            Column(name="created_at", data_type="TIMESTAMP", is_nullable=False),
                        ],
                    ),
                    Table(
                        name="orders",
                        schema="main",
                        columns=[
                            Column(name="id", data_type="INTEGER", is_nullable=False, is_primary_key=True),
                            Column(name="user_id", data_type="INTEGER", is_nullable=False),
                            Column(name="total", data_type="DECIMAL(10,2)", is_nullable=False),
                            Column(name="status", data_type="VARCHAR(50)", is_nullable=False),
                        ],
                    ),
                ],
            ),
        ],
    )


@pytest.fixture
def sample_store_pure_code():
    """Sample Pure code for a relational store."""
    return '''###Relational
Database model::store::TestDB
(
  Schema main
  (
    Table users
    (
      id INTEGER PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      email VARCHAR(255),
      created_at TIMESTAMP NOT NULL
    )
    Table orders
    (
      id INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      total DECIMAL(10,2) NOT NULL,
      status VARCHAR(50) NOT NULL
    )
  )
)'''


@pytest.fixture
def sample_class_pure_code():
    """Sample Pure code for classes."""
    return '''###Pure
Class model::domain::User
{
  id: Integer[1];
  name: String[1];
  email: String[0..1];
  createdAt: DateTime[1];
}

Class model::domain::Order
{
  id: Integer[1];
  userId: Integer[1];
  total: Decimal[1];
  status: String[1];
}'''


@pytest.fixture
def sample_connection_pure_code():
    """Sample Pure code for a connection."""
    return '''###Connection
RelationalDatabaseConnection model::connection::TestDBConnection
{
  store: model::store::TestDB;
  type: DuckDB;
  specification: LocalDuckDB
  {
    path: '/path/to/test.db';
  };
  auth: DefaultH2;
}'''


@pytest.fixture
def sample_mapping_pure_code():
    """Sample Pure code for a mapping."""
    return '''###Mapping
Mapping model::mapping::TestDBMapping
(
  model::domain::User: Relational
  {
    ~primaryKey
    (
      [model::store::TestDB]main.users.id
    )
    ~mainTable [model::store::TestDB]main.users
    id: [model::store::TestDB]main.users.id,
    name: [model::store::TestDB]main.users.name,
    email: [model::store::TestDB]main.users.email,
    createdAt: [model::store::TestDB]main.users.created_at
  }
)'''


@pytest.fixture
def mock_engine_relational_response():
    """Mock Engine API response for a relational database."""
    return {
        "modelDataContext": {
            "elements": [
                {
                    "_type": "relational",
                    "package": "model::store",
                    "name": "TestDB",
                    "schemas": [
                        {
                            "name": "main",
                            "tables": [
                                {
                                    "name": "users",
                                    "columns": [
                                        {"name": "id", "type": "INTEGER"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }


@pytest.fixture
def mock_engine_class_response():
    """Mock Engine API response for classes."""
    return {
        "modelDataContext": {
            "elements": [
                {
                    "_type": "class",
                    "package": "model::domain",
                    "name": "User",
                    "properties": []
                },
                {
                    "_type": "class",
                    "package": "model::domain",
                    "name": "Order",
                    "properties": []
                }
            ]
        }
    }


@pytest.fixture
def mock_engine_connection_response():
    """Mock Engine API response for a connection."""
    return {
        "modelDataContext": {
            "elements": [
                {
                    "_type": "relationalDatabaseConnection",
                    "package": "model::connection",
                    "name": "TestDBConnection",
                    "store": "model::store::TestDB"
                }
            ]
        }
    }


@pytest.fixture
def mock_engine_mapping_response():
    """Mock Engine API response for a mapping."""
    return {
        "modelDataContext": {
            "elements": [
                {
                    "_type": "mapping",
                    "package": "model::mapping",
                    "name": "TestDBMapping"
                }
            ]
        }
    }


@pytest.fixture
def mock_engine_empty_response():
    """Mock Engine API response with no elements (bug case)."""
    return {
        "modelDataContext": {
            "elements": []
        }
    }


@pytest.fixture
def mock_engine_stores_response():
    """Mock Engine API response with elements in 'stores' key instead of 'elements'."""
    return {
        "modelDataContext": {
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


@pytest.fixture
def mock_engine_pure_model_context_response():
    """Mock Engine API response using pureModelContextData instead of modelDataContext."""
    return {
        "pureModelContextData": {
            "elements": [
                {
                    "_type": "relational",
                    "package": "model::store",
                    "name": "TestDB"
                }
            ]
        }
    }


@pytest.fixture
def pending_artifacts_full_model(
    sample_store_pure_code,
    sample_class_pure_code,
    sample_connection_pure_code,
    sample_mapping_pure_code
):
    """Create a list of pending artifacts for a full model."""
    return [
        PendingArtifact(artifact_type="store", pure_code=sample_store_pure_code),
        PendingArtifact(artifact_type="classes", pure_code=sample_class_pure_code),
        PendingArtifact(artifact_type="connection", pure_code=sample_connection_pure_code),
        PendingArtifact(artifact_type="mapping", pure_code=sample_mapping_pure_code),
    ]


@pytest.fixture
def mock_engine_code_error_response():
    """Mock Engine API response with codeError (parsing failure)."""
    return {
        "codeError": {
            "message": "Parsing error: unexpected token 'INVALID'",
            "sourceInformation": {
                "startLine": 10,
                "startColumn": 5,
                "endLine": 10,
                "endColumn": 20,
            }
        },
        "isolatedLambdas": {},
        "renderStyle": "STANDARD"
    }


@pytest.fixture
def mock_engine_code_error_multiline():
    """Mock Engine API response with codeError spanning multiple lines."""
    return {
        "codeError": {
            "message": "Expected '}' to close block",
            "sourceInformation": {
                "startLine": 5,
                "startColumn": 1,
                "endLine": 10,
                "endColumn": 15,
            }
        },
        "isolatedLambdas": {},
        "renderStyle": "STANDARD"
    }


@pytest.fixture
def sample_invalid_pure_code():
    """Sample Pure code with syntax error."""
    return '''###Pure
Class model::domain::User
{
  id: Integer[1];
  INVALID SYNTAX HERE
  name: String[1];
}'''


@pytest.fixture
def sample_unclosed_pure_code():
    """Sample Pure code with unclosed brace."""
    return '''###Pure
Class model::domain::User
{
  id: Integer[1];
  name: String[1];
'''
