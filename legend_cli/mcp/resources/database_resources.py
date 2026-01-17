"""Database-related MCP resources for Legend CLI.

Provides resources for accessing connected databases and introspected schemas.
"""

import json
from typing import List

from mcp.types import Resource

from ..context import get_context


def get_database_resources() -> List[Resource]:
    """Get all database-related resources."""
    ctx = get_context()
    resources = []

    # Add resources for connected databases
    for key, conn in ctx.connections.items():
        resources.append(Resource(
            uri=f"legend://database/{key}",
            name=f"Database: {conn.database_name}",
            description=f"Connected {conn.db_type.value} database: {conn.database_name}",
            mimeType="application/json"
        ))

    # Add resources for introspected schemas
    for key, schema in ctx.introspected_schemas.items():
        table_count = sum(len(s.tables) for s in schema.schemas)
        resources.append(Resource(
            uri=f"legend://schema/{key}",
            name=f"Schema: {schema.name}",
            description=f"Introspected schema with {table_count} tables",
            mimeType="application/json"
        ))

    return resources


def read_database_resource(key: str) -> str:
    """Read a database connection resource."""
    ctx = get_context()
    conn = ctx.connections.get(key)

    if not conn:
        return json.dumps({
            "error": f"Database connection not found: {key}",
            "available_connections": list(ctx.connections.keys())
        })

    return json.dumps({
        "db_type": conn.db_type.value,
        "database": conn.database_name,
        "connected": conn.is_connected,
        "connection_params": conn.connection_params
    }, indent=2)


def read_schema_resource(key: str) -> str:
    """Read a database schema resource."""
    ctx = get_context()
    schema = ctx.introspected_schemas.get(key)

    if not schema:
        return json.dumps({
            "error": f"Schema not found: {key}",
            "available_schemas": list(ctx.introspected_schemas.keys())
        })

    return json.dumps({
        "name": schema.name,
        "schemas": [
            {
                "name": s.name,
                "table_count": len(s.tables),
                "tables": [
                    {
                        "name": t.name,
                        "column_count": len(t.columns),
                        "columns": [
                            {
                                "name": c.name,
                                "type": c.data_type,
                                "nullable": c.is_nullable,
                                "primary_key": c.is_primary_key
                            }
                            for c in t.columns
                        ],
                        "primary_keys": t.primary_key_columns,
                        "relationship_count": len(t.relationships)
                    }
                    for t in s.tables
                ]
            }
            for s in schema.schemas
        ],
        "relationships": [
            {
                "source": f"{r.source_table}.{r.source_column}",
                "target": f"{r.target_table}.{r.target_column}",
                "type": r.relationship_type,
                "property_name": r.property_name
            }
            for r in schema.relationships
        ],
        "summary": {
            "schema_count": len(schema.schemas),
            "table_count": sum(len(s.tables) for s in schema.schemas),
            "relationship_count": len(schema.relationships)
        }
    }, indent=2)
