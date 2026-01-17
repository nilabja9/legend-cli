"""Database MCP tools for Legend CLI.

Provides tools for connecting to databases, introspecting schemas,
and listing database objects.
"""

import json
from typing import Any, List, Optional

from mcp.types import Tool

from ..context import MCPContext, DatabaseType
from ..errors import ConnectionError, DatabaseError, IntrospectionError


def get_tools() -> List[Tool]:
    """Return all database-related tools."""
    return [
        Tool(
            name="connect_database",
            description="Connect to a Snowflake or DuckDB database. For Snowflake, requires account, user credentials via environment variables. For DuckDB, requires the path to the .duckdb file.",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type": {
                        "type": "string",
                        "enum": ["snowflake", "duckdb"],
                        "description": "Type of database to connect to"
                    },
                    "database": {
                        "type": "string",
                        "description": "For Snowflake: database name. For DuckDB: path to .duckdb file"
                    },
                    "warehouse": {
                        "type": "string",
                        "description": "Snowflake warehouse name (Snowflake only)"
                    },
                    "role": {
                        "type": "string",
                        "description": "Snowflake role (Snowflake only, default: from env)"
                    }
                },
                "required": ["db_type", "database"]
            }
        ),
        Tool(
            name="list_databases",
            description="List available databases. For DuckDB, this lists the attached databases. For Snowflake, this requires an existing connection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type": {
                        "type": "string",
                        "enum": ["snowflake", "duckdb"],
                        "description": "Type of database"
                    },
                    "database": {
                        "type": "string",
                        "description": "Database identifier (path for DuckDB, name for Snowflake)"
                    }
                },
                "required": ["db_type", "database"]
            }
        ),
        Tool(
            name="list_schemas",
            description="List all schemas in a database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type": {
                        "type": "string",
                        "enum": ["snowflake", "duckdb"],
                        "description": "Type of database"
                    },
                    "database": {
                        "type": "string",
                        "description": "Database identifier"
                    }
                },
                "required": ["db_type", "database"]
            }
        ),
        Tool(
            name="list_tables",
            description="List all tables in a database schema.",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type": {
                        "type": "string",
                        "enum": ["snowflake", "duckdb"],
                        "description": "Type of database"
                    },
                    "database": {
                        "type": "string",
                        "description": "Database identifier"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name"
                    },
                    "include_views": {
                        "type": "boolean",
                        "description": "Whether to include views (default: true)",
                        "default": True
                    }
                },
                "required": ["db_type", "database", "schema"]
            }
        ),
        Tool(
            name="describe_table",
            description="Get detailed information about a table including columns, types, and constraints.",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type": {
                        "type": "string",
                        "enum": ["snowflake", "duckdb"],
                        "description": "Type of database"
                    },
                    "database": {
                        "type": "string",
                        "description": "Database identifier"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name"
                    },
                    "table": {
                        "type": "string",
                        "description": "Table name"
                    }
                },
                "required": ["db_type", "database", "schema", "table"]
            }
        ),
        Tool(
            name="introspect_database",
            description="Perform full database introspection including all schemas, tables, columns, and relationship detection. This creates a complete schema model ready for Pure code generation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_type": {
                        "type": "string",
                        "enum": ["snowflake", "duckdb"],
                        "description": "Type of database"
                    },
                    "database": {
                        "type": "string",
                        "description": "Database identifier"
                    },
                    "schema_filter": {
                        "type": "string",
                        "description": "Optional: filter to specific schema"
                    },
                    "detect_relationships": {
                        "type": "boolean",
                        "description": "Whether to detect foreign key relationships (default: true)",
                        "default": True
                    }
                },
                "required": ["db_type", "database"]
            }
        ),
    ]


def _get_introspector(db_type: str, database: str, **kwargs):
    """Get the appropriate database introspector."""
    db_type_enum = DatabaseType(db_type.lower())

    if db_type_enum == DatabaseType.SNOWFLAKE:
        from legend_cli.database.snowflake import SnowflakeIntrospector
        return SnowflakeIntrospector(
            warehouse=kwargs.get("warehouse"),
            role=kwargs.get("role")
        )
    elif db_type_enum == DatabaseType.DUCKDB:
        from legend_cli.database.duckdb import DuckDBIntrospector
        return DuckDBIntrospector(database_path=database)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


async def connect_database(
    ctx: MCPContext,
    db_type: str,
    database: str,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
) -> str:
    """Connect to a database."""
    try:
        db_type_enum = DatabaseType(db_type.lower())

        # Check if already connected
        existing = ctx.get_connection(db_type_enum, database)
        if existing and existing.is_connected:
            return json.dumps({
                "status": "already_connected",
                "db_type": db_type,
                "database": database,
                "message": "Database connection already exists"
            })

        # Create introspector
        introspector = _get_introspector(db_type, database, warehouse=warehouse, role=role)

        # Test connection
        introspector.connect(database)

        # Store connection in context
        ctx.add_connection(
            db_type=db_type_enum,
            database=database,
            introspector=introspector,
            connection_params={
                "warehouse": warehouse,
                "role": role
            }
        )

        return json.dumps({
            "status": "connected",
            "db_type": db_type,
            "database": database,
            "message": f"Successfully connected to {db_type} database: {database}"
        })

    except ImportError as e:
        raise ConnectionError(
            f"Missing database driver: {str(e)}",
            details={"db_type": db_type, "database": database}
        )
    except Exception as e:
        raise ConnectionError(
            f"Failed to connect to {db_type} database: {str(e)}",
            details={"db_type": db_type, "database": database}
        )


async def list_databases(ctx: MCPContext, db_type: str, database: str) -> str:
    """List available databases."""
    try:
        db_type_enum = DatabaseType(db_type.lower())

        # Get or create connection
        conn = ctx.get_connection(db_type_enum, database)
        if not conn:
            introspector = _get_introspector(db_type, database)
            introspector.connect(database)
            ctx.add_connection(db_type_enum, database, introspector)
            conn = ctx.get_connection(db_type_enum, database)

        # For DuckDB, we can only access the connected database
        if db_type_enum == DatabaseType.DUCKDB:
            return json.dumps({
                "databases": [conn.introspector.get_database_name()],
                "message": "DuckDB shows the current connected database"
            })

        # For Snowflake, we could potentially list databases
        # but this requires SHOW DATABASES permission
        return json.dumps({
            "databases": [database],
            "message": "Currently connected database"
        })

    except Exception as e:
        raise DatabaseError(f"Failed to list databases: {str(e)}")


async def list_schemas(ctx: MCPContext, db_type: str, database: str) -> str:
    """List schemas in a database."""
    try:
        db_type_enum = DatabaseType(db_type.lower())

        # Get or create connection
        conn = ctx.get_connection(db_type_enum, database)
        if not conn:
            introspector = _get_introspector(db_type, database)
            introspector.connect(database)
            ctx.add_connection(db_type_enum, database, introspector)
            conn = ctx.get_connection(db_type_enum, database)

        schemas = conn.introspector.get_schemas(database)

        return json.dumps({
            "database": database,
            "schemas": schemas,
            "count": len(schemas)
        })

    except Exception as e:
        raise DatabaseError(f"Failed to list schemas: {str(e)}")


async def list_tables(
    ctx: MCPContext,
    db_type: str,
    database: str,
    schema: str,
    include_views: bool = True
) -> str:
    """List tables in a schema."""
    try:
        db_type_enum = DatabaseType(db_type.lower())

        # Get or create connection
        conn = ctx.get_connection(db_type_enum, database)
        if not conn:
            introspector = _get_introspector(db_type, database)
            introspector.connect(database)
            ctx.add_connection(db_type_enum, database, introspector)
            conn = ctx.get_connection(db_type_enum, database)

        tables = conn.introspector.get_tables(database, schema, include_views)

        return json.dumps({
            "database": database,
            "schema": schema,
            "tables": tables,
            "count": len(tables),
            "include_views": include_views
        })

    except Exception as e:
        raise DatabaseError(f"Failed to list tables: {str(e)}")


async def describe_table(
    ctx: MCPContext,
    db_type: str,
    database: str,
    schema: str,
    table: str
) -> str:
    """Describe a table's structure."""
    try:
        db_type_enum = DatabaseType(db_type.lower())

        # Get or create connection
        conn = ctx.get_connection(db_type_enum, database)
        if not conn:
            introspector = _get_introspector(db_type, database)
            introspector.connect(database)
            ctx.add_connection(db_type_enum, database, introspector)
            conn = ctx.get_connection(db_type_enum, database)

        columns = conn.introspector.get_columns(database, schema, table)
        primary_keys = conn.introspector.get_primary_keys(database, schema, table)

        return json.dumps({
            "database": database,
            "schema": schema,
            "table": table,
            "columns": [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "is_nullable": col.is_nullable,
                    "is_primary_key": col.name in primary_keys,
                    "pure_type": col.to_pure_property_type()
                }
                for col in columns
            ],
            "primary_keys": primary_keys,
            "column_count": len(columns)
        })

    except Exception as e:
        raise DatabaseError(f"Failed to describe table: {str(e)}")


async def introspect_database(
    ctx: MCPContext,
    db_type: str,
    database: str,
    schema_filter: Optional[str] = None,
    detect_relationships: bool = True
) -> str:
    """Perform full database introspection."""
    try:
        db_type_enum = DatabaseType(db_type.lower())

        # Get or create connection
        conn = ctx.get_connection(db_type_enum, database)
        if not conn:
            introspector = _get_introspector(db_type, database)
            introspector.connect(database)
            ctx.add_connection(db_type_enum, database, introspector)
            conn = ctx.get_connection(db_type_enum, database)

        # Introspect the database
        db_schema = conn.introspector.introspect_database(
            database=database,
            schema_filter=schema_filter,
            detect_relationships=detect_relationships
        )

        # Store in context for later use
        ctx.store_schema(db_type_enum, database, db_schema)

        # Build summary
        total_tables = sum(len(s.tables) for s in db_schema.schemas)
        total_columns = sum(
            len(t.columns)
            for s in db_schema.schemas
            for t in s.tables
        )

        return json.dumps({
            "status": "success",
            "database": database,
            "schema_filter": schema_filter,
            "summary": {
                "schemas": len(db_schema.schemas),
                "tables": total_tables,
                "columns": total_columns,
                "relationships": len(db_schema.relationships)
            },
            "schemas": [
                {
                    "name": s.name,
                    "tables": [
                        {
                            "name": t.name,
                            "columns": len(t.columns),
                            "primary_keys": t.primary_key_columns
                        }
                        for t in s.tables
                    ]
                }
                for s in db_schema.schemas
            ],
            "relationships": [
                {
                    "source": f"{r.source_table}.{r.source_column}",
                    "target": f"{r.target_table}.{r.target_column}",
                    "type": r.relationship_type,
                    "property_name": r.property_name
                }
                for r in db_schema.relationships
            ],
            "message": f"Successfully introspected {total_tables} tables with {len(db_schema.relationships)} relationships detected"
        })

    except Exception as e:
        raise IntrospectionError(
            f"Failed to introspect database: {str(e)}",
            details={"db_type": db_type, "database": database, "schema_filter": schema_filter}
        )
