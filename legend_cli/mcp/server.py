"""MCP server implementation for Legend CLI.

This module implements a Model Context Protocol (MCP) server that enables
Claude Desktop to interact with Legend for model generation and modification.
"""

import logging
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool,
    TextContent,
    Resource,
    ResourceTemplate,
)

from .context import get_context, reset_context
from .tools import database, model_generation, sdlc, preview, model_modification

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create the MCP server instance
server = Server("legend-cli")


# =============================================================================
# Tool Registration
# =============================================================================

@server.list_tools()
async def list_tools() -> list[Tool]:
    """List all available MCP tools."""
    tools = []

    # Database tools
    tools.extend(database.get_tools())

    # Model generation tools
    tools.extend(model_generation.get_tools())

    # SDLC tools
    tools.extend(sdlc.get_tools())

    # Preview tools
    tools.extend(preview.get_tools())

    # Model modification tools
    tools.extend(model_modification.get_tools())

    return tools


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    ctx = get_context()

    try:
        # Database tools
        if name == "connect_database":
            result = await database.connect_database(ctx, **arguments)
        elif name == "list_databases":
            result = await database.list_databases(ctx, **arguments)
        elif name == "list_schemas":
            result = await database.list_schemas(ctx, **arguments)
        elif name == "list_tables":
            result = await database.list_tables(ctx, **arguments)
        elif name == "describe_table":
            result = await database.describe_table(ctx, **arguments)
        elif name == "introspect_database":
            result = await database.introspect_database(ctx, **arguments)

        # Model generation tools
        elif name == "generate_model":
            result = await model_generation.generate_model(ctx, **arguments)
        elif name == "generate_store":
            result = await model_generation.generate_store(ctx, **arguments)
        elif name == "generate_classes":
            result = await model_generation.generate_classes(ctx, **arguments)
        elif name == "generate_connection":
            result = await model_generation.generate_connection(ctx, **arguments)
        elif name == "generate_mapping":
            result = await model_generation.generate_mapping(ctx, **arguments)
        elif name == "generate_runtime":
            result = await model_generation.generate_runtime(ctx, **arguments)
        elif name == "generate_associations":
            result = await model_generation.generate_associations(ctx, **arguments)
        elif name == "analyze_schema":
            result = await model_generation.analyze_schema(ctx, **arguments)

        # SDLC tools
        elif name == "list_projects":
            result = await sdlc.list_projects(ctx, **arguments)
        elif name == "create_project":
            result = await sdlc.create_project(ctx, **arguments)
        elif name == "list_workspaces":
            result = await sdlc.list_workspaces(ctx, **arguments)
        elif name == "create_workspace":
            result = await sdlc.create_workspace(ctx, **arguments)
        elif name == "get_workspace_entities":
            result = await sdlc.get_workspace_entities(ctx, **arguments)
        elif name == "push_artifacts":
            result = await sdlc.push_artifacts(ctx, **arguments)

        # Preview tools
        elif name == "preview_changes":
            result = await preview.preview_changes(ctx, **arguments)
        elif name == "validate_pure_code":
            result = await preview.validate_pure_code(ctx, **arguments)
        elif name == "validate_model_completeness":
            result = await preview.validate_model_completeness(ctx, **arguments)

        # Model modification tools
        elif name == "read_entity":
            result = await model_modification.read_entity(ctx, **arguments)
        elif name == "read_entities":
            result = await model_modification.read_entities(ctx, **arguments)
        elif name == "add_property":
            result = await model_modification.add_property(ctx, **arguments)
        elif name == "remove_property":
            result = await model_modification.remove_property(ctx, **arguments)
        elif name == "create_class":
            result = await model_modification.create_class(ctx, **arguments)
        elif name == "create_association":
            result = await model_modification.create_association(ctx, **arguments)
        elif name == "create_function":
            result = await model_modification.create_function(ctx, **arguments)
        elif name == "delete_entity":
            result = await model_modification.delete_entity(ctx, **arguments)
        elif name == "update_entity":
            result = await model_modification.update_entity(ctx, **arguments)

        else:
            result = f"Unknown tool: {name}"

        return [TextContent(type="text", text=str(result))]

    except Exception as e:
        logger.exception(f"Error in tool {name}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


# =============================================================================
# Resource Registration
# =============================================================================

@server.list_resources()
async def list_resources() -> list[Resource]:
    """List available resources."""
    ctx = get_context()
    resources = []

    # Add resources for connected databases
    for key, conn in ctx.connections.items():
        resources.append(Resource(
            uri=f"legend://database/{key}",
            name=f"Database: {conn.database_name}",
            description=f"Connected {conn.db_type.value} database",
            mimeType="application/json"
        ))

    # Add resources for introspected schemas
    for key, schema in ctx.introspected_schemas.items():
        resources.append(Resource(
            uri=f"legend://schema/{key}",
            name=f"Schema: {schema.name}",
            description=f"Introspected schema with {len(schema.get_all_tables())} tables",
            mimeType="application/json"
        ))

    # Add resources for pending artifacts
    if ctx.pending_artifacts:
        resources.append(Resource(
            uri="legend://pending-artifacts",
            name="Pending Artifacts",
            description=f"{len(ctx.pending_artifacts)} artifacts pending for push",
            mimeType="application/json"
        ))

    return resources


@server.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    """List available resource templates."""
    return [
        ResourceTemplate(
            uriTemplate="legend://database/{db_type}/{database}",
            name="Database Connection",
            description="Access a connected database"
        ),
        ResourceTemplate(
            uriTemplate="legend://schema/{db_type}/{database}",
            name="Database Schema",
            description="Access an introspected database schema"
        ),
        ResourceTemplate(
            uriTemplate="legend://entity/{project_id}/{workspace_id}/{entity_path}",
            name="SDLC Entity",
            description="Access an entity in SDLC workspace"
        ),
    ]


@server.read_resource()
async def read_resource(uri: str) -> str:
    """Read a resource by URI."""
    import json
    ctx = get_context()

    if uri.startswith("legend://database/"):
        key = uri.replace("legend://database/", "")
        conn = ctx.connections.get(key)
        if conn:
            return json.dumps({
                "db_type": conn.db_type.value,
                "database": conn.database_name,
                "connected": conn.is_connected,
                "params": conn.connection_params
            }, indent=2)
        return json.dumps({"error": "Database not found"})

    elif uri.startswith("legend://schema/"):
        key = uri.replace("legend://schema/", "")
        schema = ctx.introspected_schemas.get(key)
        if schema:
            return json.dumps({
                "name": schema.name,
                "schemas": [
                    {
                        "name": s.name,
                        "tables": [
                            {
                                "name": t.name,
                                "columns": [
                                    {"name": c.name, "type": c.data_type, "nullable": c.is_nullable}
                                    for c in t.columns
                                ],
                                "primary_keys": t.primary_key_columns
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
                        "type": r.relationship_type
                    }
                    for r in schema.relationships
                ]
            }, indent=2)
        return json.dumps({"error": "Schema not found"})

    elif uri == "legend://pending-artifacts":
        return json.dumps({
            "count": len(ctx.pending_artifacts),
            "artifacts": [
                {
                    "type": a.artifact_type,
                    "path": a.path,
                    "preview": a.pure_code[:200] + "..." if len(a.pure_code) > 200 else a.pure_code
                }
                for a in ctx.pending_artifacts
            ]
        }, indent=2)

    return json.dumps({"error": f"Unknown resource: {uri}"})


# =============================================================================
# Server Lifecycle
# =============================================================================

async def run_server():
    """Run the MCP server."""
    logger.info("Starting Legend CLI MCP server...")

    # Reset context for fresh session
    reset_context()

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


def main():
    """Entry point for the MCP server."""
    import asyncio
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
