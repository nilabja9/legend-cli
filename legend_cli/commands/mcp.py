"""MCP server commands for Legend CLI."""

import typer
from rich.console import Console

app = typer.Typer(
    name="mcp",
    help="Model Context Protocol (MCP) server commands",
    add_completion=False,
)

console = Console()


@app.command()
def serve():
    """Start the MCP server for Claude Desktop integration.

    This starts an MCP server that communicates over stdio, enabling Claude Desktop
    to interact with Legend for model generation and modification.

    Configuration in Claude Desktop (~/.config/claude/claude_desktop_config.json):

        {
            "mcpServers": {
                "legend-cli": {
                    "command": "legend-cli",
                    "args": ["mcp", "serve"],
                    "env": {
                        "LEGEND_SDLC_URL": "http://localhost:6900/sdlc/api",
                        "LEGEND_PAT": "your-personal-access-token"
                    }
                }
            }
        }
    """
    try:
        from legend_cli.mcp import main as mcp_main
        mcp_main()
    except ImportError as e:
        console.print(f"[red]Error: MCP dependencies not installed.[/red]")
        console.print(f"Install with: pip install 'legend-cli[mcp]'")
        console.print(f"Details: {e}")
        raise typer.Exit(1)


@app.command()
def info():
    """Show MCP server information and available tools."""
    console.print("[bold]Legend CLI MCP Server[/bold]")
    console.print()
    console.print("The MCP server enables Claude Desktop to interact with Legend for:")
    console.print("  - Database introspection (Snowflake, DuckDB)")
    console.print("  - Pure code generation (stores, classes, connections, mappings)")
    console.print("  - Enhanced schema analysis (enums, hierarchies, constraints)")
    console.print("  - SDLC operations (projects, workspaces, entity management)")
    console.print("  - Model modification (add/remove properties, create classes)")
    console.print()
    console.print("[bold]Available Tools:[/bold]")
    console.print()
    console.print("[cyan]Database Operations:[/cyan]")
    console.print("  - connect_database    Connect to Snowflake or DuckDB")
    console.print("  - list_databases      List available databases")
    console.print("  - list_schemas        List schemas in database")
    console.print("  - list_tables         List tables in schema")
    console.print("  - describe_table      Get table structure")
    console.print("  - introspect_database Full schema introspection")
    console.print()
    console.print("[cyan]Model Generation:[/cyan]")
    console.print("  - generate_model      End-to-end model generation")
    console.print("  - generate_store      Generate store definition")
    console.print("  - generate_classes    Generate class definitions")
    console.print("  - generate_connection Generate connection definition")
    console.print("  - generate_mapping    Generate mapping definition")
    console.print("  - generate_runtime    Generate runtime definition")
    console.print("  - generate_associations Generate associations")
    console.print("  - analyze_schema      Enhanced schema analysis")
    console.print()
    console.print("[cyan]SDLC Operations:[/cyan]")
    console.print("  - list_projects       List all projects")
    console.print("  - list_workspaces     List workspaces in project")
    console.print("  - create_workspace    Create new workspace")
    console.print("  - get_workspace_entities Get entities in workspace")
    console.print("  - push_artifacts      Push artifacts to SDLC")
    console.print()
    console.print("[cyan]Preview & Validation:[/cyan]")
    console.print("  - preview_changes     Preview pending artifacts")
    console.print("  - validate_pure_code  Validate Pure syntax")
    console.print()
    console.print("[cyan]Model Modification:[/cyan]")
    console.print("  - read_entity         Read entity from SDLC")
    console.print("  - read_entities       List all entities")
    console.print("  - add_property        Add property to class")
    console.print("  - remove_property     Remove property from class")
    console.print("  - create_class        Create new class")
    console.print("  - create_association  Create new association")
    console.print("  - create_function     Create new function")
    console.print("  - delete_entity       Delete entity")
    console.print("  - update_entity       Update entity content")
    console.print()
    console.print("[bold]To start the server:[/bold]")
    console.print("  legend-cli mcp serve")
