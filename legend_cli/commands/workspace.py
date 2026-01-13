"""Workspace management commands."""

import typer
from typing import Optional
from rich.console import Console
from rich.table import Table
from ..sdlc_client import SDLCClient
from ..config import settings

app = typer.Typer(help="Workspace management commands")
console = Console()


@app.command("list")
def list_workspaces(
    project_id: str = typer.Argument(
        ...,
        help="Project ID (or use DEFAULT_PROJECT_ID env var)"
    ),
):
    """List workspaces for a project."""
    with SDLCClient() as client:
        try:
            workspaces = client.list_workspaces(project_id)
            if not workspaces:
                console.print("[yellow]No workspaces found.[/yellow]")
                return

            table = Table(title=f"Workspaces for Project {project_id}")
            table.add_column("Workspace ID", style="cyan")
            table.add_column("Project ID", style="green")

            for ws in workspaces:
                table.add_row(
                    ws.get("workspaceId", "N/A"),
                    ws.get("projectId", "N/A"),
                )

            console.print(table)
        except Exception as e:
            console.print(f"[red]Error listing workspaces: {e}[/red]")
            raise typer.Exit(1)


@app.command("info")
def get_workspace(
    project_id: str = typer.Argument(..., help="Project ID"),
    workspace_id: str = typer.Argument(..., help="Workspace ID"),
):
    """Get workspace details."""
    with SDLCClient() as client:
        try:
            workspace = client.get_workspace(project_id, workspace_id)
            console.print(f"[bold]Workspace: {workspace.get('workspaceId')}[/bold]")
            console.print(f"  Project ID: {workspace.get('projectId')}")
        except Exception as e:
            console.print(f"[red]Error getting workspace: {e}[/red]")
            raise typer.Exit(1)


@app.command("create")
def create_workspace(
    project_id: str = typer.Argument(..., help="Project ID"),
    workspace_id: str = typer.Argument(..., help="Workspace ID to create"),
):
    """Create a new workspace."""
    with SDLCClient() as client:
        try:
            workspace = client.create_workspace(project_id, workspace_id)
            console.print(f"[green]Workspace created successfully![/green]")
            console.print(f"  Workspace ID: {workspace.get('workspaceId')}")
            console.print(f"  Project ID: {workspace.get('projectId')}")
        except Exception as e:
            console.print(f"[red]Error creating workspace: {e}[/red]")
            raise typer.Exit(1)


@app.command("entities")
def list_entities(
    project_id: str = typer.Argument(..., help="Project ID"),
    workspace_id: str = typer.Argument(
        "dev-workspace",
        help="Workspace ID"
    ),
):
    """List entities in a workspace."""
    with SDLCClient() as client:
        try:
            entities = client.list_entities(project_id, workspace_id)
            if not entities:
                console.print("[yellow]No entities found.[/yellow]")
                return

            table = Table(title=f"Entities in {project_id}/{workspace_id}")
            table.add_column("Path", style="cyan")
            table.add_column("Type", style="green")

            for entity in entities:
                classifier = entity.get("classifierPath", "")
                entity_type = classifier.split("::")[-1] if classifier else "Unknown"
                table.add_row(
                    entity.get("path", "N/A"),
                    entity_type,
                )

            console.print(table)
        except Exception as e:
            console.print(f"[red]Error listing entities: {e}[/red]")
            raise typer.Exit(1)
