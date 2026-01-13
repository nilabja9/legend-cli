"""Project management commands."""

import typer
from typing import Optional
from rich.console import Console
from rich.table import Table
from ..sdlc_client import SDLCClient
from ..config import settings

app = typer.Typer(help="Project management commands")
console = Console()


@app.command("list")
def list_projects():
    """List all projects."""
    with SDLCClient() as client:
        try:
            projects = client.list_projects()
            if not projects:
                console.print("[yellow]No projects found.[/yellow]")
                return

            table = Table(title="Projects")
            table.add_column("ID", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Group ID", style="blue")
            table.add_column("Artifact ID", style="magenta")

            for project in projects:
                table.add_row(
                    str(project.get("projectId", "N/A")),
                    project.get("name", "N/A"),
                    project.get("groupId", "N/A"),
                    project.get("artifactId", "N/A"),
                )

            console.print(table)
        except Exception as e:
            console.print(f"[red]Error listing projects: {e}[/red]")
            raise typer.Exit(1)


@app.command("info")
def get_project(project_id: str = typer.Argument(..., help="Project ID")):
    """Get project details."""
    with SDLCClient() as client:
        try:
            project = client.get_project(project_id)
            console.print(f"[bold]Project: {project.get('name')}[/bold]")
            console.print(f"  ID: {project.get('projectId')}")
            console.print(f"  Group ID: {project.get('groupId')}")
            console.print(f"  Artifact ID: {project.get('artifactId')}")
            console.print(f"  Description: {project.get('description', 'N/A')}")
            console.print(f"  Tags: {', '.join(project.get('tags', []))}")
        except Exception as e:
            console.print(f"[red]Error getting project: {e}[/red]")
            raise typer.Exit(1)


@app.command("create")
def create_project(
    name: str = typer.Argument(..., help="Project name"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Project description"),
    group_id: str = typer.Option("org.demo.legend", "--group-id", "-g", help="Maven group ID"),
    artifact_id: Optional[str] = typer.Option(None, "--artifact-id", "-a", help="Maven artifact ID"),
):
    """Create a new project."""
    with SDLCClient() as client:
        try:
            project = client.create_project(
                name=name,
                description=description,
                group_id=group_id,
                artifact_id=artifact_id,
            )
            console.print(f"[green]Project created successfully![/green]")
            console.print(f"  ID: {project.get('projectId')}")
            console.print(f"  Name: {project.get('name')}")
        except Exception as e:
            console.print(f"[red]Error creating project: {e}[/red]")
            raise typer.Exit(1)
