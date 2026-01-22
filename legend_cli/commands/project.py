"""Project management commands."""

import typer
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm
from ..sdlc_client import SDLCClient
from ..config import settings

app = typer.Typer(help="Project management commands")
console = Console()

# Protected project that should never be deleted with --all
PROTECTED_PROJECT_NAME = "Guided Tour"


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


@app.command("delete")
def delete_project(
    name: Optional[str] = typer.Argument(None, help="Project name to delete"),
    all_projects: bool = typer.Option(False, "--all", "-a", help="Delete all projects except 'Guided Tour'"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
):
    """Delete a project by name, or all projects except 'Guided Tour' with --all flag."""
    with SDLCClient() as client:
        try:
            projects = client.list_projects()

            if all_projects:
                # Delete all projects except Guided Tour
                projects_to_delete = [
                    p for p in projects
                    if p.get("name") != PROTECTED_PROJECT_NAME
                ]

                if not projects_to_delete:
                    console.print(f"[yellow]No projects to delete (only '{PROTECTED_PROJECT_NAME}' exists).[/yellow]")
                    return

                # Show what will be deleted
                console.print(f"[bold]Projects to delete:[/bold]")
                for p in projects_to_delete:
                    console.print(f"  - {p.get('name')} (ID: {p.get('projectId')})")
                console.print(f"\n[cyan]'{PROTECTED_PROJECT_NAME}' will be preserved.[/cyan]")

                if not force:
                    if not Confirm.ask(f"\nDelete {len(projects_to_delete)} project(s)?"):
                        console.print("[yellow]Aborted.[/yellow]")
                        raise typer.Exit(0)

                # Delete each project
                deleted_count = 0
                for p in projects_to_delete:
                    try:
                        client.delete_project(p.get("projectId"))
                        console.print(f"[green]Deleted:[/green] {p.get('name')}")
                        deleted_count += 1
                    except Exception as e:
                        console.print(f"[red]Failed to delete {p.get('name')}: {e}[/red]")

                console.print(f"\n[green]Deleted {deleted_count} project(s).[/green]")

            else:
                # Delete by name
                if not name:
                    console.print("[red]Error: Project name is required (or use --all flag)[/red]")
                    raise typer.Exit(1)

                # Prevent deleting protected project
                if name == PROTECTED_PROJECT_NAME:
                    console.print(f"[red]Error: Cannot delete protected project '{PROTECTED_PROJECT_NAME}'[/red]")
                    raise typer.Exit(1)

                # Find project by name
                matching = [p for p in projects if p.get("name") == name]

                if not matching:
                    console.print(f"[red]Error: Project '{name}' not found[/red]")
                    raise typer.Exit(1)

                if len(matching) > 1:
                    console.print(f"[yellow]Multiple projects found with name '{name}':[/yellow]")
                    for p in matching:
                        console.print(f"  - ID: {p.get('projectId')}")
                    console.print("[yellow]All matching projects will be deleted.[/yellow]")

                if not force:
                    if not Confirm.ask(f"Delete project '{name}' ({len(matching)} instance(s))?"):
                        console.print("[yellow]Aborted.[/yellow]")
                        raise typer.Exit(0)

                for p in matching:
                    client.delete_project(p.get("projectId"))
                    console.print(f"[green]Deleted:[/green] {p.get('name')} (ID: {p.get('projectId')})")

        except typer.Exit:
            raise
        except Exception as e:
            console.print(f"[red]Error deleting project: {e}[/red]")
            raise typer.Exit(1)
