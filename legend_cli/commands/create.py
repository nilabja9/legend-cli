"""Entity creation commands using Claude AI."""

import typer
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from ..claude_client import ClaudeClient
from ..sdlc_client import SDLCClient
from ..engine_client import EngineClient
from ..config import settings

app = typer.Typer(help="Create Legend entities using natural language")
console = Console()


def display_generated_code(code: str, entity_type: str):
    """Display generated Pure code with syntax highlighting."""
    # Remove markdown code blocks if present
    clean_code = code
    if clean_code.startswith("```"):
        lines = clean_code.split("\n")
        # Remove first and last lines (```pure and ```)
        lines = [l for l in lines if not l.startswith("```")]
        clean_code = "\n".join(lines)

    syntax = Syntax(clean_code, "java", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title=f"Generated {entity_type}", border_style="green"))
    return clean_code


def push_to_sdlc(
    pure_code: str,
    project_id: str,
    workspace_id: str,
    commit_message: str,
) -> bool:
    """Parse Pure code and push to Legend SDLC."""
    # Clean up code - remove markdown blocks
    clean_code = pure_code
    if clean_code.startswith("```"):
        lines = clean_code.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        clean_code = "\n".join(lines)

    console.print(f"[blue]Parsing Pure code via Legend Engine...[/blue]")

    with EngineClient() as engine:
        try:
            entities = engine.parse_pure_code(clean_code)
            if not entities:
                console.print("[red]No entities found in generated code[/red]")
                return False

            console.print(f"[green]Parsed {len(entities)} entity(ies)[/green]")
            for entity in entities:
                console.print(f"  - {entity['path']}")
        except Exception as e:
            console.print(f"[red]Error parsing code: {e}[/red]")
            return False

    console.print(f"[blue]Pushing to SDLC project {project_id}, workspace {workspace_id}...[/blue]")

    with SDLCClient() as sdlc:
        try:
            # First ensure workspace exists
            try:
                sdlc.get_workspace(project_id, workspace_id)
            except Exception:
                console.print(f"[yellow]Creating workspace {workspace_id}...[/yellow]")
                sdlc.create_workspace(project_id, workspace_id)

            # Push entities
            result = sdlc.update_entities(
                project_id=project_id,
                workspace_id=workspace_id,
                entities=entities,
                message=commit_message,
            )
            console.print(f"[green]Successfully committed to SDLC![/green]")
            console.print(f"[cyan]Revision: {result.get('revision', 'N/A')}[/cyan]")
            return True
        except Exception as e:
            console.print(f"[red]Error pushing to SDLC: {e}[/red]")
            return False


@app.command("class")
def create_class(
    description: str = typer.Argument(..., help="Natural language description of the class"),
    package: str = typer.Option("model::domain", "--package", "-p", help="Package path"),
    push: bool = typer.Option(False, "--push", help="Push to SDLC and commit"),
    project_id: Optional[str] = typer.Option(None, "--project", help="Project ID"),
    workspace_id: str = typer.Option("dev-workspace", "--workspace", "-w", help="Workspace ID"),
    message: str = typer.Option("Created via legend-cli", "--message", "-m", help="Commit message"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
):
    """Create a Pure class from natural language description."""
    try:
        client = ClaudeClient()
        console.print(f"[blue]Generating class from:[/blue] {description}")

        result = client.generate_class(description, package)
        clean_code = display_generated_code(result.code, "Class")
        console.print(f"[cyan]Path:[/cyan] {result.path}")

        if output:
            with open(output, "w") as f:
                f.write(clean_code)
            console.print(f"[green]Saved to {output}[/green]")

        if push:
            proj_id = project_id or settings.default_project_id
            if not proj_id:
                console.print("[red]Project ID required. Use --project or set DEFAULT_PROJECT_ID[/red]")
                raise typer.Exit(1)
            push_to_sdlc(clean_code, proj_id, workspace_id, message)

    except Exception as e:
        console.print(f"[red]Error generating class: {e}[/red]")
        raise typer.Exit(1)


@app.command("store")
def create_store(
    description: str = typer.Argument(..., help="Natural language description of the store"),
    package: str = typer.Option("model::store", "--package", "-p", help="Package path"),
    push: bool = typer.Option(False, "--push", help="Push to SDLC and commit"),
    project_id: Optional[str] = typer.Option(None, "--project", help="Project ID"),
    workspace_id: str = typer.Option("dev-workspace", "--workspace", "-w", help="Workspace ID"),
    message: str = typer.Option("Created via legend-cli", "--message", "-m", help="Commit message"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
):
    """Create a Pure database store from natural language description."""
    try:
        client = ClaudeClient()
        console.print(f"[blue]Generating store from:[/blue] {description}")

        result = client.generate_store(description, package)
        clean_code = display_generated_code(result.code, "Store")
        console.print(f"[cyan]Path:[/cyan] {result.path}")

        if output:
            with open(output, "w") as f:
                f.write(clean_code)
            console.print(f"[green]Saved to {output}[/green]")

        if push:
            proj_id = project_id or settings.default_project_id
            if not proj_id:
                console.print("[red]Project ID required. Use --project or set DEFAULT_PROJECT_ID[/red]")
                raise typer.Exit(1)
            push_to_sdlc(clean_code, proj_id, workspace_id, message)

    except Exception as e:
        console.print(f"[red]Error generating store: {e}[/red]")
        raise typer.Exit(1)


@app.command("connection")
def create_connection(
    description: str = typer.Argument(..., help="Natural language description of the connection"),
    package: str = typer.Option("model::connection", "--package", "-p", help="Package path"),
    store: Optional[str] = typer.Option(None, "--store", help="Store path to reference"),
    push: bool = typer.Option(False, "--push", help="Push to SDLC and commit"),
    project_id: Optional[str] = typer.Option(None, "--project", help="Project ID"),
    workspace_id: str = typer.Option("dev-workspace", "--workspace", "-w", help="Workspace ID"),
    message: str = typer.Option("Created via legend-cli", "--message", "-m", help="Commit message"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
):
    """Create a Pure connection from natural language description."""
    try:
        client = ClaudeClient()
        console.print(f"[blue]Generating connection from:[/blue] {description}")

        result = client.generate_connection(description, package, store)
        clean_code = display_generated_code(result.code, "Connection")
        console.print(f"[cyan]Path:[/cyan] {result.path}")

        if output:
            with open(output, "w") as f:
                f.write(clean_code)
            console.print(f"[green]Saved to {output}[/green]")

        if push:
            proj_id = project_id or settings.default_project_id
            if not proj_id:
                console.print("[red]Project ID required. Use --project or set DEFAULT_PROJECT_ID[/red]")
                raise typer.Exit(1)
            push_to_sdlc(clean_code, proj_id, workspace_id, message)

    except Exception as e:
        console.print(f"[red]Error generating connection: {e}[/red]")
        raise typer.Exit(1)


@app.command("mapping")
def create_mapping(
    description: str = typer.Argument(..., help="Natural language description of the mapping"),
    package: str = typer.Option("model::mapping", "--package", "-p", help="Package path"),
    store: Optional[str] = typer.Option(None, "--store", help="Store path to reference"),
    classes: Optional[str] = typer.Option(None, "--classes", "-c", help="Comma-separated class paths"),
    push: bool = typer.Option(False, "--push", help="Push to SDLC and commit"),
    project_id: Optional[str] = typer.Option(None, "--project", help="Project ID"),
    workspace_id: str = typer.Option("dev-workspace", "--workspace", "-w", help="Workspace ID"),
    message: str = typer.Option("Created via legend-cli", "--message", "-m", help="Commit message"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
):
    """Create a Pure mapping from natural language description."""
    try:
        client = ClaudeClient()
        console.print(f"[blue]Generating mapping from:[/blue] {description}")

        class_paths = classes.split(",") if classes else None
        result = client.generate_mapping(description, package, store, class_paths)
        clean_code = display_generated_code(result.code, "Mapping")
        console.print(f"[cyan]Path:[/cyan] {result.path}")

        if output:
            with open(output, "w") as f:
                f.write(clean_code)
            console.print(f"[green]Saved to {output}[/green]")

        if push:
            proj_id = project_id or settings.default_project_id
            if not proj_id:
                console.print("[red]Project ID required. Use --project or set DEFAULT_PROJECT_ID[/red]")
                raise typer.Exit(1)
            push_to_sdlc(clean_code, proj_id, workspace_id, message)

    except Exception as e:
        console.print(f"[red]Error generating mapping: {e}[/red]")
        raise typer.Exit(1)


@app.command("from-file")
def create_from_file(
    file_path: str = typer.Argument(..., help="Path to Pure code file"),
    project_id: str = typer.Option(..., "--project", "-p", help="Project ID"),
    workspace_id: str = typer.Option("dev-workspace", "--workspace", "-w", help="Workspace ID"),
    message: str = typer.Option("Created via legend-cli", "--message", "-m", help="Commit message"),
):
    """Push Pure code from a file to SDLC."""
    try:
        with open(file_path, "r") as f:
            pure_code = f.read()

        console.print(f"[blue]Reading Pure code from {file_path}[/blue]")
        push_to_sdlc(pure_code, project_id, workspace_id, message)

    except FileNotFoundError:
        console.print(f"[red]File not found: {file_path}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
