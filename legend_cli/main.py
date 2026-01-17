"""Legend CLI - Main entry point."""

import typer
from rich.console import Console
from .commands import project, workspace, create, model, mcp
from .config import settings

app = typer.Typer(
    name="legend-cli",
    help="Create Legend artifacts using natural language prompts",
    add_completion=False,
)

# Add subcommands
app.add_typer(project.app, name="project")
app.add_typer(workspace.app, name="workspace")
app.add_typer(create.app, name="create")
app.add_typer(model.app, name="model")
app.add_typer(mcp.app, name="mcp")

console = Console()


@app.command()
def config():
    """Show current configuration."""
    console.print("[bold]Current Configuration[/bold]")
    console.print(f"  SDLC URL: {settings.legend_sdlc_url}")
    console.print(f"  PAT configured: {'Yes' if settings.legend_pat else 'No'}")
    console.print(f"  Default Project: {settings.default_project_id or 'Not set'}")
    console.print(f"  Default Workspace: {settings.default_workspace_id}")
    console.print(f"  Anthropic API Key: {'Configured' if settings.anthropic_api_key else 'Not set'}")
    console.print(f"  Claude Model: {settings.claude_model}")


@app.command()
def health():
    """Check connection to Legend SDLC."""
    from .sdlc_client import SDLCClient

    with SDLCClient() as client:
        if client.health_check():
            console.print(f"[green]Connected to Legend SDLC at {settings.legend_sdlc_url}[/green]")
        else:
            console.print(f"[red]Cannot connect to Legend SDLC at {settings.legend_sdlc_url}[/red]")
            raise typer.Exit(1)


@app.callback()
def main():
    """
    Legend CLI - Create Legend artifacts using natural language prompts.

    Use 'legend-cli create' commands to generate Pure code with AI assistance.

    Examples:

        legend-cli create class "A Person with name, age, and email"

        legend-cli create store "Snowflake DB with USERS table having id, name columns"

        legend-cli project list

        legend-cli workspace entities 2
    """
    pass


if __name__ == "__main__":
    app()
