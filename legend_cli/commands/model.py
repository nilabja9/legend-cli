"""Model generation commands - creates complete Legend model from Snowflake database."""

import typer
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from ..snowflake_client import SnowflakeIntrospector, PureCodeGenerator
from ..sdlc_client import SDLCClient
from ..engine_client import EngineClient

app = typer.Typer(help="Generate complete Legend models from database schemas")
console = Console()


def push_pure_code(
    pure_code: str,
    project_id: str,
    workspace_id: str,
    commit_message: str,
    entity_name: str,
) -> bool:
    """Parse Pure code and push to Legend SDLC."""
    with EngineClient() as engine:
        try:
            entities = engine.parse_pure_code(pure_code)
            if not entities:
                console.print(f"[yellow]No entities found in {entity_name}[/yellow]")
                return False
        except Exception as e:
            console.print(f"[red]Error parsing {entity_name}: {e}[/red]")
            return False

    with SDLCClient() as sdlc:
        try:
            result = sdlc.update_entities(
                project_id=project_id,
                workspace_id=workspace_id,
                entities=entities,
                message=commit_message,
            )
            return True
        except Exception as e:
            console.print(f"[red]Error pushing {entity_name}: {e}[/red]")
            return False


@app.command("from-snowflake")
def generate_from_snowflake(
    database: str = typer.Argument(..., help="Snowflake database name to introspect"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Specific schema to introspect (default: all)"),
    project_name: Optional[str] = typer.Option(None, "--project-name", "-n", help="Name for new Legend project"),
    project_id: Optional[str] = typer.Option(None, "--project-id", "-p", help="Existing project ID to use"),
    workspace_id: str = typer.Option("dev-workspace", "--workspace", "-w", help="Workspace ID"),
    account: Optional[str] = typer.Option(None, "--account", "-a", help="Snowflake account (or SNOWFLAKE_ACCOUNT env)"),
    user: Optional[str] = typer.Option(None, "--user", "-u", help="Snowflake user (or SNOWFLAKE_USER env)"),
    password: Optional[str] = typer.Option(None, "--password", help="Snowflake password (or SNOWFLAKE_PASSWORD env)"),
    warehouse: Optional[str] = typer.Option(None, "--warehouse", help="Snowflake warehouse (or SNOWFLAKE_WAREHOUSE env)"),
    role: str = typer.Option("ACCOUNTADMIN", "--role", "-r", help="Snowflake role"),
    region: str = typer.Option("us-east-1", "--region", help="Snowflake region"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Generate code but don't push to SDLC"),
    output_dir: Optional[str] = typer.Option(None, "--output", "-o", help="Save generated Pure files to directory"),
):
    """
    Generate a complete Legend model from a Snowflake database.

    This command will:
    1. Connect to Snowflake and introspect the database schema
    2. Create a new Legend project (or use existing)
    3. Generate and commit: Store, Classes, Connection, Mapping, Runtime

    Example:
        legend-cli model from-snowflake FACTSET_MINUTE_BARS --schema TICK_HISTORY
    """
    import os

    console.print(Panel(
        f"[bold blue]Generating Legend Model from Snowflake[/bold blue]\n"
        f"Database: {database}\n"
        f"Schema: {schema or 'All schemas'}",
        title="Legend Model Generator"
    ))

    # Step 1: Connect to Snowflake and introspect
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Connecting to Snowflake...", total=None)

        try:
            introspector = SnowflakeIntrospector(
                account=account,
                user=user,
                password=password,
                warehouse=warehouse,
                role=role,
            )

            progress.update(task, description="Introspecting database schema...")
            db = introspector.introspect_database(database, schema_filter=schema)
            introspector.close()

        except ImportError as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]Error connecting to Snowflake: {e}[/red]")
            raise typer.Exit(1)

    # Display discovered schema
    console.print(f"\n[green]Discovered Schema:[/green]")
    for schema_obj in db.schemas:
        console.print(f"  Schema: [cyan]{schema_obj.name}[/cyan]")
        for table in schema_obj.tables:
            console.print(f"    - {table.name} ({len(table.columns)} columns)")

    total_tables = sum(len(s.tables) for s in db.schemas)
    if total_tables == 0:
        console.print("[yellow]No tables found in the specified database/schema[/yellow]")
        raise typer.Exit(1)

    console.print(f"\n[bold]Total: {total_tables} tables across {len(db.schemas)} schema(s)[/bold]")

    # Step 2: Generate Pure code
    console.print("\n[blue]Generating Pure code...[/blue]")

    sf_account = account or os.environ.get("SNOWFLAKE_ACCOUNT", "")
    sf_warehouse = warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    sf_user = user or os.environ.get("SNOWFLAKE_USER", "")

    generator = PureCodeGenerator(db)
    artifacts = generator.generate_all(
        account=sf_account,
        warehouse=sf_warehouse,
        role=role,
        region=region,
        username=sf_user,
    )

    # Display generated artifacts
    artifact_table = Table(title="Generated Artifacts")
    artifact_table.add_column("Artifact", style="cyan")
    artifact_table.add_column("Path", style="green")
    artifact_table.add_column("Lines", style="magenta")

    artifact_table.add_row("Store", f"model::store::{database}", str(artifacts['store'].count('\n') + 1))
    artifact_table.add_row("Classes", f"model::domain::*", str(artifacts['classes'].count('\n') + 1))
    artifact_table.add_row("Connection", f"model::connection::{database}Connection", str(artifacts['connection'].count('\n') + 1))
    artifact_table.add_row("Mapping", f"model::mapping::{database}Mapping", str(artifacts['mapping'].count('\n') + 1))
    artifact_table.add_row("Runtime", f"model::runtime::{database}Runtime", str(artifacts['runtime'].count('\n') + 1))

    console.print(artifact_table)

    # Save to files if requested
    if output_dir:
        import os
        os.makedirs(output_dir, exist_ok=True)

        for name, code in artifacts.items():
            file_path = os.path.join(output_dir, f"{name}.pure")
            with open(file_path, "w") as f:
                f.write(code)
            console.print(f"[green]Saved {file_path}[/green]")

    if dry_run:
        console.print("\n[yellow]Dry run - not pushing to SDLC[/yellow]")
        console.print("\n[bold]Generated Store:[/bold]")
        console.print(artifacts['store'][:1000] + "..." if len(artifacts['store']) > 1000 else artifacts['store'])
        return

    # Step 3: Create or use existing project
    with SDLCClient() as sdlc:
        if project_id:
            console.print(f"\n[blue]Using existing project: {project_id}[/blue]")
            actual_project_id = project_id
        else:
            proj_name = project_name or f"{database.lower().replace('_', '-')}-model"
            console.print(f"\n[blue]Creating project: {proj_name}[/blue]")
            try:
                project = sdlc.create_project(
                    name=proj_name,
                    description=f"Legend model for Snowflake database {database}",
                )
                actual_project_id = str(project.get("projectId"))
                console.print(f"[green]Created project with ID: {actual_project_id}[/green]")
            except Exception as e:
                console.print(f"[red]Error creating project: {e}[/red]")
                raise typer.Exit(1)

        # Create workspace if needed
        try:
            sdlc.get_workspace(actual_project_id, workspace_id)
        except Exception:
            console.print(f"[blue]Creating workspace: {workspace_id}[/blue]")
            sdlc.create_workspace(actual_project_id, workspace_id)

    # Step 4: Push artifacts in order
    console.print(f"\n[blue]Pushing artifacts to project {actual_project_id}...[/blue]")

    push_order = [
        ("store", "Store", f"Added {database} store definition"),
        ("classes", "Classes", f"Added domain classes for {database}"),
        ("connection", "Connection", f"Added Snowflake connection for {database}"),
        ("mapping", "Mapping", f"Added mapping for {database}"),
        ("runtime", "Runtime", f"Added runtime for {database}"),
    ]

    success_count = 0
    for artifact_key, artifact_name, commit_msg in push_order:
        console.print(f"  Pushing {artifact_name}...", end=" ")
        if push_pure_code(
            artifacts[artifact_key],
            actual_project_id,
            workspace_id,
            commit_msg,
            artifact_name,
        ):
            console.print("[green]OK[/green]")
            success_count += 1
        else:
            console.print("[red]FAILED[/red]")

    # Summary
    console.print(f"\n[bold green]Model generation complete![/bold green]")
    console.print(f"  Project ID: {actual_project_id}")
    console.print(f"  Workspace: {workspace_id}")
    console.print(f"  Artifacts pushed: {success_count}/{len(push_order)}")
    console.print(f"\n  View in Legend Studio: http://localhost:6900/studio/edit/{actual_project_id}/{workspace_id}")


@app.command("list-databases")
def list_databases(
    account: Optional[str] = typer.Option(None, "--account", "-a", help="Snowflake account"),
    user: Optional[str] = typer.Option(None, "--user", "-u", help="Snowflake user"),
    password: Optional[str] = typer.Option(None, "--password", help="Snowflake password"),
    warehouse: Optional[str] = typer.Option(None, "--warehouse", help="Snowflake warehouse"),
    role: str = typer.Option("ACCOUNTADMIN", "--role", "-r", help="Snowflake role"),
):
    """List available Snowflake databases."""
    try:
        import snowflake.connector
    except ImportError:
        console.print("[red]snowflake-connector-python required. Install with: pip install snowflake-connector-python[/red]")
        raise typer.Exit(1)

    import os

    try:
        conn = snowflake.connector.connect(
            account=account or os.environ.get("SNOWFLAKE_ACCOUNT"),
            user=user or os.environ.get("SNOWFLAKE_USER"),
            password=password or os.environ.get("SNOWFLAKE_PASSWORD"),
            warehouse=warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE"),
            role=role,
        )

        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")

        table = Table(title="Available Snowflake Databases")
        table.add_column("Database", style="cyan")
        table.add_column("Owner", style="green")

        for row in cursor.fetchall():
            table.add_row(row[1], row[5] if len(row) > 5 else "N/A")

        console.print(table)
        cursor.close()
        conn.close()

    except Exception as e:
        console.print(f"[red]Error connecting to Snowflake: {e}[/red]")
        raise typer.Exit(1)


@app.command("list-schemas")
def list_schemas(
    database: str = typer.Argument(..., help="Snowflake database name"),
    account: Optional[str] = typer.Option(None, "--account", "-a", help="Snowflake account"),
    user: Optional[str] = typer.Option(None, "--user", "-u", help="Snowflake user"),
    password: Optional[str] = typer.Option(None, "--password", help="Snowflake password"),
    warehouse: Optional[str] = typer.Option(None, "--warehouse", help="Snowflake warehouse"),
    role: str = typer.Option("ACCOUNTADMIN", "--role", "-r", help="Snowflake role"),
):
    """List schemas in a Snowflake database."""
    try:
        introspector = SnowflakeIntrospector(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role,
        )

        schemas = introspector.get_schemas(database)
        introspector.close()

        table = Table(title=f"Schemas in {database}")
        table.add_column("Schema", style="cyan")

        for schema in schemas:
            table.add_row(schema)

        console.print(table)

    except ImportError:
        console.print("[red]snowflake-connector-python required. Install with: pip install snowflake-connector-python[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
