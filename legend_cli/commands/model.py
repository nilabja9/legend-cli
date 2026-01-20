"""Model generation commands - creates complete Legend model from database schemas."""

import asyncio
import os
import typer
from typing import Optional, List
from typing_extensions import Annotated
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

# Use new modular structure
from ..database import SnowflakeIntrospector, DuckDBIntrospector
from ..pure import PureCodeGenerator, SnowflakeConnectionGenerator, DuckDBConnectionGenerator
from ..pure.enhanced_generator import EnhancedPureCodeGenerator
from ..sdlc_client import SDLCClient
from ..engine_client import EngineClient
from ..doc_generator import DocGenerator
from ..analysis import (
    SchemaAnalyzer,
    AnalysisContext,
    AnalysisOptions,
    analyze_schema,
)
from ..parsers.sql_parser import parse_sql_files

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


def push_all_artifacts(
    artifacts: dict,
    project_id: str,
    workspace_id: str,
    commit_message: str,
    db_name: str,
) -> bool:
    """Push all artifacts in a single atomic transaction.

    This function combines all artifacts into a single SDLC push request,
    ensuring proper ordering and avoiding dependency resolution issues
    that occur when pushing artifacts separately.

    The order of entities in the payload follows dependency order:
    1. Enumerations (no dependencies)
    2. Classes (may depend on enums)
    3. Associations (depend on classes)
    4. Store (no dependencies)
    5. Connection (depends on store)
    6. Mapping (depends on classes, store)
    7. Runtime (depends on mapping, connection)

    Args:
        artifacts: Dict of artifact name -> Pure code
        project_id: Legend SDLC project ID
        workspace_id: Legend SDLC workspace ID
        commit_message: Commit message for the push
        db_name: Database name (for logging)

    Returns:
        True if push succeeded, False otherwise
    """
    # Define the order to ensure dependencies are resolved correctly
    push_order = [
        "enumerations",
        "classes",
        "associations",
        "store",
        "connection",
        "mapping",
        "runtime",
    ]

    # Build ordered artifacts dict
    ordered_artifacts = {}
    for key in push_order:
        if key in artifacts and artifacts[key] and artifacts[key].strip():
            ordered_artifacts[key] = artifacts[key]

    if not ordered_artifacts:
        console.print("[yellow]No artifacts to push[/yellow]")
        return False

    # Parse all artifacts
    console.print(f"  Parsing {len(ordered_artifacts)} artifacts...")
    with EngineClient() as engine:
        try:
            all_entities = engine.parse_multiple_pure_codes(ordered_artifacts)
            if not all_entities:
                console.print("[yellow]No entities found in artifacts[/yellow]")
                return False
            console.print(f"  [green]Parsed {len(all_entities)} entities[/green]")
        except Exception as e:
            console.print(f"[red]Error parsing artifacts: {e}[/red]")
            return False

    # Push all entities in a single transaction
    console.print(f"  Pushing {len(all_entities)} entities in single transaction...")
    with SDLCClient() as sdlc:
        try:
            result = sdlc.update_entities(
                project_id=project_id,
                workspace_id=workspace_id,
                entities=all_entities,
                message=commit_message,
            )
            console.print(f"  [green]Successfully pushed all entities[/green]")
            return True
        except Exception as e:
            console.print(f"[red]Error pushing entities: {e}[/red]")
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
    region: Optional[str] = typer.Option(None, "--region", help="Snowflake region (omit for standard public endpoint)"),
    # Authentication options for Legend connection
    auth_type: str = typer.Option("keypair", "--auth-type", help="Auth type: 'keypair' (SnowflakePublic) or 'password' (MiddleTierUserNamePassword)"),
    legend_user: Optional[str] = typer.Option(None, "--legend-user", help="Snowflake username for Legend connection (defaults to --user)"),
    private_key_vault_ref: str = typer.Option("SNOWFLAKE_PRIVATE_KEY", "--private-key-ref", help="Vault reference for private key"),
    passphrase_vault_ref: str = typer.Option("SNOWFLAKE_PASSPHRASE", "--passphrase-ref", help="Vault reference for passphrase"),
    password_vault_ref: str = typer.Option("SNOWFLAKE_PASSWORD", "--password-ref", help="Vault reference for password (if using password auth)"),
    # AWS Secrets Manager option
    aws_secret: Optional[str] = typer.Option(None, "--aws-secret", help="AWS Secrets Manager secret name (e.g., 'legend/snowflake/credentials'). Vault refs will be formatted as 'secretName:key'"),
    # Documentation generation options
    doc_source: Annotated[Optional[List[str]], typer.Option(
        "--doc-source", "-d",
        help="Documentation source (URL, PDF path, or JSON path). Can be specified multiple times."
    )] = None,
    auto_docs: bool = typer.Option(False, "--auto-docs", help="Auto-generate documentation from class/attribute names using AI"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Generate code but don't push to SDLC"),
    output_dir: Optional[str] = typer.Option(None, "--output", "-o", help="Save generated Pure files to directory"),
    # Enhanced model generation options
    enhanced: bool = typer.Option(False, "--enhanced", "-e", help="Enable LLM-powered advanced model generation (inheritance, enums, constraints, derived properties)"),
    sql_source: Annotated[Optional[List[str]], typer.Option(
        "--sql-source",
        help="SQL files or directories for pattern analysis. Can be specified multiple times."
    )] = None,
    analyze_only: bool = typer.Option(False, "--analyze-only", help="Show analysis suggestions without generating code"),
    confidence: float = typer.Option(0.7, "--confidence", help="Minimum confidence threshold for suggestions (0.0-1.0)"),
    hierarchies: bool = typer.Option(False, "--hierarchies/--no-hierarchies", help="Detect class inheritance hierarchies (disabled by default)"),
    enums: bool = typer.Option(True, "--enums/--no-enums", help="Detect enumeration candidates"),
    constraints: bool = typer.Option(False, "--constraints/--no-constraints", help="Generate data constraints (disabled by default)"),
    derived: bool = typer.Option(False, "--derived/--no-derived", help="Detect derived properties (disabled by default)"),
):
    """
    Generate a complete Legend model from a Snowflake database.

    This command will:
    1. Connect to Snowflake and introspect the database schema
    2. Create a new Legend project (or use existing)
    3. Generate and commit: Store, Classes, Connection, Mapping, Runtime

    With --enhanced flag, also:
    - Detect class inheritance hierarchies
    - Generate enumerations from reference tables and low-cardinality columns
    - Add data quality constraints
    - Create derived (computed) properties

    Examples:
        legend-cli model from-snowflake FACTSET_MINUTE_BARS --schema TICK_HISTORY
        legend-cli model from-snowflake DB --enhanced --analyze-only
        legend-cli model from-snowflake DB --enhanced --sql-source ./queries/ --confidence 0.8
    """
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

    # Display detected relationships
    if db.relationships:
        console.print(f"\n[green]Detected Relationships:[/green]")
        rel_table = Table(title="Detected Relationships")
        rel_table.add_column("Source", style="cyan")
        rel_table.add_column("Target", style="green")
        rel_table.add_column("Type", style="yellow")
        rel_table.add_column("Property", style="magenta")

        for rel in db.relationships:
            rel_table.add_row(
                f"{rel.source_table}.{rel.source_column}",
                f"{rel.target_table}.{rel.target_column}",
                rel.relationship_type,
                rel.property_name
            )
        console.print(rel_table)
        console.print(f"[bold]Total: {len(db.relationships)} relationships detected[/bold]")
    else:
        console.print(f"\n[yellow]No relationships detected (no matching patterns found)[/yellow]")

    # Step 2: Generate documentation (if requested)
    docs = None
    if doc_source or auto_docs:
        console.print("\n[blue]Generating documentation...[/blue]")

        # Collect all tables for documentation
        all_tables = []
        for schema_obj in db.schemas:
            all_tables.extend(schema_obj.tables)

        try:
            doc_gen = DocGenerator()

            if doc_source:
                # Parse documentation sources
                console.print(f"  Parsing {len(doc_source)} documentation source(s)...")
                doc_sources = asyncio.run(doc_gen.parse_sources(doc_source))
                for src in doc_sources:
                    console.print(f"    - {src.source_type}: {src.source_path}")

                # Generate docs with matching + fallback for unmatched
                console.print("  Generating documentation from sources...")
                docs = doc_gen.generate_class_docs(all_tables, doc_sources, generate_fallback=True)
            else:
                # Auto-docs: generate purely from names
                console.print("  Generating documentation from class/attribute names...")
                docs = doc_gen.generate_docs_from_names_only(all_tables)

            # Display summary
            matched_count = sum(1 for d in docs.values() if d.source == "matched")
            inferred_count = len(docs) - matched_count
            console.print(f"  [green]Documentation generated for {len(docs)} classes[/green]")
            if doc_source:
                console.print(f"    - Matched from sources: {matched_count}")
                console.print(f"    - Inferred from names: {inferred_count}")

        except Exception as e:
            console.print(f"[yellow]Warning: Documentation generation failed: {e}[/yellow]")
            console.print("[yellow]Continuing without documentation...[/yellow]")
            docs = None

    # Step 3: Enhanced analysis (if requested)
    enhanced_spec = None
    if enhanced:
        console.print("\n[blue]Running enhanced schema analysis...[/blue]")

        # Parse SQL sources if provided
        sql_queries = None
        if sql_source:
            console.print(f"  Parsing {len(sql_source)} SQL source(s)...")
            sql_queries = parse_sql_files(sql_source)
            console.print(f"    Found {len(sql_queries)} SQL queries")

        # Combine documentation content
        doc_content = None
        if docs:
            doc_parts = []
            for class_name, class_doc in docs.items():
                doc_parts.append(f"{class_name}: {getattr(class_doc, 'class_doc', '')}")
            doc_content = "\n".join(doc_parts)

        # Configure analysis options
        analysis_options = AnalysisOptions(
            detect_hierarchies=hierarchies,
            detect_enums=enums,
            detect_constraints=constraints,
            detect_derived=derived,
            use_llm=True,
            confidence_threshold=confidence,
        )

        # Run analysis
        try:
            analyzer = SchemaAnalyzer(options=analysis_options)
            context = AnalysisContext(
                database=db,
                documentation=doc_content,
                sql_queries=sql_queries,
            )
            enhanced_spec = analyzer.analyze(context)

            # Display analysis results
            console.print(f"\n[green]Enhanced Analysis Results:[/green]")
            console.print(f"  Hierarchies detected: {len(enhanced_spec.hierarchies)}")
            for h in enhanced_spec.hierarchies[:5]:
                console.print(f"    - {h.base_class_name} <- {', '.join(h.derived_classes[:3])} (confidence: {h.confidence:.2f})")

            console.print(f"  Enumerations detected: {len(enhanced_spec.enumerations)}")
            for e in enhanced_spec.enumerations[:5]:
                console.print(f"    - {e.name}: {len(e.values)} values (confidence: {e.confidence:.2f})")

            console.print(f"  Constraints suggested: {len(enhanced_spec.constraints)}")
            for c in enhanced_spec.constraints[:5]:
                console.print(f"    - {c.class_name}.{c.constraint_name} (confidence: {c.confidence:.2f})")

            console.print(f"  Derived properties suggested: {len(enhanced_spec.derived_properties)}")
            for d in enhanced_spec.derived_properties[:5]:
                console.print(f"    - {d.class_name}.{d.property_name}: {d.return_type} (confidence: {d.confidence:.2f})")

            if analyze_only:
                console.print("\n[yellow]Analysis only mode - not generating code[/yellow]")
                console.print("\n" + enhanced_spec.summary())
                return

        except Exception as e:
            console.print(f"[yellow]Warning: Enhanced analysis failed: {e}[/yellow]")
            console.print("[yellow]Continuing with basic generation...[/yellow]")
            enhanced_spec = None

    # Step 4: Generate Pure code
    console.print("\n[blue]Generating Pure code...[/blue]")

    sf_account = account or os.environ.get("SNOWFLAKE_ACCOUNT", "")
    sf_warehouse = warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    sf_user = user or os.environ.get("SNOWFLAKE_USER", "")
    connection_user = legend_user or sf_user  # User for Legend connection

    # If AWS secret is provided, format vault references as secretName:key
    actual_private_key_ref = private_key_vault_ref
    actual_passphrase_ref = passphrase_vault_ref
    actual_password_ref = password_vault_ref

    if aws_secret:
        console.print(f"[cyan]Using AWS Secrets Manager: {aws_secret}[/cyan]")
        actual_private_key_ref = f"{aws_secret}:private_key"
        actual_passphrase_ref = f"{aws_secret}:passphrase"
        # For MiddleTierUserNamePassword, use the secret name directly (not :password)
        # Legend will extract both username and password from the secret
        actual_password_ref = aws_secret

    # Generate connection using connection generator
    conn_generator = SnowflakeConnectionGenerator()
    connection_code = conn_generator.generate(
        database_name=database,
        store_path=f"model::store::{database}",
        package_prefix="model",
        account=sf_account,
        warehouse=sf_warehouse,
        role=role,
        region=region,
        auth_type=auth_type,
        username=connection_user,
        private_key_vault_ref=actual_private_key_ref,
        passphrase_vault_ref=actual_passphrase_ref,
        password_vault_ref=actual_password_ref,
    )

    # Use enhanced generator if we have enhanced spec
    if enhanced_spec:
        generator = EnhancedPureCodeGenerator(db, enhanced_spec=enhanced_spec)
        artifacts = generator.generate_all_enhanced(
            connection_code=connection_code,
            docs=docs,
        )
    else:
        generator = PureCodeGenerator(db)
        artifacts = generator.generate_all(
            connection_code=connection_code,
            docs=docs,
        )

    # Display generated artifacts
    artifact_table = Table(title="Generated Artifacts")
    artifact_table.add_column("Artifact", style="cyan")
    artifact_table.add_column("Path", style="green")
    artifact_table.add_column("Lines", style="magenta")

    # Add enumerations if present (enhanced mode)
    if 'enumerations' in artifacts:
        artifact_table.add_row("Enumerations", f"model::domain::*", str(artifacts['enumerations'].count('\n') + 1))

    artifact_table.add_row("Store", f"model::store::{database}", str(artifacts['store'].count('\n') + 1))
    artifact_table.add_row("Classes", f"model::domain::*", str(artifacts['classes'].count('\n') + 1))
    if 'associations' in artifacts:
        artifact_table.add_row("Associations", f"model::domain::*", str(artifacts['associations'].count('\n') + 1))
    artifact_table.add_row("Connection", f"model::connection::{database}Connection", str(artifacts['connection'].count('\n') + 1))
    artifact_table.add_row("Mapping", f"model::mapping::{database}Mapping", str(artifacts['mapping'].count('\n') + 1))
    artifact_table.add_row("Runtime", f"model::runtime::{database}Runtime", str(artifacts['runtime'].count('\n') + 1))

    console.print(artifact_table)

    # Save to files if requested
    if output_dir:
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

    # Step 4: Push all artifacts in a single atomic transaction
    console.print(f"\n[blue]Pushing artifacts to project {actual_project_id}...[/blue]")

    push_success = push_all_artifacts(
        artifacts=artifacts,
        project_id=actual_project_id,
        workspace_id=workspace_id,
        commit_message=f"Added complete Legend model for {database}",
        db_name=database,
    )

    # Summary
    if push_success:
        console.print(f"\n[bold green]Model generation complete![/bold green]")
        console.print(f"  Project ID: {actual_project_id}")
        console.print(f"  Workspace: {workspace_id}")
        console.print(f"  All artifacts pushed successfully")
        console.print(f"\n  View in Legend Studio: http://localhost:6900/studio/edit/{actual_project_id}/{workspace_id}")
    else:
        console.print(f"\n[bold red]Model generation failed![/bold red]")
        console.print(f"  Project ID: {actual_project_id}")
        console.print(f"  Workspace: {workspace_id}")
        console.print(f"  Check the error messages above for details")
        raise typer.Exit(1)


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


@app.command("from-duckdb")
def generate_from_duckdb(
    database_path: str = typer.Argument(..., help="Path to DuckDB database file (or :memory: for in-memory)"),
    database_name: Optional[str] = typer.Option(None, "--name", "-n", help="Name for the Legend model (defaults to filename)"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Specific schema to introspect (default: main)"),
    project_name: Optional[str] = typer.Option(None, "--project-name", help="Name for new Legend project"),
    project_id: Optional[str] = typer.Option(None, "--project-id", "-p", help="Existing project ID to use"),
    workspace_id: str = typer.Option("dev-workspace", "--workspace", "-w", help="Workspace ID"),
    connection_string: Optional[str] = typer.Option(None, "--connection-string", help="DuckDB connection string (alternative to path)"),
    # Documentation generation options
    doc_source: Annotated[Optional[List[str]], typer.Option(
        "--doc-source", "-d",
        help="Documentation source (URL, PDF path, or JSON path). Can be specified multiple times."
    )] = None,
    auto_docs: bool = typer.Option(False, "--auto-docs", help="Auto-generate documentation from class/attribute names using AI"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Generate code but don't push to SDLC"),
    output_dir: Optional[str] = typer.Option(None, "--output", "-o", help="Save generated Pure files to directory"),
    # Enhanced model generation options
    enhanced: bool = typer.Option(False, "--enhanced", "-e", help="Enable LLM-powered advanced model generation (inheritance, enums, constraints, derived properties)"),
    sql_source: Annotated[Optional[List[str]], typer.Option(
        "--sql-source",
        help="SQL files or directories for pattern analysis. Can be specified multiple times."
    )] = None,
    analyze_only: bool = typer.Option(False, "--analyze-only", help="Show analysis suggestions without generating code"),
    confidence: float = typer.Option(0.7, "--confidence", help="Minimum confidence threshold for suggestions (0.0-1.0)"),
    hierarchies: bool = typer.Option(False, "--hierarchies/--no-hierarchies", help="Detect class inheritance hierarchies (disabled by default)"),
    enums: bool = typer.Option(True, "--enums/--no-enums", help="Detect enumeration candidates"),
    constraints: bool = typer.Option(False, "--constraints/--no-constraints", help="Generate data constraints (disabled by default)"),
    derived: bool = typer.Option(False, "--derived/--no-derived", help="Detect derived properties (disabled by default)"),
):
    """
    Generate a complete Legend model from a DuckDB database.

    This command will:
    1. Connect to DuckDB and introspect the database schema
    2. Create a new Legend project (or use existing)
    3. Generate and commit: Store, Classes, Associations, Connection, Mapping, Runtime

    With --enhanced flag, also:
    - Detect class inheritance hierarchies
    - Generate enumerations from reference tables and low-cardinality columns
    - Add data quality constraints
    - Create derived (computed) properties

    Examples:
        legend-cli model from-duckdb ./my_database.duckdb
        legend-cli model from-duckdb ./analytics.duckdb --schema main --name analytics_model
        legend-cli model from-duckdb :memory: --name test_db --dry-run
        legend-cli model from-duckdb ./db.duckdb --enhanced --analyze-only
    """
    # Determine the actual path and database name
    actual_path = database_path
    if connection_string and not database_path:
        actual_path = None

    console.print(Panel(
        f"[bold blue]Generating Legend Model from DuckDB[/bold blue]\n"
        f"Database: {database_path}\n"
        f"Schema: {schema or 'All schemas'}",
        title="Legend Model Generator"
    ))

    # Step 1: Connect to DuckDB and introspect
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Connecting to DuckDB...", total=None)

        try:
            introspector = DuckDBIntrospector(
                database_path=actual_path,
                connection_string=connection_string,
                read_only=True,
            )

            progress.update(task, description="Introspecting database schema...")
            db_name_to_use = database_name or introspector.get_database_name()
            db = introspector.introspect_database(database=db_name_to_use, schema_filter=schema)
            introspector.close()

        except ImportError as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]Error connecting to DuckDB: {e}[/red]")
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

    # Display detected relationships
    if db.relationships:
        console.print(f"\n[green]Detected Relationships:[/green]")
        rel_table = Table(title="Detected Relationships")
        rel_table.add_column("Source", style="cyan")
        rel_table.add_column("Target", style="green")
        rel_table.add_column("Type", style="yellow")
        rel_table.add_column("Property", style="magenta")

        for rel in db.relationships:
            rel_table.add_row(
                f"{rel.source_table}.{rel.source_column}",
                f"{rel.target_table}.{rel.target_column}",
                rel.relationship_type,
                rel.property_name
            )
        console.print(rel_table)
        console.print(f"[bold]Total: {len(db.relationships)} relationships detected[/bold]")
    else:
        console.print(f"\n[yellow]No relationships detected (no matching patterns found)[/yellow]")

    # Step 2: Generate documentation (if requested)
    docs = None
    if doc_source or auto_docs:
        console.print("\n[blue]Generating documentation...[/blue]")

        # Collect all tables for documentation
        all_tables = []
        for schema_obj in db.schemas:
            all_tables.extend(schema_obj.tables)

        try:
            doc_gen = DocGenerator()

            if doc_source:
                # Parse documentation sources
                console.print(f"  Parsing {len(doc_source)} documentation source(s)...")
                doc_sources = asyncio.run(doc_gen.parse_sources(doc_source))
                for src in doc_sources:
                    console.print(f"    - {src.source_type}: {src.source_path}")

                # Generate docs with matching + fallback for unmatched
                console.print("  Generating documentation from sources...")
                docs = doc_gen.generate_class_docs(all_tables, doc_sources, generate_fallback=True)
            else:
                # Auto-docs: generate purely from names
                console.print("  Generating documentation from class/attribute names...")
                docs = doc_gen.generate_docs_from_names_only(all_tables)

            # Display summary
            matched_count = sum(1 for d in docs.values() if d.source == "matched")
            inferred_count = len(docs) - matched_count
            console.print(f"  [green]Documentation generated for {len(docs)} classes[/green]")
            if doc_source:
                console.print(f"    - Matched from sources: {matched_count}")
                console.print(f"    - Inferred from names: {inferred_count}")

        except Exception as e:
            console.print(f"[yellow]Warning: Documentation generation failed: {e}[/yellow]")
            console.print("[yellow]Continuing without documentation...[/yellow]")
            docs = None

    # Step 3: Enhanced analysis (if requested)
    enhanced_spec = None
    if enhanced:
        console.print("\n[blue]Running enhanced schema analysis...[/blue]")

        # Parse SQL sources if provided
        sql_queries = None
        if sql_source:
            console.print(f"  Parsing {len(sql_source)} SQL source(s)...")
            sql_queries = parse_sql_files(sql_source)
            console.print(f"    Found {len(sql_queries)} SQL queries")

        # Combine documentation content
        doc_content = None
        if docs:
            doc_parts = []
            for class_name, class_doc in docs.items():
                doc_parts.append(f"{class_name}: {getattr(class_doc, 'class_doc', '')}")
            doc_content = "\n".join(doc_parts)

        # Configure analysis options
        analysis_options = AnalysisOptions(
            detect_hierarchies=hierarchies,
            detect_enums=enums,
            detect_constraints=constraints,
            detect_derived=derived,
            use_llm=True,
            confidence_threshold=confidence,
        )

        # Run analysis
        try:
            analyzer = SchemaAnalyzer(options=analysis_options)
            context = AnalysisContext(
                database=db,
                documentation=doc_content,
                sql_queries=sql_queries,
            )
            enhanced_spec = analyzer.analyze(context)

            # Display analysis results
            console.print(f"\n[green]Enhanced Analysis Results:[/green]")
            console.print(f"  Hierarchies detected: {len(enhanced_spec.hierarchies)}")
            for h in enhanced_spec.hierarchies[:5]:
                console.print(f"    - {h.base_class_name} <- {', '.join(h.derived_classes[:3])} (confidence: {h.confidence:.2f})")

            console.print(f"  Enumerations detected: {len(enhanced_spec.enumerations)}")
            for e in enhanced_spec.enumerations[:5]:
                console.print(f"    - {e.name}: {len(e.values)} values (confidence: {e.confidence:.2f})")

            console.print(f"  Constraints suggested: {len(enhanced_spec.constraints)}")
            for c in enhanced_spec.constraints[:5]:
                console.print(f"    - {c.class_name}.{c.constraint_name} (confidence: {c.confidence:.2f})")

            console.print(f"  Derived properties suggested: {len(enhanced_spec.derived_properties)}")
            for d in enhanced_spec.derived_properties[:5]:
                console.print(f"    - {d.class_name}.{d.property_name}: {d.return_type} (confidence: {d.confidence:.2f})")

            if analyze_only:
                console.print("\n[yellow]Analysis only mode - not generating code[/yellow]")
                console.print("\n" + enhanced_spec.summary())
                return

        except Exception as e:
            console.print(f"[yellow]Warning: Enhanced analysis failed: {e}[/yellow]")
            console.print("[yellow]Continuing with basic generation...[/yellow]")
            enhanced_spec = None

    # Step 4: Generate Pure code
    console.print("\n[blue]Generating Pure code...[/blue]")

    # Generate connection using DuckDB connection generator (LocalH2)
    conn_generator = DuckDBConnectionGenerator()
    connection_code = conn_generator.generate(
        database_name=db.name,
        store_path=f"model::store::{db.name}",
        package_prefix="model",
        database_path=database_path,
    )

    # Use enhanced generator if we have enhanced spec
    if enhanced_spec:
        generator = EnhancedPureCodeGenerator(db, enhanced_spec=enhanced_spec)
        artifacts = generator.generate_all_enhanced(
            connection_code=connection_code,
            docs=docs,
        )
    else:
        generator = PureCodeGenerator(db)
        artifacts = generator.generate_all(
            connection_code=connection_code,
            docs=docs,
        )

    # Display generated artifacts
    artifact_table = Table(title="Generated Artifacts")
    artifact_table.add_column("Artifact", style="cyan")
    artifact_table.add_column("Path", style="green")
    artifact_table.add_column("Lines", style="magenta")

    # Add enumerations if present (enhanced mode)
    if 'enumerations' in artifacts:
        artifact_table.add_row("Enumerations", f"model::domain::*", str(artifacts['enumerations'].count('\n') + 1))

    artifact_table.add_row("Store", f"model::store::{db.name}", str(artifacts['store'].count('\n') + 1))
    artifact_table.add_row("Classes", f"model::domain::*", str(artifacts['classes'].count('\n') + 1))
    if 'associations' in artifacts:
        artifact_table.add_row("Associations", f"model::domain::*", str(artifacts['associations'].count('\n') + 1))
    artifact_table.add_row("Connection", f"model::connection::{db.name}Connection", str(artifacts['connection'].count('\n') + 1))
    artifact_table.add_row("Mapping", f"model::mapping::{db.name}Mapping", str(artifacts['mapping'].count('\n') + 1))
    artifact_table.add_row("Runtime", f"model::runtime::{db.name}Runtime", str(artifacts['runtime'].count('\n') + 1))

    console.print(artifact_table)

    # Save to files if requested
    if output_dir:
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

    # Step 4: Create or use existing project
    with SDLCClient() as sdlc:
        if project_id:
            console.print(f"\n[blue]Using existing project: {project_id}[/blue]")
            actual_project_id = project_id
        else:
            proj_name = project_name or f"{db.name.lower().replace('_', '-')}-model"
            console.print(f"\n[blue]Creating project: {proj_name}[/blue]")
            try:
                project = sdlc.create_project(
                    name=proj_name,
                    description=f"Legend model for DuckDB database {db.name}",
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

    # Step 5: Push all artifacts in a single atomic transaction
    console.print(f"\n[blue]Pushing artifacts to project {actual_project_id}...[/blue]")

    push_success = push_all_artifacts(
        artifacts=artifacts,
        project_id=actual_project_id,
        workspace_id=workspace_id,
        commit_message=f"Added complete Legend model for {db.name}",
        db_name=db.name,
    )

    # Summary
    if push_success:
        console.print(f"\n[bold green]Model generation complete![/bold green]")
        console.print(f"  Project ID: {actual_project_id}")
        console.print(f"  Workspace: {workspace_id}")
        console.print(f"  All artifacts pushed successfully")
        console.print(f"\n  View in Legend Studio: http://localhost:6900/studio/edit/{actual_project_id}/{workspace_id}")
    else:
        console.print(f"\n[bold red]Model generation failed![/bold red]")
        console.print(f"  Project ID: {actual_project_id}")
        console.print(f"  Workspace: {workspace_id}")
        console.print(f"  Check the error messages above for details")
        raise typer.Exit(1)


@app.command("list-duckdb-tables")
def list_duckdb_tables(
    database_path: str = typer.Argument(..., help="Path to DuckDB database file"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s", help="Filter to specific schema"),
):
    """List tables in a DuckDB database."""
    try:
        introspector = DuckDBIntrospector(database_path=database_path, read_only=True)
        db = introspector.introspect_database(schema_filter=schema, detect_relationships=False)
        introspector.close()

        for schema_obj in db.schemas:
            table_display = Table(title=f"Tables in schema: {schema_obj.name}")
            table_display.add_column("Table", style="cyan")
            table_display.add_column("Columns", style="green")

            for tbl in schema_obj.tables:
                table_display.add_row(tbl.name, str(len(tbl.columns)))

            console.print(table_display)

    except ImportError:
        console.print("[red]duckdb required. Install with: pip install duckdb[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
