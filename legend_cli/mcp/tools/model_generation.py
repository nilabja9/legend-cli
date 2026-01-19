"""Model generation MCP tools for Legend CLI.

Provides tools for generating Pure code from database schemas,
including stores, classes, connections, mappings, and runtimes.
"""

import json
from typing import Any, Dict, List, Optional

from mcp.types import Tool

from ..context import MCPContext, DatabaseType, sanitize_pure_identifier
from ..errors import GenerationError, IntrospectionError


def _needs_database_input_response(db_type: str, tool_name: str) -> str:
    """Return a structured response asking for database info."""
    hints = {
        "snowflake": "For Snowflake: database name (e.g., 'MY_DB')",
        "duckdb": "For DuckDB: path to .duckdb file (e.g., '/path/to/data.duckdb')",
    }
    hint = hints.get(db_type.lower(), "Provide the database identifier")

    return json.dumps({
        "status": "needs_input",
        "required_field": "database",
        "message": f"Please provide the database identifier to {tool_name.replace('_', ' ')}.",
        "db_type": db_type,
        "hint": hint,
        "options": [
            {
                "action": "provide_database",
                "description": "Provide database identifier"
            },
            {
                "action": "skip",
                "description": "Generate classes only (no store/connection)",
                "how": "Call with skip_database_prompt=true"
            }
        ]
    })


def get_tools() -> List[Tool]:
    """Return all model generation tools."""
    return [
        Tool(
            name="generate_model",
            description="End-to-end model generation from a database. Generates all artifacts: Store, Classes, Connection, Mapping, Runtime, and optionally Associations. This is the main tool for complete model generation.",
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
                    "package_prefix": {
                        "type": "string",
                        "description": "Package prefix for generated Pure code (default: 'model')",
                        "default": "model"
                    },
                    "enhanced": {
                        "type": "boolean",
                        "description": "Use enhanced analysis (enums, hierarchies, constraints). Default: true",
                        "default": True
                    },
                    "generate_docs": {
                        "type": "boolean",
                        "description": "Generate doc.doc annotations. Default: true",
                        "default": True
                    },
                    "snowflake_account": {
                        "type": "string",
                        "description": "Snowflake account for connection (Snowflake only)"
                    },
                    "snowflake_warehouse": {
                        "type": "string",
                        "description": "Snowflake warehouse for connection (Snowflake only)"
                    },
                    "snowflake_role": {
                        "type": "string",
                        "description": "Snowflake role for connection (Snowflake only)"
                    },
                    "duckdb_host": {
                        "type": "string",
                        "description": "DuckDB PostgreSQL proxy host (DuckDB only, default: host.docker.internal)"
                    },
                    "duckdb_port": {
                        "type": "integer",
                        "description": "DuckDB PostgreSQL proxy port (DuckDB only, default: 5433)"
                    },
                    "skip_database_prompt": {
                        "type": "boolean",
                        "description": "If true, generate classes only without store/connection when database is not provided",
                        "default": False
                    }
                },
                "required": ["db_type"]
            }
        ),
        Tool(
            name="generate_store",
            description="Generate only the database store definition (###Relational section) from an introspected schema.",
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
                    "package_prefix": {
                        "type": "string",
                        "description": "Package prefix (default: 'model')",
                        "default": "model"
                    },
                    "include_joins": {
                        "type": "boolean",
                        "description": "Include join definitions from relationships (default: true)",
                        "default": True
                    },
                    "skip_database_prompt": {
                        "type": "boolean",
                        "description": "If true, skip prompting for database",
                        "default": False
                    }
                },
                "required": ["db_type"]
            }
        ),
        Tool(
            name="generate_classes",
            description="Generate Pure class definitions from an introspected schema.",
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
                    "package_prefix": {
                        "type": "string",
                        "description": "Package prefix (default: 'model')",
                        "default": "model"
                    },
                    "generate_docs": {
                        "type": "boolean",
                        "description": "Generate doc.doc annotations",
                        "default": False
                    },
                    "skip_database_prompt": {
                        "type": "boolean",
                        "description": "If true, skip prompting for database",
                        "default": False
                    }
                },
                "required": ["db_type"]
            }
        ),
        Tool(
            name="generate_connection",
            description="Generate a database connection definition.",
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
                        "description": "Database name"
                    },
                    "package_prefix": {
                        "type": "string",
                        "description": "Package prefix (default: 'model')",
                        "default": "model"
                    },
                    "account": {
                        "type": "string",
                        "description": "Snowflake account (Snowflake only)"
                    },
                    "warehouse": {
                        "type": "string",
                        "description": "Snowflake warehouse (Snowflake only)"
                    },
                    "role": {
                        "type": "string",
                        "description": "Snowflake role (Snowflake only)",
                        "default": "ACCOUNTADMIN"
                    },
                    "region": {
                        "type": "string",
                        "description": "Snowflake region (Snowflake only)"
                    },
                    "auth_type": {
                        "type": "string",
                        "enum": ["keypair", "password"],
                        "description": "Authentication type (Snowflake only)",
                        "default": "keypair"
                    },
                    "host": {
                        "type": "string",
                        "description": "PostgreSQL proxy host (DuckDB only)",
                        "default": "host.docker.internal"
                    },
                    "port": {
                        "type": "integer",
                        "description": "PostgreSQL proxy port (DuckDB only)",
                        "default": 5433
                    },
                    "skip_database_prompt": {
                        "type": "boolean",
                        "description": "If true, skip prompting for database",
                        "default": False
                    }
                },
                "required": ["db_type"]
            }
        ),
        Tool(
            name="generate_mapping",
            description="Generate Pure mapping definition from an introspected schema.",
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
                    "package_prefix": {
                        "type": "string",
                        "description": "Package prefix (default: 'model')",
                        "default": "model"
                    },
                    "skip_database_prompt": {
                        "type": "boolean",
                        "description": "If true, skip prompting for database",
                        "default": False
                    }
                },
                "required": ["db_type"]
            }
        ),
        Tool(
            name="generate_runtime",
            description="Generate Pure runtime definition.",
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
                        "description": "Database name"
                    },
                    "package_prefix": {
                        "type": "string",
                        "description": "Package prefix (default: 'model')",
                        "default": "model"
                    },
                    "skip_database_prompt": {
                        "type": "boolean",
                        "description": "If true, skip prompting for database",
                        "default": False
                    }
                },
                "required": ["db_type"]
            }
        ),
        Tool(
            name="generate_associations",
            description="Generate Pure Association definitions from detected relationships.",
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
                    "package_prefix": {
                        "type": "string",
                        "description": "Package prefix (default: 'model')",
                        "default": "model"
                    },
                    "skip_database_prompt": {
                        "type": "boolean",
                        "description": "If true, skip prompting for database",
                        "default": False
                    }
                },
                "required": ["db_type"]
            }
        ),
        Tool(
            name="analyze_schema",
            description="Perform enhanced schema analysis to detect enumerations, class hierarchies, constraints, and derived properties. Uses LLM for intelligent pattern detection.",
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
                    "documentation": {
                        "type": "string",
                        "description": "Optional documentation content to inform analysis"
                    },
                    "detect_hierarchies": {
                        "type": "boolean",
                        "description": "Detect class hierarchies",
                        "default": True
                    },
                    "detect_enums": {
                        "type": "boolean",
                        "description": "Detect enumeration candidates",
                        "default": True
                    },
                    "detect_constraints": {
                        "type": "boolean",
                        "description": "Detect constraint suggestions",
                        "default": True
                    },
                    "detect_derived": {
                        "type": "boolean",
                        "description": "Detect derived properties",
                        "default": True
                    },
                    "use_llm": {
                        "type": "boolean",
                        "description": "Use LLM for enhanced detection",
                        "default": True
                    },
                    "confidence_threshold": {
                        "type": "number",
                        "description": "Minimum confidence threshold (0-1)",
                        "default": 0.7
                    }
                },
                "required": ["db_type", "database"]
            }
        ),
    ]


def _get_schema_or_error(ctx: MCPContext, db_type: str, database: str):
    """Get introspected schema or raise an error."""
    db_type_enum = DatabaseType(db_type.lower())
    schema = ctx.get_schema(db_type_enum, database)
    if not schema:
        raise IntrospectionError(
            f"Database not introspected. Call introspect_database first.",
            details={"db_type": db_type, "database": database}
        )
    return schema


async def generate_model(
    ctx: MCPContext,
    db_type: str,
    database: Optional[str] = None,
    schema_filter: Optional[str] = None,
    package_prefix: str = "model",
    enhanced: bool = True,
    generate_docs: bool = True,
    snowflake_account: Optional[str] = None,
    snowflake_warehouse: Optional[str] = None,
    snowflake_role: Optional[str] = None,
    duckdb_host: str = "host.docker.internal",
    duckdb_port: int = 5433,
    skip_database_prompt: bool = False,
) -> str:
    """Generate complete model from database."""
    # Check if database is needed
    if not database and not skip_database_prompt:
        return _needs_database_input_response(db_type, "generate_model")

    if not database and skip_database_prompt:
        return json.dumps({
            "status": "error",
            "message": "Cannot generate model without database. The 'skip' option is not available for generate_model as it requires database introspection.",
            "suggestion": "Please provide the database parameter, or use generate_classes with an already-introspected schema."
        })

    try:
        from legend_cli.pure.generator import PureCodeGenerator
        from legend_cli.pure.connections import SnowflakeConnectionGenerator, DuckDBConnectionGenerator

        db_type_enum = DatabaseType(db_type.lower())

        # Get schema - introspect if not already done
        schema = ctx.get_schema(db_type_enum, database)
        if not schema:
            # Need to introspect first
            from .database import introspect_database as do_introspect
            await do_introspect(ctx, db_type, database, schema_filter, detect_relationships=True)
            schema = ctx.get_schema(db_type_enum, database)

        if not schema:
            raise GenerationError("Failed to introspect database schema")

        # Update context settings
        ctx.package_prefix = package_prefix

        # Sanitize database name for Pure code
        db_name = sanitize_pure_identifier(schema.name)

        # Update the schema name to use sanitized version
        schema.name = db_name

        # Create generator
        generator = PureCodeGenerator(schema, package_prefix)

        # Generate connection code
        store_path = f"{package_prefix}::store::{db_name}"

        if db_type_enum == DatabaseType.SNOWFLAKE:
            conn_gen = SnowflakeConnectionGenerator()
            connection_code = conn_gen.generate(
                database_name=db_name,
                store_path=store_path,
                package_prefix=package_prefix,
                account=snowflake_account or "",
                warehouse=snowflake_warehouse or "",
                role=snowflake_role or "ACCOUNTADMIN",
            )
        else:
            conn_gen = DuckDBConnectionGenerator()
            connection_code = conn_gen.generate(
                database_name=db_name,
                store_path=store_path,
                package_prefix=package_prefix,
                host=duckdb_host,
                port=duckdb_port,
            )

        # Generate all artifacts
        docs = None
        if generate_docs and enhanced:
            # Could integrate doc generation here
            pass

        artifacts = generator.generate_all(connection_code, docs=docs)

        # Store pending artifacts
        ctx.clear_pending_artifacts()
        for artifact_type, code in artifacts.items():
            ctx.add_pending_artifact(artifact_type, code)

        # Build response
        return json.dumps({
            "status": "success",
            "database": database,
            "package_prefix": package_prefix,
            "artifacts_generated": list(artifacts.keys()),
            "summary": {
                "schemas": len(schema.schemas),
                "tables": sum(len(s.tables) for s in schema.schemas),
                "relationships": len(schema.relationships),
            },
            "artifacts": {
                artifact_type: {
                    "lines": len(code.split("\n")),
                    "preview": code[:500] + "..." if len(code) > 500 else code
                }
                for artifact_type, code in artifacts.items()
            },
            "message": f"Generated {len(artifacts)} artifacts. Use preview_changes to review or push_artifacts to push to SDLC."
        })

    except Exception as e:
        raise GenerationError(f"Model generation failed: {str(e)}")


async def generate_store(
    ctx: MCPContext,
    db_type: str,
    database: Optional[str] = None,
    package_prefix: str = "model",
    include_joins: bool = True,
    skip_database_prompt: bool = False,
) -> str:
    """Generate store definition."""
    if not database and not skip_database_prompt:
        return _needs_database_input_response(db_type, "generate_store")

    if not database and skip_database_prompt:
        return json.dumps({
            "status": "error",
            "message": "Cannot generate store without database. Store requires a database schema to be introspected.",
            "suggestion": "Please provide the database parameter."
        })

    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)

        # Sanitize database name for Pure code
        schema.name = sanitize_pure_identifier(schema.name)

        generator = PureCodeGenerator(schema, package_prefix)

        if include_joins:
            code = generator.generate_store_with_joins()
        else:
            code = generator.generate_store()

        ctx.add_pending_artifact("store", code)

        return json.dumps({
            "status": "success",
            "artifact_type": "store",
            "code": code,
            "message": "Store definition generated and added to pending artifacts"
        })

    except IntrospectionError:
        raise
    except Exception as e:
        raise GenerationError(f"Store generation failed: {str(e)}")


async def generate_classes(
    ctx: MCPContext,
    db_type: str,
    database: Optional[str] = None,
    package_prefix: str = "model",
    generate_docs: bool = False,
    skip_database_prompt: bool = False,
) -> str:
    """Generate class definitions."""
    if not database and not skip_database_prompt:
        return _needs_database_input_response(db_type, "generate_classes")

    if not database and skip_database_prompt:
        return json.dumps({
            "status": "error",
            "message": "Cannot generate classes without database. Classes require a database schema to be introspected.",
            "suggestion": "Please provide the database parameter."
        })

    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)

        # Sanitize database name for Pure code
        schema.name = sanitize_pure_identifier(schema.name)

        generator = PureCodeGenerator(schema, package_prefix)

        docs = None
        # Could integrate doc generation here if generate_docs is True

        code = generator.generate_classes(docs=docs)
        ctx.add_pending_artifact("classes", code)

        return json.dumps({
            "status": "success",
            "artifact_type": "classes",
            "code": code,
            "message": "Class definitions generated and added to pending artifacts"
        })

    except IntrospectionError:
        raise
    except Exception as e:
        raise GenerationError(f"Class generation failed: {str(e)}")


async def generate_connection(
    ctx: MCPContext,
    db_type: str,
    database: Optional[str] = None,
    package_prefix: str = "model",
    # Snowflake params
    account: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: str = "ACCOUNTADMIN",
    region: Optional[str] = None,
    auth_type: str = "keypair",
    # DuckDB params
    host: str = "host.docker.internal",
    port: int = 5433,
    skip_database_prompt: bool = False,
) -> str:
    """Generate connection definition."""
    if not database and not skip_database_prompt:
        return _needs_database_input_response(db_type, "generate_connection")

    if not database and skip_database_prompt:
        return json.dumps({
            "status": "error",
            "message": "Cannot generate connection without database. Connection requires a database name.",
            "suggestion": "Please provide the database parameter."
        })

    try:
        from legend_cli.pure.connections import SnowflakeConnectionGenerator, DuckDBConnectionGenerator

        db_type_enum = DatabaseType(db_type.lower())

        # Sanitize database name for Pure code
        db_name = sanitize_pure_identifier(database)
        store_path = f"{package_prefix}::store::{db_name}"

        if db_type_enum == DatabaseType.SNOWFLAKE:
            conn_gen = SnowflakeConnectionGenerator()
            code = conn_gen.generate(
                database_name=db_name,
                store_path=store_path,
                package_prefix=package_prefix,
                account=account or "",
                warehouse=warehouse or "",
                role=role,
                region=region,
                auth_type=auth_type,
            )
        else:
            conn_gen = DuckDBConnectionGenerator()
            code = conn_gen.generate(
                database_name=db_name,
                store_path=store_path,
                package_prefix=package_prefix,
                host=host,
                port=port,
            )

        ctx.add_pending_artifact("connection", code)

        return json.dumps({
            "status": "success",
            "artifact_type": "connection",
            "db_type": db_type,
            "code": code,
            "message": "Connection definition generated and added to pending artifacts"
        })

    except Exception as e:
        raise GenerationError(f"Connection generation failed: {str(e)}")


async def generate_mapping(
    ctx: MCPContext,
    db_type: str,
    database: Optional[str] = None,
    package_prefix: str = "model",
    skip_database_prompt: bool = False,
) -> str:
    """Generate mapping definition."""
    if not database and not skip_database_prompt:
        return _needs_database_input_response(db_type, "generate_mapping")

    if not database and skip_database_prompt:
        return json.dumps({
            "status": "error",
            "message": "Cannot generate mapping without database. Mapping requires a database schema to be introspected.",
            "suggestion": "Please provide the database parameter."
        })

    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)

        # Sanitize database name for Pure code
        schema.name = sanitize_pure_identifier(schema.name)

        generator = PureCodeGenerator(schema, package_prefix)
        code = generator.generate_mapping()

        ctx.add_pending_artifact("mapping", code)

        return json.dumps({
            "status": "success",
            "artifact_type": "mapping",
            "code": code,
            "message": "Mapping definition generated and added to pending artifacts"
        })

    except IntrospectionError:
        raise
    except Exception as e:
        raise GenerationError(f"Mapping generation failed: {str(e)}")


async def generate_runtime(
    ctx: MCPContext,
    db_type: str,
    database: Optional[str] = None,
    package_prefix: str = "model",
    skip_database_prompt: bool = False,
) -> str:
    """Generate runtime definition."""
    if not database and not skip_database_prompt:
        return _needs_database_input_response(db_type, "generate_runtime")

    if not database and skip_database_prompt:
        return json.dumps({
            "status": "error",
            "message": "Cannot generate runtime without database. Runtime requires a database schema to be introspected.",
            "suggestion": "Please provide the database parameter."
        })

    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)

        # Sanitize database name for Pure code
        schema.name = sanitize_pure_identifier(schema.name)

        generator = PureCodeGenerator(schema, package_prefix)
        code = generator.generate_runtime()

        ctx.add_pending_artifact("runtime", code)

        return json.dumps({
            "status": "success",
            "artifact_type": "runtime",
            "code": code,
            "message": "Runtime definition generated and added to pending artifacts"
        })

    except IntrospectionError:
        raise
    except Exception as e:
        raise GenerationError(f"Runtime generation failed: {str(e)}")


async def generate_associations(
    ctx: MCPContext,
    db_type: str,
    database: Optional[str] = None,
    package_prefix: str = "model",
    skip_database_prompt: bool = False,
) -> str:
    """Generate association definitions."""
    if not database and not skip_database_prompt:
        return _needs_database_input_response(db_type, "generate_associations")

    if not database and skip_database_prompt:
        return json.dumps({
            "status": "error",
            "message": "Cannot generate associations without database. Associations require a database schema to be introspected.",
            "suggestion": "Please provide the database parameter."
        })

    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)

        # Sanitize database name for Pure code
        schema.name = sanitize_pure_identifier(schema.name)

        generator = PureCodeGenerator(schema, package_prefix)
        code = generator.generate_associations()

        if not code:
            return json.dumps({
                "status": "success",
                "artifact_type": "associations",
                "code": "",
                "message": "No relationships detected - no associations generated"
            })

        ctx.add_pending_artifact("associations", code)

        return json.dumps({
            "status": "success",
            "artifact_type": "associations",
            "code": code,
            "relationship_count": len(schema.relationships),
            "message": "Association definitions generated and added to pending artifacts"
        })

    except IntrospectionError:
        raise
    except Exception as e:
        raise GenerationError(f"Association generation failed: {str(e)}")


async def analyze_schema(
    ctx: MCPContext,
    db_type: str,
    database: str,
    documentation: Optional[str] = None,
    detect_hierarchies: bool = True,
    detect_enums: bool = True,
    detect_constraints: bool = True,
    detect_derived: bool = True,
    use_llm: bool = True,
    confidence_threshold: float = 0.7,
) -> str:
    """Perform enhanced schema analysis."""
    try:
        from legend_cli.analysis.schema_analyzer import SchemaAnalyzer, AnalysisOptions, AnalysisContext

        schema = _get_schema_or_error(ctx, db_type, database)

        options = AnalysisOptions(
            detect_hierarchies=detect_hierarchies,
            detect_enums=detect_enums,
            detect_constraints=detect_constraints,
            detect_derived=detect_derived,
            use_llm=use_llm,
            confidence_threshold=confidence_threshold,
        )

        analyzer = SchemaAnalyzer(options=options)
        analysis_context = AnalysisContext(
            database=schema,
            documentation=documentation,
        )

        spec = analyzer.analyze(analysis_context)

        return json.dumps({
            "status": "success",
            "database": database,
            "analysis": {
                "hierarchies": [
                    {
                        "base_class": h.base_class_name,
                        "derived_classes": h.derived_classes,
                        "confidence": h.confidence,
                        "discriminator": h.discriminator_column
                    }
                    for h in spec.hierarchies
                ],
                "enumerations": [
                    {
                        "name": e.name,
                        "source_table": e.source_table,
                        "source_column": e.source_column,
                        "values": e.values[:10],  # Limit values shown
                        "confidence": e.confidence
                    }
                    for e in spec.enumerations
                ],
                "constraints": [
                    {
                        "class_name": c.class_name,
                        "constraint_name": c.constraint_name,
                        "expression": c.expression,
                        "confidence": c.confidence
                    }
                    for c in spec.constraints
                ],
                "derived_properties": [
                    {
                        "class_name": d.class_name,
                        "property_name": d.property_name,
                        "expression": d.expression,
                        "return_type": d.return_type,
                        "confidence": d.confidence
                    }
                    for d in spec.derived_properties
                ]
            },
            "summary": {
                "hierarchies": len(spec.hierarchies),
                "enumerations": len(spec.enumerations),
                "constraints": len(spec.constraints),
                "derived_properties": len(spec.derived_properties)
            },
            "message": f"Analysis complete: {len(spec.hierarchies)} hierarchies, {len(spec.enumerations)} enums, {len(spec.constraints)} constraints, {len(spec.derived_properties)} derived properties"
        })

    except IntrospectionError:
        raise
    except Exception as e:
        raise GenerationError(f"Schema analysis failed: {str(e)}")
