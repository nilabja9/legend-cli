"""Model generation MCP tools for Legend CLI.

Provides tools for generating Pure code from database schemas,
including stores, classes, connections, mappings, and runtimes.
"""

import json
from typing import Any, Dict, List, Optional

from mcp.types import Tool

from ..context import MCPContext, DatabaseType
from ..errors import GenerationError, IntrospectionError


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
                    }
                },
                "required": ["db_type", "database"]
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
                    }
                },
                "required": ["db_type", "database"]
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
                    }
                },
                "required": ["db_type", "database"]
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
                    }
                },
                "required": ["db_type", "database"]
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
                    }
                },
                "required": ["db_type", "database"]
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
                    }
                },
                "required": ["db_type", "database"]
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
                    }
                },
                "required": ["db_type", "database"]
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
    database: str,
    schema_filter: Optional[str] = None,
    package_prefix: str = "model",
    enhanced: bool = True,
    generate_docs: bool = True,
    snowflake_account: Optional[str] = None,
    snowflake_warehouse: Optional[str] = None,
    snowflake_role: Optional[str] = None,
    duckdb_host: str = "host.docker.internal",
    duckdb_port: int = 5433,
) -> str:
    """Generate complete model from database."""
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

        # Create generator
        generator = PureCodeGenerator(schema, package_prefix)

        # Generate connection code
        store_path = f"{package_prefix}::store::{schema.name}"

        if db_type_enum == DatabaseType.SNOWFLAKE:
            conn_gen = SnowflakeConnectionGenerator()
            connection_code = conn_gen.generate(
                database_name=schema.name,
                store_path=store_path,
                package_prefix=package_prefix,
                account=snowflake_account or "",
                warehouse=snowflake_warehouse or "",
                role=snowflake_role or "ACCOUNTADMIN",
            )
        else:
            conn_gen = DuckDBConnectionGenerator()
            connection_code = conn_gen.generate(
                database_name=schema.name,
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
    database: str,
    package_prefix: str = "model",
    include_joins: bool = True,
) -> str:
    """Generate store definition."""
    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)
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
    database: str,
    package_prefix: str = "model",
    generate_docs: bool = False,
) -> str:
    """Generate class definitions."""
    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)
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
    database: str,
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
) -> str:
    """Generate connection definition."""
    try:
        from legend_cli.pure.connections import SnowflakeConnectionGenerator, DuckDBConnectionGenerator

        db_type_enum = DatabaseType(db_type.lower())
        store_path = f"{package_prefix}::store::{database}"

        if db_type_enum == DatabaseType.SNOWFLAKE:
            conn_gen = SnowflakeConnectionGenerator()
            code = conn_gen.generate(
                database_name=database,
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
                database_name=database,
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
    database: str,
    package_prefix: str = "model",
) -> str:
    """Generate mapping definition."""
    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)
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
    database: str,
    package_prefix: str = "model",
) -> str:
    """Generate runtime definition."""
    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)
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
    database: str,
    package_prefix: str = "model",
) -> str:
    """Generate association definitions."""
    try:
        from legend_cli.pure.generator import PureCodeGenerator

        schema = _get_schema_or_error(ctx, db_type, database)
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
