"""SDLC MCP tools for Legend CLI.

Provides tools for interacting with Legend SDLC for project/workspace
management and entity operations.
"""

import json
import logging
from typing import Any, List, Optional

from mcp.types import Tool

from ..context import MCPContext
from ..errors import SDLCError, WorkspaceNotFoundError, ProjectNotFoundError, PartialPushError, EngineParseError

logger = logging.getLogger(__name__)


def get_tools() -> List[Tool]:
    """Return all SDLC-related tools."""
    return [
        Tool(
            name="list_projects",
            description="List all Legend SDLC projects accessible to the current user.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="create_project",
            description="Create a new Legend SDLC project. Use this when you need to create a new project for storing models. The project will be created with the specified name and optional description.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the project to create (e.g., 'SEC-FILINGS', 'Customer-Analytics')"
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional description of the project"
                    },
                    "group_id": {
                        "type": "string",
                        "description": "Maven group ID for the project (default: 'org.demo.legend')",
                        "default": "org.demo.legend"
                    },
                    "artifact_id": {
                        "type": "string",
                        "description": "Maven artifact ID (default: derived from project name)"
                    }
                },
                "required": ["name"]
            }
        ),
        Tool(
            name="list_workspaces",
            description="List all workspaces in a Legend SDLC project.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    }
                },
                "required": ["project_id"]
            }
        ),
        Tool(
            name="create_workspace",
            description="Create a new workspace in a Legend SDLC project.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID to create"
                    }
                },
                "required": ["project_id", "workspace_id"]
            }
        ),
        Tool(
            name="get_workspace_entities",
            description="Get all entities in a Legend SDLC workspace.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    }
                },
                "required": ["project_id", "workspace_id"]
            }
        ),
        Tool(
            name="push_artifacts",
            description="Push pending artifacts to Legend SDLC workspace. This performs an atomic batch push of all generated Pure code with optional verification and retry logic.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Project ID"
                    },
                    "workspace_id": {
                        "type": "string",
                        "description": "Workspace ID"
                    },
                    "commit_message": {
                        "type": "string",
                        "description": "Commit message for the change",
                        "default": "Generated via Legend CLI MCP"
                    },
                    "clear_pending": {
                        "type": "boolean",
                        "description": "Clear pending artifacts after push",
                        "default": True
                    },
                    "verify_push": {
                        "type": "boolean",
                        "description": "Verify entities exist after push (default: true)",
                        "default": True
                    },
                    "max_retries": {
                        "type": "integer",
                        "description": "Maximum retry attempts for transient failures (default: 3)",
                        "default": 3
                    }
                },
                "required": ["project_id", "workspace_id"]
            }
        ),
    ]


async def list_projects(ctx: MCPContext) -> str:
    """List all SDLC projects."""
    try:
        from legend_cli.sdlc_client import SDLCClient

        with SDLCClient() as client:
            projects = client.list_projects()

        return json.dumps({
            "status": "success",
            "projects": [
                {
                    "id": p.get("projectId"),
                    "name": p.get("name"),
                    "description": p.get("description"),
                    "group_id": p.get("groupId"),
                    "artifact_id": p.get("artifactId"),
                }
                for p in projects
            ],
            "count": len(projects),
            "message": f"Found {len(projects)} projects"
        })

    except Exception as e:
        raise SDLCError(f"Failed to list projects: {str(e)}")


async def create_project(
    ctx: MCPContext,
    name: str,
    description: Optional[str] = None,
    group_id: str = "org.demo.legend",
    artifact_id: Optional[str] = None,
) -> str:
    """Create a new SDLC project."""
    try:
        from legend_cli.sdlc_client import SDLCClient

        with SDLCClient() as client:
            project = client.create_project(
                name=name,
                description=description,
                group_id=group_id,
                artifact_id=artifact_id,
            )

        project_id = project.get("projectId")

        # Update context with new project
        ctx.current_project_id = project_id

        return json.dumps({
            "status": "success",
            "project": {
                "id": project_id,
                "name": project.get("name"),
                "description": project.get("description"),
                "group_id": project.get("groupId"),
                "artifact_id": project.get("artifactId"),
            },
            "message": f"Successfully created project '{name}' with ID '{project_id}'. You can now create a workspace in this project using create_workspace."
        })

    except Exception as e:
        error_str = str(e)
        if "409" in error_str or "already exists" in error_str.lower():
            return json.dumps({
                "status": "exists",
                "name": name,
                "message": f"Project '{name}' already exists. Use list_projects to find the existing project ID."
            })
        raise SDLCError(f"Failed to create project: {error_str}")


async def list_workspaces(ctx: MCPContext, project_id: str) -> str:
    """List workspaces in a project."""
    try:
        from legend_cli.sdlc_client import SDLCClient

        with SDLCClient() as client:
            workspaces = client.list_workspaces(project_id)

        return json.dumps({
            "status": "success",
            "project_id": project_id,
            "workspaces": [
                {
                    "id": w.get("workspaceId"),
                    "type": w.get("type"),
                }
                for w in workspaces
            ],
            "count": len(workspaces),
            "message": f"Found {len(workspaces)} workspaces in project {project_id}"
        })

    except Exception as e:
        if "404" in str(e):
            raise ProjectNotFoundError(project_id)
        raise SDLCError(f"Failed to list workspaces: {str(e)}")


async def create_workspace(ctx: MCPContext, project_id: str, workspace_id: str) -> str:
    """Create a new workspace."""
    try:
        from legend_cli.sdlc_client import SDLCClient

        with SDLCClient() as client:
            workspace = client.create_workspace(project_id, workspace_id)

        # Update context
        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "project_id": project_id,
            "workspace_id": workspace_id,
            "workspace": workspace,
            "message": f"Created workspace '{workspace_id}' in project '{project_id}'"
        })

    except Exception as e:
        if "404" in str(e):
            raise ProjectNotFoundError(project_id)
        if "409" in str(e) or "already exists" in str(e).lower():
            return json.dumps({
                "status": "exists",
                "project_id": project_id,
                "workspace_id": workspace_id,
                "message": f"Workspace '{workspace_id}' already exists"
            })
        raise SDLCError(f"Failed to create workspace: {str(e)}")


async def get_workspace_entities(ctx: MCPContext, project_id: str, workspace_id: str) -> str:
    """Get entities in a workspace."""
    try:
        from legend_cli.sdlc_client import SDLCClient

        with SDLCClient() as client:
            entities = client.list_entities(project_id, workspace_id)

        # Update context
        ctx.set_sdlc_context(project_id, workspace_id)

        return json.dumps({
            "status": "success",
            "project_id": project_id,
            "workspace_id": workspace_id,
            "entities": [
                {
                    "path": e.get("path"),
                    "classifier": e.get("classifierPath"),
                }
                for e in entities
            ],
            "count": len(entities),
            "message": f"Found {len(entities)} entities in workspace"
        })

    except Exception as e:
        if "404" in str(e):
            raise WorkspaceNotFoundError(project_id, workspace_id)
        raise SDLCError(f"Failed to get workspace entities: {str(e)}")


def _validate_artifacts(ctx: MCPContext) -> dict:
    """Validate pending artifacts before push.

    Returns dict with:
        - valid: bool indicating if all validations passed
        - errors: list of error messages
        - warnings: list of warning messages
        - artifact_types: set of present artifact types
    """
    errors = []
    warnings = []
    artifact_types = set()

    for artifact in ctx.pending_artifacts:
        if artifact.pure_code and artifact.pure_code.strip():
            artifact_types.add(artifact.artifact_type)
        else:
            errors.append(f"Artifact '{artifact.artifact_type}' has empty Pure code")

    # Check for required artifacts when we have interconnected model
    has_mapping = "mapping" in artifact_types
    has_runtime = "runtime" in artifact_types
    has_store = "store" in artifact_types
    has_connection = "connection" in artifact_types
    has_classes = "classes" in artifact_types

    # If we have mapping/runtime, we need store and classes
    if has_mapping and not has_store:
        errors.append("Mapping artifact is present but Store is missing. Mapping references the store and will fail.")

    if has_mapping and not has_classes:
        errors.append("Mapping artifact is present but Classes are missing. Mapping references classes and will fail.")

    if has_runtime and not has_store:
        errors.append("Runtime artifact is present but Store is missing. Runtime references the store and will fail.")

    if has_runtime and not has_connection:
        errors.append("Runtime artifact is present but Connection is missing. Runtime references the connection and will fail.")

    if has_runtime and not has_mapping:
        errors.append("Runtime artifact is present but Mapping is missing. Runtime references the mapping and will fail.")

    if has_connection and not has_store:
        errors.append("Connection artifact is present but Store is missing. Connection references the store and will fail.")

    # Warnings for incomplete models
    if has_store and not has_classes:
        warnings.append("Store is present but no Classes. Consider generating classes for a complete model.")

    if has_classes and not has_mapping:
        warnings.append("Classes are present but no Mapping. The classes won't be mapped to the database.")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "artifact_types": artifact_types
    }


def _validate_parsed_entities(entities: list, artifact_types: set, parse_diagnostics: dict = None) -> dict:
    """Validate that parsed entities match expected artifact types.

    Args:
        entities: List of parsed entities
        artifact_types: Set of artifact types expected to be present
        parse_diagnostics: Optional dict with diagnostic info from parsing (e.g., from debug_parse_response)

    Returns dict with:
        - valid: bool
        - errors: list of error messages
        - entity_types: dict mapping classifier to count
        - missing: list of expected but missing types
        - diagnostics: Additional diagnostic information
    """
    errors = []
    entity_types = {}

    for entity in entities:
        classifier = entity.get("classifierPath", "")
        entity_types[classifier] = entity_types.get(classifier, 0) + 1

    # Map artifact types to expected classifiers
    artifact_to_classifier = {
        "store": "meta::relational::metamodel::Database",
        "classes": "meta::pure::metamodel::type::Class",
        "connection": "meta::pure::runtime::PackageableConnection",
        "mapping": "meta::pure::mapping::Mapping",
        "runtime": "meta::pure::runtime::PackageableRuntime",
        "associations": "meta::pure::metamodel::relationship::Association",
    }

    missing = []
    for artifact_type in artifact_types:
        expected_classifier = artifact_to_classifier.get(artifact_type)
        if expected_classifier and expected_classifier not in entity_types:
            missing.append(artifact_type)
            error_msg = f"Artifact type '{artifact_type}' was in pending artifacts but no entity with classifier '{expected_classifier}' was parsed."
            if parse_diagnostics and artifact_type in parse_diagnostics:
                diag = parse_diagnostics[artifact_type]
                error_msg += f" Diagnostic: {diag.get('diagnostic', 'N/A')}"
            errors.append(error_msg)

    # Log validation results
    logger.debug("Entity validation: %d entities, types=%s, missing=%s", len(entities), list(entity_types.keys()), missing)

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "entity_types": entity_types,
        "missing": missing,
        "diagnostics": parse_diagnostics or {}
    }


async def push_artifacts(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    commit_message: str = "Generated via Legend CLI MCP",
    clear_pending: bool = True,
    verify_push: bool = True,
    max_retries: int = 3,
) -> str:
    """Push pending artifacts to SDLC with comprehensive validation, retry logic, and verification."""
    try:
        from legend_cli.sdlc_client import SDLCClient
        from legend_cli.engine_client import EngineClient

        if not ctx.pending_artifacts:
            return json.dumps({
                "status": "no_artifacts",
                "message": "No pending artifacts to push. Generate artifacts first using generate_model or other generation tools."
            })

        # Step 1: Validate artifacts before parsing
        validation = _validate_artifacts(ctx)
        if not validation["valid"]:
            return json.dumps({
                "status": "validation_error",
                "errors": validation["errors"],
                "warnings": validation["warnings"],
                "artifact_types_present": list(validation["artifact_types"]),
                "message": "Artifact validation failed. Please regenerate the missing artifacts before pushing.",
                "suggestion": "Use 'generate_model' to regenerate all artifacts, or use individual generation tools (generate_store, generate_classes, etc.) to regenerate specific missing artifacts."
            })

        # Step 2: Parse all Pure code through Engine with error tracking
        parse_errors = []
        parsed_by_type = {}
        parse_diagnostics = {}  # Store diagnostics for failed parses

        with EngineClient() as engine:
            pure_codes = {}
            for artifact in ctx.pending_artifacts:
                if artifact.pure_code and artifact.pure_code.strip():
                    pure_codes[artifact.artifact_type] = artifact.pure_code

            # Parse each artifact separately to track failures
            all_entities = []
            for artifact_type, code in pure_codes.items():
                # Log artifact being parsed (truncated for readability)
                code_preview = code[:200] + "..." if len(code) > 200 else code
                logger.debug("Parsing artifact '%s': %s", artifact_type, code_preview.replace('\n', '\\n'))

                try:
                    entities = engine.parse_pure_code(code)
                    if entities:
                        all_entities.extend(entities)
                        parsed_by_type[artifact_type] = len(entities)
                        logger.debug("Artifact '%s' parsed successfully: %d entities", artifact_type, len(entities))
                    else:
                        # Use debug_parse_response to get detailed diagnostics
                        logger.warning("Artifact '%s' parsed but returned no entities, getting diagnostics", artifact_type)
                        diag_result = engine.debug_parse_response(code)
                        parse_diagnostics[artifact_type] = diag_result
                        diag_msg = diag_result.get("diagnostic", "No diagnostic available")
                        parse_errors.append(f"Artifact '{artifact_type}' parsed but returned 0 entities. Diagnostic: {diag_msg}")
                except EngineParseError as parse_err:
                    # Handle parsing errors with detailed location information
                    logger.error(
                        "Parsing error in artifact '%s': %s at %s",
                        artifact_type,
                        parse_err.message,
                        parse_err.get_formatted_location(),
                    )
                    error_msg = f"'{artifact_type}' has syntax error: {parse_err.message}"
                    if parse_err.source_info:
                        error_msg += f" (at {parse_err.get_formatted_location()})"
                    parse_errors.append(error_msg)
                    parse_diagnostics[artifact_type] = {
                        "error": parse_err.message,
                        "location": parse_err.get_formatted_location(),
                        "source_info": parse_err.source_info,
                    }
                except Exception as parse_err:
                    logger.error("Failed to parse artifact '%s': %s", artifact_type, str(parse_err))
                    parse_errors.append(f"Failed to parse '{artifact_type}': {str(parse_err)}")

        if parse_errors:
            # Build suggestion with line info if available
            suggestion = "Use 'preview_changes' to review the generated Pure code, or 'validate_pure_code' to check syntax."
            for diag in parse_diagnostics.values():
                if diag.get("location"):
                    suggestion += f" Check {diag.get('location')} for the error."
                    break

            response_data = {
                "status": "parse_error",
                "errors": parse_errors,
                "successfully_parsed": parsed_by_type,
                "message": "Some artifacts failed to parse. Please check the Pure code syntax.",
                "suggestion": suggestion,
            }
            # Include diagnostics for failed parses
            if parse_diagnostics:
                response_data["diagnostics"] = {
                    k: {
                        "error": v.get("error"),
                        "location": v.get("location"),
                        "source_info": v.get("source_info"),
                        "diagnostic": v.get("diagnostic"),
                    }
                    for k, v in parse_diagnostics.items()
                }
            return json.dumps(response_data)

        if not all_entities:
            return json.dumps({
                "status": "error",
                "message": "No valid entities extracted from Pure code",
                "artifact_types_attempted": list(pure_codes.keys())
            })

        # Step 3: Validate parsed entities match expected types
        entity_validation = _validate_parsed_entities(all_entities, validation["artifact_types"], parse_diagnostics)
        if not entity_validation["valid"]:
            return json.dumps({
                "status": "entity_validation_error",
                "errors": entity_validation["errors"],
                "missing_types": entity_validation["missing"],
                "parsed_entity_types": entity_validation["entity_types"],
                "message": "Parsed entities don't match expected artifact types. Some Pure code may have syntax errors.",
                "suggestion": "Use 'preview_changes' with include_full_code=true to review the Pure code for the missing types."
            })

        # Step 4: Push to SDLC with retry logic
        with SDLCClient() as client:
            if max_retries > 0:
                result = client.update_entities_with_retry(
                    project_id=project_id,
                    workspace_id=workspace_id,
                    entities=all_entities,
                    message=commit_message,
                    max_retries=max_retries,
                )
            else:
                result = client.update_entities(
                    project_id=project_id,
                    workspace_id=workspace_id,
                    entities=all_entities,
                    message=commit_message,
                )

            # Step 5: Verify entities were created
            verification_result = None
            if verify_push:
                entity_paths = [e.get("path") for e in all_entities]
                verification_result = client.verify_entities_exist(
                    project_id, workspace_id, entity_paths
                )

                if not verification_result["all_found"]:
                    return json.dumps({
                        "status": "verification_failed",
                        "message": f"Push appeared successful but {verification_result['total_missing']} entities not found in workspace",
                        "missing_entities": verification_result["missing"],
                        "found_entities": verification_result["found"],
                        "total_expected": verification_result["total_expected"],
                        "total_found": verification_result["total_found"],
                        "suggestion": "Try pushing again with max_retries=3, or check Legend Studio for any errors."
                    })

        # Update context
        ctx.set_sdlc_context(project_id, workspace_id)

        pushed_count = len(all_entities)
        artifact_types = list(pure_codes.keys())

        if clear_pending:
            ctx.clear_pending_artifacts()

        response = {
            "status": "success",
            "project_id": project_id,
            "workspace_id": workspace_id,
            "commit_message": commit_message,
            "pushed_entities": [
                {
                    "path": e.get("path"),
                    "classifier": e.get("classifierPath")
                }
                for e in all_entities
            ],
            "artifact_types": artifact_types,
            "entity_count": pushed_count,
            "parsed_by_type": parsed_by_type,
            "cleared_pending": clear_pending,
            "message": f"Successfully pushed {pushed_count} entities to workspace '{workspace_id}'"
        }

        # Include revision from SDLC response if available
        if result and isinstance(result, dict) and result.get("revision"):
            response["revision"] = result["revision"]

        # Include verification details
        if verify_push and verification_result:
            response["verification"] = {
                "verified": True,
                "all_found": verification_result["all_found"],
                "total_verified": verification_result["total_found"]
            }

        # Include warnings if any
        if validation["warnings"]:
            response["warnings"] = validation["warnings"]

        return json.dumps(response)

    except Exception as e:
        if "404" in str(e):
            raise WorkspaceNotFoundError(project_id, workspace_id)
        raise SDLCError(f"Failed to push artifacts: {str(e)}")
