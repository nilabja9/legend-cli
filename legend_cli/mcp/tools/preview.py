"""Preview and validation MCP tools for Legend CLI.

Provides tools for previewing generated Pure code and validating
syntax before pushing to SDLC.
"""

import json
from typing import Any, List, Optional

from mcp.types import Tool

from ..context import MCPContext
from ..errors import ValidationError, EngineError, EngineParseError


def get_tools() -> List[Tool]:
    """Return all preview and validation tools."""
    return [
        Tool(
            name="preview_changes",
            description="Preview all pending artifacts before pushing to SDLC. Shows the generated Pure code for review.",
            inputSchema={
                "type": "object",
                "properties": {
                    "artifact_type": {
                        "type": "string",
                        "description": "Optional: filter to specific artifact type (store, classes, connection, mapping, runtime, associations)"
                    },
                    "include_full_code": {
                        "type": "boolean",
                        "description": "Include full code (default: True, otherwise shows preview)",
                        "default": True
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="validate_pure_code",
            description="Validate Pure code syntax using Legend Engine. Can validate pending artifacts or custom Pure code.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pure_code": {
                        "type": "string",
                        "description": "Optional: Pure code to validate. If not provided, validates all pending artifacts."
                    },
                    "artifact_type": {
                        "type": "string",
                        "description": "Optional: if validating pending artifacts, filter to specific type"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="validate_model_completeness",
            description="Validate that all required artifacts are present for a complete model before pushing. Checks for missing dependencies (e.g., mapping needs store and classes, runtime needs connection and mapping). Use this BEFORE push_artifacts to ensure the model is complete.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
    ]


async def preview_changes(
    ctx: MCPContext,
    artifact_type: Optional[str] = None,
    include_full_code: bool = True,
) -> str:
    """Preview pending artifacts."""
    try:
        if not ctx.pending_artifacts:
            return json.dumps({
                "status": "no_artifacts",
                "message": "No pending artifacts. Generate artifacts first using generate_model or other generation tools."
            })

        # Filter by type if specified
        artifacts = ctx.pending_artifacts
        if artifact_type:
            artifacts = [a for a in artifacts if a.artifact_type == artifact_type]
            if not artifacts:
                return json.dumps({
                    "status": "not_found",
                    "artifact_type": artifact_type,
                    "available_types": list(set(a.artifact_type for a in ctx.pending_artifacts)),
                    "message": f"No pending artifacts of type '{artifact_type}'"
                })

        # Build preview
        preview = []
        total_lines = 0

        for artifact in artifacts:
            lines = len(artifact.pure_code.split("\n"))
            total_lines += lines

            artifact_preview = {
                "type": artifact.artifact_type,
                "lines": lines,
                "path": artifact.path,
            }

            if include_full_code:
                artifact_preview["code"] = artifact.pure_code
            else:
                # Show first 20 lines as preview
                preview_lines = artifact.pure_code.split("\n")[:20]
                artifact_preview["preview"] = "\n".join(preview_lines)
                if lines > 20:
                    artifact_preview["preview"] += f"\n... ({lines - 20} more lines)"

            preview.append(artifact_preview)

        return json.dumps({
            "status": "success",
            "artifact_count": len(artifacts),
            "total_lines": total_lines,
            "artifacts": preview,
            "summary": ctx.get_pending_artifacts_summary(),
            "message": f"Showing {len(artifacts)} pending artifacts ({total_lines} total lines)"
        }, indent=2)

    except Exception as e:
        return json.dumps({
            "status": "error",
            "message": f"Preview failed: {str(e)}"
        })


async def validate_pure_code(
    ctx: MCPContext,
    pure_code: Optional[str] = None,
    artifact_type: Optional[str] = None,
) -> str:
    """Validate Pure code syntax."""
    try:
        from legend_cli.engine_client import EngineClient

        codes_to_validate = {}

        if pure_code:
            # Validate provided code
            codes_to_validate["custom"] = pure_code
        else:
            # Validate pending artifacts
            if not ctx.pending_artifacts:
                return json.dumps({
                    "status": "no_code",
                    "message": "No code to validate. Provide pure_code parameter or generate artifacts first."
                })

            artifacts = ctx.pending_artifacts
            if artifact_type:
                artifacts = [a for a in artifacts if a.artifact_type == artifact_type]

            for artifact in artifacts:
                if artifact.pure_code:
                    codes_to_validate[artifact.artifact_type] = artifact.pure_code

        if not codes_to_validate:
            return json.dumps({
                "status": "no_code",
                "message": "No code to validate"
            })

        # Validate each piece of code
        validation_results = []
        all_valid = True

        with EngineClient() as engine:
            for code_type, code in codes_to_validate.items():
                try:
                    result = engine.grammar_to_json(code)
                    entities = engine.extract_entities(result)

                    validation_results.append({
                        "type": code_type,
                        "valid": True,
                        "entities_found": len(entities),
                        "entity_paths": [e.get("path") for e in entities]
                    })
                except EngineParseError as parse_err:
                    # Handle parsing errors with detailed location information
                    all_valid = False
                    validation_results.append({
                        "type": code_type,
                        "valid": False,
                        "error": parse_err.message,
                        "location": parse_err.get_formatted_location(),
                        "source_info": parse_err.source_info,
                        "user_message": parse_err.get_user_friendly_message(),
                    })
                except Exception as e:
                    all_valid = False
                    error_msg = str(e)
                    # Try to extract meaningful error from response
                    if "message" in error_msg:
                        try:
                            import re
                            match = re.search(r'"message":\s*"([^"]+)"', error_msg)
                            if match:
                                error_msg = match.group(1)
                        except Exception:
                            pass

                    validation_results.append({
                        "type": code_type,
                        "valid": False,
                        "error": error_msg
                    })

        return json.dumps({
            "status": "valid" if all_valid else "invalid",
            "all_valid": all_valid,
            "validated_count": len(codes_to_validate),
            "results": validation_results,
            "message": "All code is valid" if all_valid else "Some code has validation errors"
        })

    except Exception as e:
        raise EngineError(f"Validation failed: {str(e)}")


async def validate_model_completeness(ctx: MCPContext) -> str:
    """Validate that all required artifacts are present for a complete model.

    This checks:
    1. All artifact types have non-empty Pure code
    2. Dependencies are satisfied (e.g., mapping needs store and classes)
    3. Each artifact can be parsed by the Legend Engine
    """
    try:
        from legend_cli.engine_client import EngineClient

        if not ctx.pending_artifacts:
            return json.dumps({
                "status": "no_artifacts",
                "complete": False,
                "message": "No pending artifacts. Generate artifacts first using generate_model or other generation tools."
            })

        # Collect artifact types and check for empty code
        artifact_types = set()
        empty_artifacts = []

        for artifact in ctx.pending_artifacts:
            if artifact.pure_code and artifact.pure_code.strip():
                artifact_types.add(artifact.artifact_type)
            else:
                empty_artifacts.append(artifact.artifact_type)

        errors = []
        warnings = []

        # Report empty artifacts
        for empty in empty_artifacts:
            errors.append(f"Artifact '{empty}' has empty Pure code")

        # Check dependencies
        has_mapping = "mapping" in artifact_types
        has_runtime = "runtime" in artifact_types
        has_store = "store" in artifact_types
        has_connection = "connection" in artifact_types
        has_classes = "classes" in artifact_types
        has_associations = "associations" in artifact_types

        # Critical dependencies (will cause push to fail)
        if has_mapping and not has_store:
            errors.append("CRITICAL: Mapping references Store but Store is missing")
        if has_mapping and not has_classes:
            errors.append("CRITICAL: Mapping references Classes but Classes are missing")
        if has_runtime and not has_store:
            errors.append("CRITICAL: Runtime references Store but Store is missing")
        if has_runtime and not has_connection:
            errors.append("CRITICAL: Runtime references Connection but Connection is missing")
        if has_runtime and not has_mapping:
            errors.append("CRITICAL: Runtime references Mapping but Mapping is missing")
        if has_connection and not has_store:
            errors.append("CRITICAL: Connection references Store but Store is missing")

        # Warnings (model may be incomplete)
        if has_store and not has_classes:
            warnings.append("Store is present but Classes are missing - model incomplete")
        if has_classes and not has_mapping:
            warnings.append("Classes are present but Mapping is missing - classes won't be mapped to database")
        if has_mapping and not has_runtime:
            warnings.append("Mapping is present but Runtime is missing - won't be executable")

        # Validate syntax with Engine
        parse_results = []
        parse_errors = []

        with EngineClient() as engine:
            for artifact in ctx.pending_artifacts:
                if not artifact.pure_code or not artifact.pure_code.strip():
                    continue

                try:
                    result = engine.grammar_to_json(artifact.pure_code)
                    entities = engine.extract_entities(result)
                    parse_results.append({
                        "artifact_type": artifact.artifact_type,
                        "valid": True,
                        "entity_count": len(entities),
                        "entity_paths": [e.get("path") for e in entities]
                    })
                except EngineParseError as parse_err:
                    # Handle parsing errors with location info
                    error_msg = parse_err.get_user_friendly_message()
                    parse_errors.append(f"Failed to parse '{artifact.artifact_type}': {error_msg}")
                    parse_results.append({
                        "artifact_type": artifact.artifact_type,
                        "valid": False,
                        "error": parse_err.message,
                        "location": parse_err.get_formatted_location(),
                        "source_info": parse_err.source_info,
                    })
                except Exception as parse_err:
                    error_msg = str(parse_err)
                    parse_errors.append(f"Failed to parse '{artifact.artifact_type}': {error_msg[:200]}")
                    parse_results.append({
                        "artifact_type": artifact.artifact_type,
                        "valid": False,
                        "error": error_msg[:200]
                    })

        if parse_errors:
            errors.extend(parse_errors)

        # Determine overall status
        is_complete = len(errors) == 0
        status = "complete" if is_complete else "incomplete"

        response = {
            "status": status,
            "complete": is_complete,
            "artifact_types_present": sorted(list(artifact_types)),
            "artifact_count": len(artifact_types),
            "parse_results": parse_results,
        }

        if errors:
            response["errors"] = errors
            response["error_count"] = len(errors)

        if warnings:
            response["warnings"] = warnings

        if is_complete:
            response["message"] = f"Model is complete with {len(artifact_types)} artifact types. Ready to push."
        else:
            response["message"] = f"Model has {len(errors)} error(s). Please fix before pushing."
            response["suggestion"] = "Use 'generate_model' to regenerate all artifacts, or use individual tools to regenerate specific artifacts."

        return json.dumps(response, indent=2)

    except Exception as e:
        return json.dumps({
            "status": "error",
            "complete": False,
            "message": f"Validation failed: {str(e)}"
        })
