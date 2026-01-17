"""Preview and validation MCP tools for Legend CLI.

Provides tools for previewing generated Pure code and validating
syntax before pushing to SDLC.
"""

import json
from typing import Any, List, Optional

from mcp.types import Tool

from ..context import MCPContext
from ..errors import ValidationError, EngineError


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
