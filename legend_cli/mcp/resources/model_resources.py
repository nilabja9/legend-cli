"""Model-related MCP resources for Legend CLI.

Provides resources for accessing generated Pure code and pending artifacts.
"""

import json
from typing import List

from mcp.types import Resource

from ..context import get_context


def get_model_resources() -> List[Resource]:
    """Get all model-related resources."""
    ctx = get_context()
    resources = []

    # Add resource for pending artifacts
    if ctx.pending_artifacts:
        artifact_summary = ctx.get_pending_artifacts_summary()
        artifact_types = ", ".join(artifact_summary.keys())
        resources.append(Resource(
            uri="legend://pending-artifacts",
            name="Pending Artifacts",
            description=f"{len(ctx.pending_artifacts)} artifacts pending ({artifact_types})",
            mimeType="application/json"
        ))

        # Add individual artifact resources
        for i, artifact in enumerate(ctx.pending_artifacts):
            resources.append(Resource(
                uri=f"legend://artifact/{i}/{artifact.artifact_type}",
                name=f"Artifact: {artifact.artifact_type}",
                description=f"Generated {artifact.artifact_type} Pure code",
                mimeType="text/plain"
            ))

    return resources


def read_pending_artifacts() -> str:
    """Read the pending artifacts resource."""
    ctx = get_context()

    if not ctx.pending_artifacts:
        return json.dumps({
            "message": "No pending artifacts",
            "count": 0,
            "artifacts": []
        })

    return json.dumps({
        "count": len(ctx.pending_artifacts),
        "summary": ctx.get_pending_artifacts_summary(),
        "artifacts": [
            {
                "index": i,
                "type": a.artifact_type,
                "path": a.path,
                "lines": len(a.pure_code.split("\n")),
                "preview": a.pure_code[:300] + "..." if len(a.pure_code) > 300 else a.pure_code
            }
            for i, a in enumerate(ctx.pending_artifacts)
        ]
    }, indent=2)


def read_artifact(index: int) -> str:
    """Read a specific artifact by index."""
    ctx = get_context()

    if index < 0 or index >= len(ctx.pending_artifacts):
        return json.dumps({
            "error": f"Artifact index {index} out of range",
            "available_count": len(ctx.pending_artifacts)
        })

    artifact = ctx.pending_artifacts[index]
    return artifact.pure_code


def read_artifact_by_type(artifact_type: str) -> str:
    """Read artifacts of a specific type."""
    ctx = get_context()

    matching = [a for a in ctx.pending_artifacts if a.artifact_type == artifact_type]

    if not matching:
        available = list(set(a.artifact_type for a in ctx.pending_artifacts))
        return json.dumps({
            "error": f"No artifacts of type '{artifact_type}'",
            "available_types": available
        })

    if len(matching) == 1:
        return matching[0].pure_code

    return json.dumps({
        "type": artifact_type,
        "count": len(matching),
        "artifacts": [
            {
                "index": i,
                "path": a.path,
                "code": a.pure_code
            }
            for i, a in enumerate(matching)
        ]
    }, indent=2)
