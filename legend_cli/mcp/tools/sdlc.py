"""SDLC MCP tools for Legend CLI.

Provides tools for interacting with Legend SDLC for project/workspace
management and entity operations.
"""

import json
from typing import Any, List, Optional

from mcp.types import Tool

from ..context import MCPContext
from ..errors import SDLCError, WorkspaceNotFoundError, ProjectNotFoundError


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
            description="Push pending artifacts to Legend SDLC workspace. This performs an atomic batch push of all generated Pure code.",
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


async def push_artifacts(
    ctx: MCPContext,
    project_id: str,
    workspace_id: str,
    commit_message: str = "Generated via Legend CLI MCP",
    clear_pending: bool = True,
) -> str:
    """Push pending artifacts to SDLC."""
    try:
        from legend_cli.sdlc_client import SDLCClient
        from legend_cli.engine_client import EngineClient

        if not ctx.pending_artifacts:
            return json.dumps({
                "status": "no_artifacts",
                "message": "No pending artifacts to push. Generate artifacts first using generate_model or other generation tools."
            })

        # Parse all Pure code through Engine
        with EngineClient() as engine:
            pure_codes = {}
            for artifact in ctx.pending_artifacts:
                if artifact.pure_code:
                    pure_codes[artifact.artifact_type] = artifact.pure_code

            entities = engine.parse_multiple_pure_codes(pure_codes)

        if not entities:
            return json.dumps({
                "status": "error",
                "message": "No valid entities extracted from Pure code"
            })

        # Push to SDLC
        with SDLCClient() as client:
            result = client.update_entities(
                project_id=project_id,
                workspace_id=workspace_id,
                entities=entities,
                message=commit_message
            )

        # Update context
        ctx.set_sdlc_context(project_id, workspace_id)

        pushed_count = len(entities)
        artifact_types = list(pure_codes.keys())

        if clear_pending:
            ctx.clear_pending_artifacts()

        return json.dumps({
            "status": "success",
            "project_id": project_id,
            "workspace_id": workspace_id,
            "commit_message": commit_message,
            "pushed_entities": [
                {
                    "path": e.get("path"),
                    "classifier": e.get("classifierPath")
                }
                for e in entities
            ],
            "artifact_types": artifact_types,
            "entity_count": pushed_count,
            "cleared_pending": clear_pending,
            "message": f"Successfully pushed {pushed_count} entities to workspace '{workspace_id}'"
        })

    except Exception as e:
        if "404" in str(e):
            raise WorkspaceNotFoundError(project_id, workspace_id)
        raise SDLCError(f"Failed to push artifacts: {str(e)}")
