"""Mock SDLC client for testing."""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field


@dataclass
class MockProject:
    """Mock project data."""
    project_id: str
    name: str
    description: str = ""
    group_id: str = "org.demo.legend"
    artifact_id: str = ""


@dataclass
class MockWorkspace:
    """Mock workspace data."""
    workspace_id: str
    project_id: str
    workspace_type: str = "USER"


class MockSDLCClient:
    """Mock SDLCClient for testing without a real SDLC server.

    Provides in-memory storage for projects, workspaces, and entities,
    with call tracking for assertions.
    """

    def __init__(self):
        self._projects: Dict[str, MockProject] = {}
        self._workspaces: Dict[str, Dict[str, MockWorkspace]] = {}  # project_id -> {workspace_id -> workspace}
        self._entities: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}  # project_id -> {workspace_id -> entities}
        self._call_history: List[Dict[str, Any]] = []

    def _record_call(self, method: str, **kwargs):
        """Record a method call for later assertions."""
        self._call_history.append({"method": method, **kwargs})

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects."""
        self._record_call("list_projects")
        return [
            {
                "projectId": p.project_id,
                "name": p.name,
                "description": p.description,
                "groupId": p.group_id,
                "artifactId": p.artifact_id,
            }
            for p in self._projects.values()
        ]

    def create_project(
        self,
        name: str,
        description: Optional[str] = None,
        group_id: str = "org.demo.legend",
        artifact_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new project."""
        self._record_call("create_project", name=name, description=description, group_id=group_id, artifact_id=artifact_id)

        # Generate project ID from name
        project_id = f"PROJ-{name.upper().replace(' ', '-')}"

        if project_id in self._projects:
            raise Exception(f"409: Project '{name}' already exists")

        project = MockProject(
            project_id=project_id,
            name=name,
            description=description or "",
            group_id=group_id,
            artifact_id=artifact_id or name.lower().replace(" ", "-"),
        )
        self._projects[project_id] = project
        self._workspaces[project_id] = {}
        self._entities[project_id] = {}

        return {
            "projectId": project.project_id,
            "name": project.name,
            "description": project.description,
            "groupId": project.group_id,
            "artifactId": project.artifact_id,
        }

    def list_workspaces(self, project_id: str) -> List[Dict[str, Any]]:
        """List workspaces in a project."""
        self._record_call("list_workspaces", project_id=project_id)

        if project_id not in self._projects:
            raise Exception(f"404: Project '{project_id}' not found")

        workspaces = self._workspaces.get(project_id, {})
        return [
            {
                "workspaceId": w.workspace_id,
                "type": w.workspace_type,
            }
            for w in workspaces.values()
        ]

    def create_workspace(self, project_id: str, workspace_id: str) -> Dict[str, Any]:
        """Create a new workspace."""
        self._record_call("create_workspace", project_id=project_id, workspace_id=workspace_id)

        if project_id not in self._projects:
            raise Exception(f"404: Project '{project_id}' not found")

        if project_id in self._workspaces and workspace_id in self._workspaces[project_id]:
            raise Exception(f"409: Workspace '{workspace_id}' already exists")

        workspace = MockWorkspace(
            workspace_id=workspace_id,
            project_id=project_id,
        )

        if project_id not in self._workspaces:
            self._workspaces[project_id] = {}
        self._workspaces[project_id][workspace_id] = workspace

        if project_id not in self._entities:
            self._entities[project_id] = {}
        self._entities[project_id][workspace_id] = []

        return {
            "workspaceId": workspace.workspace_id,
            "type": workspace.workspace_type,
        }

    def list_entities(self, project_id: str, workspace_id: str) -> List[Dict[str, Any]]:
        """List entities in a workspace."""
        self._record_call("list_entities", project_id=project_id, workspace_id=workspace_id)

        if project_id not in self._projects:
            raise Exception(f"404: Project '{project_id}' not found")

        workspaces = self._workspaces.get(project_id, {})
        if workspace_id not in workspaces:
            raise Exception(f"404: Workspace '{workspace_id}' not found in project '{project_id}'")

        return self._entities.get(project_id, {}).get(workspace_id, [])

    def update_entities(
        self,
        project_id: str,
        workspace_id: str,
        entities: List[Dict[str, Any]],
        message: str,
    ) -> Dict[str, Any]:
        """Update entities in a workspace."""
        self._record_call(
            "update_entities",
            project_id=project_id,
            workspace_id=workspace_id,
            entities=entities,
            message=message,
        )

        if project_id not in self._projects:
            raise Exception(f"404: Project '{project_id}' not found")

        workspaces = self._workspaces.get(project_id, {})
        if workspace_id not in workspaces:
            raise Exception(f"404: Workspace '{workspace_id}' not found")

        # Store entities
        if project_id not in self._entities:
            self._entities[project_id] = {}
        if workspace_id not in self._entities[project_id]:
            self._entities[project_id][workspace_id] = []

        # Add or update entities by path
        existing_paths = {e.get("path"): i for i, e in enumerate(self._entities[project_id][workspace_id])}
        for entity in entities:
            path = entity.get("path")
            if path in existing_paths:
                self._entities[project_id][workspace_id][existing_paths[path]] = entity
            else:
                self._entities[project_id][workspace_id].append(entity)

        return {
            "revision": f"rev-{len(self._entities[project_id][workspace_id])}",
            "message": message,
        }

    def update_entities_with_retry(
        self,
        project_id: str,
        workspace_id: str,
        entities: List[Dict[str, Any]],
        message: str,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """Update entities with retry logic."""
        self._record_call(
            "update_entities_with_retry",
            project_id=project_id,
            workspace_id=workspace_id,
            entities=entities,
            message=message,
            max_retries=max_retries,
        )
        return self.update_entities(project_id, workspace_id, entities, message)

    def verify_entities_exist(
        self,
        project_id: str,
        workspace_id: str,
        entity_paths: List[str],
    ) -> Dict[str, Any]:
        """Verify that entities exist in a workspace."""
        self._record_call(
            "verify_entities_exist",
            project_id=project_id,
            workspace_id=workspace_id,
            entity_paths=entity_paths,
        )

        entities = self._entities.get(project_id, {}).get(workspace_id, [])
        existing_paths = {e.get("path") for e in entities}

        found = [p for p in entity_paths if p in existing_paths]
        missing = [p for p in entity_paths if p not in existing_paths]

        return {
            "all_found": len(missing) == 0,
            "found": found,
            "missing": missing,
            "total_expected": len(entity_paths),
            "total_found": len(found),
            "total_missing": len(missing),
        }

    def get_call_history(self) -> List[Dict[str, Any]]:
        """Get the history of method calls for assertions."""
        return self._call_history

    def clear_call_history(self):
        """Clear the call history."""
        self._call_history.clear()

    def add_project(self, project_id: str, name: str, **kwargs) -> MockProject:
        """Helper to pre-populate a project for testing."""
        project = MockProject(project_id=project_id, name=name, **kwargs)
        self._projects[project_id] = project
        self._workspaces[project_id] = {}
        self._entities[project_id] = {}
        return project

    def add_workspace(self, project_id: str, workspace_id: str) -> MockWorkspace:
        """Helper to pre-populate a workspace for testing."""
        if project_id not in self._workspaces:
            self._workspaces[project_id] = {}
        if project_id not in self._entities:
            self._entities[project_id] = {}

        workspace = MockWorkspace(workspace_id=workspace_id, project_id=project_id)
        self._workspaces[project_id][workspace_id] = workspace
        self._entities[project_id][workspace_id] = []
        return workspace

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
