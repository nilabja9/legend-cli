"""Legend SDLC API client."""

import httpx
from typing import Optional, List, Dict, Any
from .config import settings
from .models import Project, Workspace, Entity


class SDLCClient:
    """Client for Legend SDLC API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        pat: Optional[str] = None,
    ):
        self.base_url = (base_url or settings.legend_sdlc_url).rstrip("/")
        self.pat = pat or settings.legend_pat
        self._client: Optional[httpx.Client] = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            headers = {"Content-Type": "application/json"}
            if self.pat:
                headers["Authorization"] = f"Bearer {self.pat}"
            self._client = httpx.Client(
                base_url=self.base_url,
                headers=headers,
                timeout=30.0,
            )
        return self._client

    def close(self):
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Project operations
    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects."""
        response = self.client.get("/projects")
        response.raise_for_status()
        return response.json()

    def get_project(self, project_id: str) -> Dict[str, Any]:
        """Get project by ID."""
        response = self.client.get(f"/projects/{project_id}")
        response.raise_for_status()
        return response.json()

    def create_project(
        self,
        name: str,
        description: Optional[str] = None,
        group_id: str = "org.demo.legend",
        artifact_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new project."""
        if artifact_id is None:
            artifact_id = name.lower().replace(" ", "-").replace("_", "-")

        data = {
            "name": name,
            "description": description or f"Project: {name}",
            "groupId": group_id,
            "artifactId": artifact_id,
            "tags": ["legend"],
        }
        response = self.client.post("/projects", json=data)
        response.raise_for_status()
        return response.json()

    # Workspace operations
    def list_workspaces(self, project_id: str) -> List[Dict[str, Any]]:
        """List workspaces for a project."""
        response = self.client.get(f"/projects/{project_id}/workspaces")
        response.raise_for_status()
        return response.json()

    def get_workspace(self, project_id: str, workspace_id: str) -> Dict[str, Any]:
        """Get workspace details."""
        response = self.client.get(
            f"/projects/{project_id}/workspaces/{workspace_id}"
        )
        response.raise_for_status()
        return response.json()

    def create_workspace(self, project_id: str, workspace_id: str) -> Dict[str, Any]:
        """Create a new workspace."""
        response = self.client.post(
            f"/projects/{project_id}/workspaces/{workspace_id}"
        )
        response.raise_for_status()
        return response.json()

    # Entity operations
    def list_entities(self, project_id: str, workspace_id: str) -> List[Dict[str, Any]]:
        """List entities in a workspace."""
        response = self.client.get(
            f"/projects/{project_id}/workspaces/{workspace_id}/entities"
        )
        response.raise_for_status()
        return response.json()

    def get_entity(
        self, project_id: str, workspace_id: str, entity_path: str
    ) -> Dict[str, Any]:
        """Get entity by path."""
        response = self.client.get(
            f"/projects/{project_id}/workspaces/{workspace_id}/entities/{entity_path}"
        )
        response.raise_for_status()
        return response.json()

    def create_entity(
        self,
        project_id: str,
        workspace_id: str,
        path: str,
        classifier_path: str,
        content: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a new entity."""
        data = {
            "path": path,
            "classifierPath": classifier_path,
            "content": content,
        }
        response = self.client.post(
            f"/projects/{project_id}/workspaces/{workspace_id}/entities",
            json=data,
        )
        response.raise_for_status()
        return response.json()

    def update_entities(
        self,
        project_id: str,
        workspace_id: str,
        entities: List[Dict[str, Any]],
        message: str = "Updated via legend-cli",
    ) -> Dict[str, Any]:
        """Update multiple entities using entity changes API."""
        # Prepare entity changes
        entity_changes = []
        for entity in entities:
            entity_changes.append({
                "type": "CREATE",
                "entityPath": entity["path"],
                "classifierPath": entity["classifierPath"],
                "content": entity["content"],
            })

        data = {
            "message": message,
            "entityChanges": entity_changes,
        }
        response = self.client.post(
            f"/projects/{project_id}/workspaces/{workspace_id}/entityChanges",
            json=data,
        )
        response.raise_for_status()
        return response.json()

    def delete_entity(
        self, project_id: str, workspace_id: str, entity_path: str
    ) -> None:
        """Delete an entity."""
        response = self.client.delete(
            f"/projects/{project_id}/workspaces/{workspace_id}/entities/{entity_path}"
        )
        response.raise_for_status()

    # Utility methods
    def health_check(self) -> bool:
        """Check if SDLC server is healthy."""
        try:
            response = self.client.get("/info")
            return response.status_code == 200
        except Exception:
            return False
