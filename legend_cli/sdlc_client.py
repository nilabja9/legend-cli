"""Legend SDLC API client."""

import httpx
import time
from typing import Optional, List, Dict, Any, Callable, TypeVar
from .config import settings
from .models import Project, Workspace, Entity

T = TypeVar('T')


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
        result = response.json()

        # Return structured result with response data
        return {
            "response": result,
            "status_code": response.status_code,
            "entity_count": len(entities),
            "revision": result.get("revision") if isinstance(result, dict) else None,
        }

    def verify_entities_exist(
        self,
        project_id: str,
        workspace_id: str,
        entity_paths: List[str],
    ) -> Dict[str, Any]:
        """Verify entities exist in workspace after push.

        Args:
            project_id: Project ID
            workspace_id: Workspace ID
            entity_paths: List of entity paths to verify

        Returns:
            Dict with:
                - all_found: bool indicating if all entities were found
                - found: list of entity paths that were found
                - missing: list of entity paths that were not found
        """
        existing = self.list_entities(project_id, workspace_id)
        existing_paths = {e.get("path") for e in existing}

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

    def _retry_with_backoff(
        self,
        operation: Callable[[], T],
        max_retries: int = 3,
        initial_delay: float = 1.0,
        retryable_status_codes: tuple = (429, 500, 502, 503, 504),
    ) -> T:
        """Execute operation with exponential backoff for transient failures.

        Args:
            operation: Callable to execute
            max_retries: Maximum number of retry attempts (default: 3)
            initial_delay: Initial delay in seconds (default: 1.0)
            retryable_status_codes: HTTP status codes to retry on

        Returns:
            Result of the operation

        Raises:
            Last exception if all retries fail
        """
        last_exception = None
        delay = initial_delay

        for attempt in range(max_retries + 1):
            try:
                return operation()
            except httpx.HTTPStatusError as e:
                if e.response.status_code not in retryable_status_codes:
                    raise
                last_exception = e
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_exception = e

            if attempt < max_retries:
                time.sleep(delay)
                delay *= 2  # Exponential backoff

        if last_exception:
            raise last_exception
        raise RuntimeError("Retry operation failed without exception")

    def update_entities_with_retry(
        self,
        project_id: str,
        workspace_id: str,
        entities: List[Dict[str, Any]],
        message: str = "Updated via legend-cli",
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """Update multiple entities with retry logic for transient failures."""
        def do_update():
            return self.update_entities(project_id, workspace_id, entities, message)

        return self._retry_with_backoff(do_update, max_retries=max_retries)

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
