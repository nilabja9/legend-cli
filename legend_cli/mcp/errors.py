"""Error types for Legend MCP server."""

from typing import Optional, Dict, Any


class MCPError(Exception):
    """Base exception for MCP errors."""

    def __init__(self, message: str, code: str = "MCP_ERROR", details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for MCP response."""
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class ConnectionError(MCPError):
    """Error connecting to database or external service."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="CONNECTION_ERROR", details=details)


class DatabaseError(MCPError):
    """Error during database operations."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="DATABASE_ERROR", details=details)


class IntrospectionError(MCPError):
    """Error during database introspection."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="INTROSPECTION_ERROR", details=details)


class GenerationError(MCPError):
    """Error during Pure code generation."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="GENERATION_ERROR", details=details)


class SDLCError(MCPError):
    """Error during SDLC operations."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="SDLC_ERROR", details=details)


class EngineError(MCPError):
    """Error during Engine operations (grammar parsing)."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="ENGINE_ERROR", details=details)


class EngineParseError(EngineError):
    """Specific error for Pure code parsing failures from Engine API.

    Raised when the Engine API returns a codeError in the response,
    indicating a syntax or parsing error in the Pure code.
    """

    def __init__(
        self,
        message: str,
        source_info: Optional[Dict[str, Any]] = None,
        raw_error: Optional[Dict[str, Any]] = None,
    ):
        details = {
            "source_info": source_info or {},
            "raw_error": raw_error or {},
        }
        super().__init__(message, details=details)
        self.code = "ENGINE_PARSE_ERROR"
        self.source_info = source_info or {}
        self.raw_error = raw_error or {}

    def get_formatted_location(self) -> str:
        """Return human-readable location string like 'Line X, columns Y-Z'."""
        if not self.source_info:
            return ""

        start_line = self.source_info.get("startLine", "?")
        start_col = self.source_info.get("startColumn", "?")
        end_line = self.source_info.get("endLine", "?")
        end_col = self.source_info.get("endColumn", "?")

        if start_line == end_line:
            return f"Line {start_line}, columns {start_col}-{end_col}"
        return f"Lines {start_line}-{end_line}"

    def get_user_friendly_message(self) -> str:
        """Return a user-friendly error message with location info."""
        location = self.get_formatted_location()
        if location:
            return f"{self.message} at {location}"
        return self.message


class ValidationError(MCPError):
    """Error during validation."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="VALIDATION_ERROR", details=details)


class EntityNotFoundError(MCPError):
    """Entity not found in SDLC."""

    def __init__(self, entity_path: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            f"Entity not found: {entity_path}",
            code="ENTITY_NOT_FOUND",
            details=details or {"entity_path": entity_path}
        )


class WorkspaceNotFoundError(MCPError):
    """Workspace not found in SDLC."""

    def __init__(self, project_id: str, workspace_id: str):
        super().__init__(
            f"Workspace not found: {workspace_id} in project {project_id}",
            code="WORKSPACE_NOT_FOUND",
            details={"project_id": project_id, "workspace_id": workspace_id}
        )


class ProjectNotFoundError(MCPError):
    """Project not found in SDLC."""

    def __init__(self, project_id: str):
        super().__init__(
            f"Project not found: {project_id}",
            code="PROJECT_NOT_FOUND",
            details={"project_id": project_id}
        )


class ModificationError(MCPError):
    """Error during model modification."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="MODIFICATION_ERROR", details=details)


class PartialPushError(MCPError):
    """Some entities failed to push to SDLC."""

    def __init__(
        self,
        message: str,
        succeeded: Optional[list] = None,
        failed: Optional[list] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        error_details = details or {}
        error_details["succeeded"] = succeeded or []
        error_details["failed"] = failed or []
        super().__init__(message, code="PARTIAL_PUSH_ERROR", details=error_details)
        self.succeeded = succeeded or []
        self.failed = failed or []
