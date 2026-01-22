"""Configuration management for Legend CLI."""

import os
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


def _find_env_file() -> Optional[str]:
    """Find .env file in multiple locations.

    Search order:
    1. Current working directory
    2. ~/.legend-cli/.env
    3. Package directory (where this file is located)
    """
    # Current directory
    if os.path.exists(".env"):
        return ".env"

    # User config directory
    user_env = Path.home() / ".legend-cli" / ".env"
    if user_env.exists():
        return str(user_env)

    # Package directory
    package_dir = Path(__file__).parent.parent
    package_env = package_dir / ".env"
    if package_env.exists():
        return str(package_env)

    return None


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Legend SDLC configuration
    legend_sdlc_url: str = Field(
        default="http://localhost:6900/sdlc/api",
        description="Legend SDLC API URL"
    )
    legend_pat: Optional[str] = Field(
        default=None,
        description="Legend Personal Access Token for authentication"
    )

    # Default project/workspace
    default_project_id: Optional[str] = Field(
        default=None,
        description="Default project ID to use"
    )
    default_workspace_id: str = Field(
        default="dev-workspace",
        description="Default workspace ID to use"
    )

    # Claude API configuration
    anthropic_api_key: Optional[str] = Field(
        default=None,
        description="Anthropic API key for Claude"
    )
    claude_model: str = Field(
        default="claude-sonnet-4-20250514",
        description="Claude model to use for code generation"
    )

    # MCP logging configuration
    mcp_logging_enabled: bool = Field(
        default=True,
        description="Enable database logging for MCP tool calls"
    )
    mcp_logging_db_path: Optional[str] = Field(
        default=None,
        description="Path to MCP logs database file (default: ~/.legend-cli/mcp_logs.db)"
    )
    mcp_logging_retention_days: int = Field(
        default=30,
        description="Number of days to retain MCP log entries"
    )
    mcp_logging_max_result_size: int = Field(
        default=10000,
        description="Maximum size of result/parameter strings to store in logs"
    )

    # CLI run logging configuration
    cli_logging_enabled: bool = Field(
        default=True,
        description="Enable database logging for CLI command runs"
    )
    cli_logging_db_path: Optional[str] = Field(
        default=None,
        description="Path to CLI runs database file (default: ~/.legend-cli/cli_runs.db)"
    )
    cli_logging_retention_days: int = Field(
        default=30,
        description="Number of days to retain CLI run log entries"
    )

    class Config:
        env_file = _find_env_file()
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
