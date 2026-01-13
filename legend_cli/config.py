"""Configuration management for Legend CLI."""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
