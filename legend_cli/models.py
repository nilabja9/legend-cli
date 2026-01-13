"""Pydantic models for Legend entities."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum


class EntityType(str, Enum):
    """Types of Legend entities."""
    CLASS = "class"
    STORE = "store"
    CONNECTION = "connection"
    MAPPING = "mapping"
    FUNCTION = "function"
    RUNTIME = "runtime"


class Project(BaseModel):
    """Legend SDLC Project."""
    project_id: str = Field(alias="projectId")
    name: str
    description: Optional[str] = None
    tags: List[str] = []

    class Config:
        populate_by_name = True


class Workspace(BaseModel):
    """Legend SDLC Workspace."""
    workspace_id: str = Field(alias="workspaceId")
    project_id: str = Field(alias="projectId")

    class Config:
        populate_by_name = True


class Entity(BaseModel):
    """Legend entity (class, store, mapping, etc.)."""
    path: str
    classifier_path: str = Field(alias="classifierPath")
    content: Dict[str, Any]

    class Config:
        populate_by_name = True


class PureCode(BaseModel):
    """Generated Pure code."""
    entity_type: EntityType
    path: str
    code: str
    description: str


class GenerationRequest(BaseModel):
    """Request to generate Pure code from natural language."""
    entity_type: EntityType
    description: str
    package: str = "model::domain"
    additional_context: Optional[str] = None
