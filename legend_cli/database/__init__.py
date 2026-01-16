"""Database introspection module for legend-cli.

This module provides database-agnostic introspection capabilities
with specific implementations for Snowflake and DuckDB.
"""

from .models import Column, Table, Schema, Database, Relationship
from .base import DatabaseIntrospector
from .relationship import RelationshipDetector
from .type_mappers import TypeMapper, SnowflakeTypeMapper, DuckDBTypeMapper
from .snowflake import SnowflakeIntrospector
from .duckdb import DuckDBIntrospector

__all__ = [
    # Data models
    "Column",
    "Table",
    "Schema",
    "Database",
    "Relationship",
    # Base classes
    "DatabaseIntrospector",
    "RelationshipDetector",
    # Type mappers
    "TypeMapper",
    "SnowflakeTypeMapper",
    "DuckDBTypeMapper",
    # Introspectors
    "SnowflakeIntrospector",
    "DuckDBIntrospector",
]
