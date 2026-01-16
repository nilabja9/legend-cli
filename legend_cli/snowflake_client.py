"""Snowflake client for schema introspection with relationship detection.

DEPRECATED: This module is deprecated and will be removed in a future version.
Please use the new modular structure instead:

- Database introspection: legend_cli.database
  - SnowflakeIntrospector, DuckDBIntrospector
  - Column, Table, Schema, Database, Relationship
  - RelationshipDetector

- Pure code generation: legend_cli.pure
  - PureCodeGenerator
  - SnowflakeConnectionGenerator, DuckDBConnectionGenerator

Example migration:
    # Old way (deprecated)
    from legend_cli.snowflake_client import SnowflakeIntrospector, PureCodeGenerator

    # New way
    from legend_cli.database import SnowflakeIntrospector
    from legend_cli.pure import PureCodeGenerator
"""

import warnings

# Re-export for backward compatibility
from .database import (
    Column,
    Table,
    Schema,
    Database,
    Relationship,
    RelationshipDetector,
    SnowflakeIntrospector,
)
from .pure import PureCodeGenerator

# Issue deprecation warning on import
warnings.warn(
    "legend_cli.snowflake_client is deprecated. "
    "Use legend_cli.database and legend_cli.pure instead. "
    "See module docstring for migration guide.",
    DeprecationWarning,
    stacklevel=2
)

__all__ = [
    "Column",
    "Table",
    "Schema",
    "Database",
    "Relationship",
    "RelationshipDetector",
    "SnowflakeIntrospector",
    "PureCodeGenerator",
]
