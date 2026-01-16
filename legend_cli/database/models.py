"""Database data models for schema introspection."""

from typing import Optional, List, TYPE_CHECKING
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from .type_mappers import TypeMapper


@dataclass
class Column:
    """Represents a database column."""
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    _type_mapper: Optional['TypeMapper'] = field(default=None, repr=False)

    def to_pure_type(self) -> str:
        """Convert database type to Pure column type."""
        if self._type_mapper:
            return self._type_mapper.to_pure_column_type(self.data_type)
        # Default Snowflake-compatible mapping for backward compatibility
        return self._default_pure_type()

    def to_pure_property_type(self) -> str:
        """Convert database type to Pure property type."""
        if self._type_mapper:
            return self._type_mapper.to_pure_property_type(self.data_type)
        return self._default_property_type()

    def _default_pure_type(self) -> str:
        """Default Pure column type mapping (Snowflake-compatible)."""
        type_upper = self.data_type.upper()

        if "VARCHAR" in type_upper or "TEXT" in type_upper or "STRING" in type_upper or "CHAR" in type_upper:
            if "(" in type_upper:
                return self.data_type.upper()
            return "VARCHAR(256)"
        elif "INT" in type_upper or "NUMBER" in type_upper or "NUMERIC" in type_upper:
            if "." in str(self.data_type) or "FLOAT" in type_upper or "DOUBLE" in type_upper or "DECIMAL" in type_upper:
                return "FLOAT"
            return "INTEGER"
        elif "FLOAT" in type_upper or "DOUBLE" in type_upper or "REAL" in type_upper:
            return "FLOAT"
        elif "BOOL" in type_upper:
            return "BIT"
        elif "DATE" in type_upper and "TIME" not in type_upper:
            return "DATE"
        elif "TIMESTAMP" in type_upper or "DATETIME" in type_upper:
            return "TIMESTAMP"
        elif "TIME" in type_upper:
            return "TIME"
        else:
            return "VARCHAR(256)"

    def _default_property_type(self) -> str:
        """Default Pure property type mapping (Snowflake-compatible)."""
        type_upper = self.data_type.upper()

        if "VARCHAR" in type_upper or "TEXT" in type_upper or "STRING" in type_upper or "CHAR" in type_upper:
            return "String"
        elif "INT" in type_upper:
            return "Integer"
        elif "NUMBER" in type_upper or "NUMERIC" in type_upper:
            if "," in str(self.data_type):
                return "Float"
            return "Integer"
        elif "FLOAT" in type_upper or "DOUBLE" in type_upper or "REAL" in type_upper or "DECIMAL" in type_upper:
            return "Float"
        elif "BOOL" in type_upper:
            return "Boolean"
        elif "DATE" in type_upper:
            return "Date"
        elif "TIMESTAMP" in type_upper or "DATETIME" in type_upper:
            return "DateTime"
        else:
            return "String"


@dataclass
class Relationship:
    """Represents a relationship between two tables."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str  # 'many_to_one', 'one_to_many', 'one_to_one'
    property_name: str  # Name for the association property

    def get_reverse_property_name(self, source_table_name: str) -> str:
        """Get property name for reverse relationship."""
        # Convert TABLE_NAME to tableNames (plural, camelCase)
        parts = source_table_name.lower().split('_')
        name = parts[0] + ''.join(word.capitalize() for word in parts[1:])
        # Make it plural for one_to_many
        if not name.endswith('s'):
            name += 's'
        return name


@dataclass
class Table:
    """Represents a database table."""
    name: str
    schema: str
    columns: List[Column] = field(default_factory=list)
    primary_key_columns: List[str] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)

    def get_class_name(self) -> str:
        """Convert table name to class name (PascalCase)."""
        parts = self.name.lower().split('_')
        return ''.join(word.capitalize() for word in parts)

    def get_property_name(self, column_name: str) -> str:
        """Convert column name to property name (camelCase)."""
        parts = column_name.lower().split('_')
        return parts[0] + ''.join(word.capitalize() for word in parts[1:])

    def get_potential_key_columns(self) -> List[str]:
        """Get columns that could be primary/foreign keys."""
        key_patterns = ['_ID', '_KEY', '_CODE', '_NUM', '_NO']
        return [col.name for col in self.columns
                if any(col.name.upper().endswith(p) for p in key_patterns)
                or col.name.upper() in ('ID', 'KEY', 'CODE')]


@dataclass
class Schema:
    """Represents a database schema."""
    name: str
    tables: List[Table] = field(default_factory=list)


@dataclass
class Database:
    """Represents a database."""
    name: str
    schemas: List[Schema] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)

    def get_all_tables(self) -> List[Table]:
        """Get all tables across all schemas."""
        tables = []
        for schema in self.schemas:
            tables.extend(schema.tables)
        return tables

    def get_table_by_name(self, table_name: str) -> Optional[Table]:
        """Find a table by name."""
        for schema in self.schemas:
            for table in schema.tables:
                if table.name == table_name:
                    return table
        return None
