"""Database-specific type mapping strategies."""

from abc import ABC, abstractmethod


class TypeMapper(ABC):
    """Abstract base class for database type mapping."""

    @abstractmethod
    def to_pure_column_type(self, db_type: str) -> str:
        """Convert database type to Pure store column type."""
        pass

    @abstractmethod
    def to_pure_property_type(self, db_type: str) -> str:
        """Convert database type to Pure class property type."""
        pass


class SnowflakeTypeMapper(TypeMapper):
    """Type mapper for Snowflake database types."""

    def to_pure_column_type(self, db_type: str) -> str:
        """Convert Snowflake type to Pure column type."""
        type_upper = db_type.upper()

        if any(t in type_upper for t in ["VARCHAR", "TEXT", "STRING", "CHAR"]):
            if "(" in type_upper:
                return db_type.upper()
            return "VARCHAR(256)"
        elif "INT" in type_upper or "NUMBER" in type_upper or "NUMERIC" in type_upper:
            if "." in str(db_type) or "FLOAT" in type_upper or "DOUBLE" in type_upper or "DECIMAL" in type_upper:
                return "FLOAT"
            return "INTEGER"
        elif any(t in type_upper for t in ["FLOAT", "DOUBLE", "REAL"]):
            return "FLOAT"
        elif "BOOL" in type_upper:
            return "BIT"
        elif "DATE" in type_upper and "TIME" not in type_upper:
            return "DATE"
        elif "TIMESTAMP" in type_upper or "DATETIME" in type_upper:
            return "TIMESTAMP"
        elif "TIME" in type_upper:
            return "TIME"
        return "VARCHAR(256)"

    def to_pure_property_type(self, db_type: str) -> str:
        """Convert Snowflake type to Pure property type."""
        type_upper = db_type.upper()

        if any(t in type_upper for t in ["VARCHAR", "TEXT", "STRING", "CHAR"]):
            return "String"
        elif "INT" in type_upper:
            return "Integer"
        elif "NUMBER" in type_upper or "NUMERIC" in type_upper:
            if "," in str(db_type):
                return "Float"
            return "Integer"
        elif any(t in type_upper for t in ["FLOAT", "DOUBLE", "REAL", "DECIMAL"]):
            return "Float"
        elif "BOOL" in type_upper:
            return "Boolean"
        elif "DATE" in type_upper:
            return "Date"
        elif "TIMESTAMP" in type_upper or "DATETIME" in type_upper:
            return "DateTime"
        return "String"


class DuckDBTypeMapper(TypeMapper):
    """Type mapper for DuckDB database types."""

    def to_pure_column_type(self, db_type: str) -> str:
        """Convert DuckDB type to Pure column type."""
        type_upper = db_type.upper()

        # String types
        if any(t in type_upper for t in ["VARCHAR", "TEXT", "STRING", "CHAR", "UUID"]):
            if "(" in type_upper:
                return db_type.upper()
            return "VARCHAR(256)"

        # Integer types
        elif any(t in type_upper for t in ["BIGINT", "HUGEINT", "UBIGINT"]):
            return "BIGINT"
        elif any(t in type_upper for t in ["INTEGER", "INT4", "UINTEGER"]) or type_upper == "INT":
            return "INTEGER"
        elif any(t in type_upper for t in ["SMALLINT", "INT2", "TINYINT", "UTINYINT", "USMALLINT"]):
            return "SMALLINT"

        # Floating point types
        elif any(t in type_upper for t in ["DOUBLE", "FLOAT8", "NUMERIC", "DECIMAL"]):
            return "DOUBLE"
        elif any(t in type_upper for t in ["FLOAT", "FLOAT4", "REAL"]):
            return "FLOAT"

        # Boolean
        elif any(t in type_upper for t in ["BOOLEAN", "BOOL"]):
            return "BIT"

        # Date/Time types
        elif type_upper == "DATE":
            return "DATE"
        elif "TIMESTAMP" in type_upper:
            return "TIMESTAMP"
        elif type_upper == "TIME":
            return "TIME"
        elif "INTERVAL" in type_upper:
            return "VARCHAR(256)"  # Map intervals to string

        # Binary types
        elif "BLOB" in type_upper or "BYTEA" in type_upper:
            return "VARBINARY"

        # JSON type
        elif "JSON" in type_upper:
            return "VARCHAR(65535)"  # Map JSON to large varchar

        return "VARCHAR(256)"

    def to_pure_property_type(self, db_type: str) -> str:
        """Convert DuckDB type to Pure property type."""
        type_upper = db_type.upper()

        # String types
        if any(t in type_upper for t in ["VARCHAR", "TEXT", "STRING", "CHAR", "UUID", "JSON"]):
            return "String"

        # Integer types
        elif any(t in type_upper for t in ["BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT"]) or type_upper == "INT":
            return "Integer"
        elif any(t in type_upper for t in ["UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT"]):
            return "Integer"

        # Floating point types
        elif any(t in type_upper for t in ["DOUBLE", "FLOAT", "REAL", "NUMERIC", "DECIMAL"]):
            return "Float"

        # Boolean
        elif any(t in type_upper for t in ["BOOLEAN", "BOOL"]):
            return "Boolean"

        # Date/Time types
        elif type_upper == "DATE":
            return "Date"
        elif "TIMESTAMP" in type_upper:
            return "DateTime"
        elif type_upper == "TIME" or "INTERVAL" in type_upper:
            return "String"  # Time without date maps to String

        # Binary types
        elif "BLOB" in type_upper or "BYTEA" in type_upper:
            return "Binary"

        return "String"
