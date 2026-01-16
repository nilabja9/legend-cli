"""Abstract base class for database introspection."""

from abc import ABC, abstractmethod
from typing import Optional, List

from .models import Database, Column, Table, Schema


class DatabaseIntrospector(ABC):
    """Abstract base class for database introspection.

    Subclasses must implement the abstract methods to provide
    database-specific introspection logic.
    """

    # Override in subclasses to exclude system schemas
    EXCLUDED_SCHEMAS: set = {'INFORMATION_SCHEMA'}

    @abstractmethod
    def connect(self, database: str):
        """Establish connection to the database.

        Args:
            database: Database name or identifier
        """
        pass

    @abstractmethod
    def close(self):
        """Close the database connection."""
        pass

    @abstractmethod
    def get_schemas(self, database: str) -> List[str]:
        """Get all user schemas in the database.

        Args:
            database: Database name

        Returns:
            List of schema names (excluding system schemas)
        """
        pass

    @abstractmethod
    def get_tables(self, database: str, schema: str, include_views: bool = True) -> List[str]:
        """Get all tables (and optionally views) in a schema.

        Args:
            database: Database name
            schema: Schema name
            include_views: Whether to include views

        Returns:
            List of table names
        """
        pass

    @abstractmethod
    def get_columns(self, database: str, schema: str, table: str) -> List[Column]:
        """Get all columns for a table.

        Args:
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            List of Column objects
        """
        pass

    @abstractmethod
    def get_primary_keys(self, database: str, schema: str, table: str) -> List[str]:
        """Get primary key columns for a table.

        Args:
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            List of primary key column names
        """
        pass

    def introspect_database(
        self,
        database: str,
        schema_filter: Optional[str] = None,
        detect_relationships: bool = True
    ) -> Database:
        """Introspect an entire database and return its structure.

        This method provides a common implementation across all databases
        and uses the abstract methods for database-specific operations.

        Args:
            database: Database name
            schema_filter: Optional schema name to filter to
            detect_relationships: Whether to detect FK relationships

        Returns:
            Database object containing the full schema structure
        """
        from .relationship import RelationshipDetector

        db = Database(name=database)
        schemas = self.get_schemas(database)

        if schema_filter:
            schemas = [s for s in schemas if s == schema_filter]

        for schema_name in schemas:
            schema = Schema(name=schema_name)
            table_names = self.get_tables(database, schema_name)

            for table_name in table_names:
                columns = self.get_columns(database, schema_name, table_name)
                pks = self.get_primary_keys(database, schema_name, table_name)

                table = Table(
                    name=table_name,
                    schema=schema_name,
                    columns=columns,
                    primary_key_columns=pks
                )
                schema.tables.append(table)

            if schema.tables:
                db.schemas.append(schema)

        if detect_relationships:
            detector = RelationshipDetector(db)
            detector.detect_relationships()

        return db

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
