"""DuckDB database introspector."""

from typing import Optional, List
from pathlib import Path

from .base import DatabaseIntrospector
from .models import Column, Database, Schema, Table
from .type_mappers import DuckDBTypeMapper


class DuckDBIntrospector(DatabaseIntrospector):
    """Client for introspecting DuckDB database schema."""

    EXCLUDED_SCHEMAS = {'information_schema', 'pg_catalog'}

    def __init__(
        self,
        database_path: Optional[str] = None,
        connection_string: Optional[str] = None,
        read_only: bool = True,
    ):
        """Initialize DuckDB introspector.

        Args:
            database_path: Path to .duckdb file (can be :memory: for in-memory)
            connection_string: Alternative connection string format
                               (e.g., duckdb:///path/to/db.duckdb)
            read_only: Open database in read-only mode (default True for introspection)
        """
        self.database_path = database_path
        self.connection_string = connection_string
        self.read_only = read_only
        self._connection = None
        self._type_mapper = DuckDBTypeMapper()
        self._database_name = self._extract_database_name()

    def _extract_database_name(self) -> str:
        """Extract database name from path or connection string."""
        if self.database_path:
            if self.database_path == ':memory:':
                return 'memory'
            path = Path(self.database_path)
            return path.stem  # filename without extension
        elif self.connection_string:
            # Parse connection string to extract database name
            # Format: duckdb:///path/to/file.duckdb
            if ':///' in self.connection_string:
                path_part = self.connection_string.split(':///')[-1]
                # Remove any query parameters
                if '?' in path_part:
                    path_part = path_part.split('?')[0]
                return Path(path_part).stem
        return 'duckdb_database'

    def get_database_name(self) -> str:
        """Get the database name for Pure code generation."""
        return self._database_name

    def connect(self, database: str = None):
        """Connect to DuckDB database.

        Args:
            database: Ignored for DuckDB (path is set in constructor)
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "duckdb is required. "
                "Install it with: pip install duckdb"
            )

        if self._connection is not None:
            return self._connection

        if self.database_path:
            self._connection = duckdb.connect(
                self.database_path,
                read_only=self.read_only
            )
        elif self.connection_string:
            # Parse connection string
            # Remove duckdb:/// prefix if present
            path = self.connection_string
            if path.startswith('duckdb:///'):
                path = path[10:]
            elif path.startswith('duckdb://'):
                path = path[9:]
            # Remove query parameters if any
            if '?' in path:
                path = path.split('?')[0]
            self._connection = duckdb.connect(path, read_only=self.read_only)
        else:
            # Default to in-memory database
            self._connection = duckdb.connect(':memory:')

        return self._connection

    def close(self):
        """Close the DuckDB connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def get_schemas(self, database: str = None) -> List[str]:
        """Get all user schemas in the database."""
        conn = self.connect()
        result = conn.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE catalog_name = current_database()
            ORDER BY schema_name
        """).fetchall()

        schemas = [row[0] for row in result]
        return [s for s in schemas if s.lower() not in self.EXCLUDED_SCHEMAS]

    def get_tables(self, database: str, schema: str, include_views: bool = True) -> List[str]:
        """Get all tables (and optionally views) in a schema."""
        conn = self.connect()

        table_types = ["'BASE TABLE'"]
        if include_views:
            table_types.append("'VIEW'")

        type_filter = ", ".join(table_types)

        result = conn.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_type IN ({type_filter})
            ORDER BY table_name
        """).fetchall()

        return [row[0] for row in result]

    def get_columns(self, database: str, schema: str, table: str) -> List[Column]:
        """Get all columns for a table."""
        conn = self.connect()

        result = conn.execute(f"""
            SELECT
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
            ORDER BY ordinal_position
        """).fetchall()

        columns = []
        for row in result:
            col = Column(
                name=row[0],
                data_type=row[1],
                is_nullable=(row[2] == 'YES'),
                _type_mapper=self._type_mapper,
            )
            columns.append(col)

        return columns

    def get_primary_keys(self, database: str, schema: str, table: str) -> List[str]:
        """Get primary key columns for a table.

        DuckDB stores constraint info in duckdb_constraints() table function.
        """
        conn = self.connect()

        try:
            # Try using duckdb_constraints table function
            result = conn.execute(f"""
                SELECT constraint_column_names
                FROM duckdb_constraints()
                WHERE schema_name = '{schema}'
                  AND table_name = '{table}'
                  AND constraint_type = 'PRIMARY KEY'
            """).fetchall()

            if result:
                # Result is a list of column names
                pk_columns = result[0][0]
                if isinstance(pk_columns, list):
                    return pk_columns
                return [pk_columns]
            return []
        except Exception:
            # Fallback: try PRAGMA approach
            try:
                result = conn.execute(f"PRAGMA table_info('{schema}.{table}')").fetchall()
                return [row[1] for row in result if row[5] == 1]  # pk column is index 5
            except Exception:
                return []

    def introspect_database(
        self,
        database: str = None,
        schema_filter: Optional[str] = None,
        detect_relationships: bool = True
    ) -> Database:
        """Introspect the DuckDB database.

        For DuckDB, the 'database' parameter is optional since the path
        is already specified in the constructor.

        Args:
            database: Optional database name (defaults to extracted name from path)
            schema_filter: Optional schema name to filter to
            detect_relationships: Whether to detect FK relationships

        Returns:
            Database object containing the full schema structure
        """
        from .relationship import RelationshipDetector

        db_name = database or self._database_name
        db = Database(name=db_name)

        schemas = self.get_schemas()
        if schema_filter:
            schemas = [s for s in schemas if s == schema_filter]

        # Default to 'main' schema if no schemas found (common in DuckDB)
        if not schemas:
            schemas = ['main']

        for schema_name in schemas:
            schema = Schema(name=schema_name)
            table_names = self.get_tables(db_name, schema_name)

            for table_name in table_names:
                columns = self.get_columns(db_name, schema_name, table_name)
                pks = self.get_primary_keys(db_name, schema_name, table_name)

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
