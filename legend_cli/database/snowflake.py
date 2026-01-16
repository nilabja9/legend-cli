"""Snowflake database introspector."""

import os
from typing import Optional, List

from .base import DatabaseIntrospector
from .models import Column
from .type_mappers import SnowflakeTypeMapper


class SnowflakeIntrospector(DatabaseIntrospector):
    """Client for introspecting Snowflake schema."""

    EXCLUDED_SCHEMAS = {'INFORMATION_SCHEMA'}

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
    ):
        self.account = account or os.environ.get("SNOWFLAKE_ACCOUNT")
        self.user = user or os.environ.get("SNOWFLAKE_USER")
        self.password = password or os.environ.get("SNOWFLAKE_PASSWORD")
        self.warehouse = warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE")
        self.role = role or os.environ.get("SNOWFLAKE_ROLE")
        self._connection = None
        self._type_mapper = SnowflakeTypeMapper()

    def connect(self, database: str):
        """Connect to Snowflake."""
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError(
                "snowflake-connector-python is required. "
                "Install it with: pip install snowflake-connector-python"
            )

        self._connection = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=database,
            role=self.role,
        )
        return self._connection

    def close(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def get_schemas(self, database: str) -> List[str]:
        """Get all schemas in a database (excludes INFORMATION_SCHEMA)."""
        conn = self.connect(database)
        cursor = conn.cursor()
        try:
            cursor.execute("SHOW SCHEMAS")
            schemas = [row[1] for row in cursor.fetchall()]
            return [s for s in schemas if s not in self.EXCLUDED_SCHEMAS]
        finally:
            cursor.close()

    def get_tables(self, database: str, schema: str, include_views: bool = True) -> List[str]:
        """Get all tables (and optionally views) in a schema."""
        conn = self.connect(database)
        cursor = conn.cursor()
        try:
            tables = []
            cursor.execute(f"SHOW TABLES IN {database}.{schema}")
            tables.extend([row[1] for row in cursor.fetchall()])

            if include_views:
                cursor.execute(f"SHOW VIEWS IN {database}.{schema}")
                tables.extend([row[1] for row in cursor.fetchall()])

            return tables
        finally:
            cursor.close()

    def get_columns(self, database: str, schema: str, table: str) -> List[Column]:
        """Get all columns in a table."""
        conn = self.connect(database)
        cursor = conn.cursor()
        try:
            cursor.execute(f"DESCRIBE TABLE {database}.{schema}.{table}")
            columns = []
            for row in cursor.fetchall():
                col_name = row[0]
                col_type = row[1]
                is_nullable = row[3] == 'Y' if len(row) > 3 else True
                columns.append(Column(
                    name=col_name,
                    data_type=col_type,
                    is_nullable=is_nullable,
                    _type_mapper=self._type_mapper,
                ))
            return columns
        finally:
            cursor.close()

    def get_primary_keys(self, database: str, schema: str, table: str) -> List[str]:
        """Try to get primary key columns for a table."""
        conn = self.connect(database)
        cursor = conn.cursor()
        try:
            # Try to get PK from constraints
            cursor.execute(f"""
                SELECT COLUMN_NAME
                FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE tc.TABLE_SCHEMA = '{schema}'
                  AND tc.TABLE_NAME = '{table}'
                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """)
            pks = [row[0] for row in cursor.fetchall()]
            return pks
        except:
            return []
        finally:
            cursor.close()
