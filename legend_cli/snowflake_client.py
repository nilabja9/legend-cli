"""Snowflake client for schema introspection."""

import os
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field


@dataclass
class Column:
    """Represents a database column."""
    name: str
    data_type: str
    is_nullable: bool = True

    def to_pure_type(self) -> str:
        """Convert Snowflake type to Pure column type."""
        type_upper = self.data_type.upper()

        if "VARCHAR" in type_upper or "TEXT" in type_upper or "STRING" in type_upper or "CHAR" in type_upper:
            # Extract length if present
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
            return "BOOLEAN"
        elif "DATE" in type_upper and "TIME" not in type_upper:
            return "DATE"
        elif "TIMESTAMP" in type_upper or "DATETIME" in type_upper:
            return "TIMESTAMP"
        elif "TIME" in type_upper:
            return "TIME"
        else:
            return "VARCHAR(256)"

    def to_pure_property_type(self) -> str:
        """Convert Snowflake type to Pure property type."""
        type_upper = self.data_type.upper()

        if "VARCHAR" in type_upper or "TEXT" in type_upper or "STRING" in type_upper or "CHAR" in type_upper:
            return "String"
        elif "INT" in type_upper:
            return "Integer"
        elif "NUMBER" in type_upper or "NUMERIC" in type_upper:
            if "," in str(self.data_type):  # Has decimal places
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
class Table:
    """Represents a database table."""
    name: str
    schema: str
    columns: List[Column] = field(default_factory=list)

    def get_class_name(self) -> str:
        """Convert table name to class name (PascalCase)."""
        # Convert SNAKE_CASE to PascalCase
        parts = self.name.lower().split('_')
        return ''.join(word.capitalize() for word in parts)

    def get_property_name(self, column_name: str) -> str:
        """Convert column name to property name (camelCase)."""
        parts = column_name.lower().split('_')
        return parts[0] + ''.join(word.capitalize() for word in parts[1:])


@dataclass
class Schema:
    """Represents a database schema."""
    name: str
    tables: List[Table] = field(default_factory=list)


@dataclass
class Database:
    """Represents a Snowflake database."""
    name: str
    schemas: List[Schema] = field(default_factory=list)


class SnowflakeIntrospector:
    """Client for introspecting Snowflake schema."""

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Schemas to exclude from introspection
    EXCLUDED_SCHEMAS = {'INFORMATION_SCHEMA'}

    def get_schemas(self, database: str) -> List[str]:
        """Get all schemas in a database (excludes INFORMATION_SCHEMA)."""
        conn = self.connect(database)
        cursor = conn.cursor()
        try:
            cursor.execute("SHOW SCHEMAS")
            schemas = [row[1] for row in cursor.fetchall()]
            # Filter out internal schemas like INFORMATION_SCHEMA
            return [s for s in schemas if s not in self.EXCLUDED_SCHEMAS]
        finally:
            cursor.close()

    def get_tables(self, database: str, schema: str, include_views: bool = True) -> List[str]:
        """Get all tables (and optionally views) in a schema."""
        conn = self.connect(database)
        cursor = conn.cursor()
        try:
            tables = []
            # Get tables
            cursor.execute(f"SHOW TABLES IN {database}.{schema}")
            tables.extend([row[1] for row in cursor.fetchall()])

            # Also get views if requested (many shared databases use views)
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
                ))
            return columns
        finally:
            cursor.close()

    def introspect_database(self, database: str, schema_filter: Optional[str] = None) -> Database:
        """Introspect an entire database and return its structure."""
        db = Database(name=database)

        schemas = self.get_schemas(database)
        if schema_filter:
            schemas = [s for s in schemas if s == schema_filter]

        for schema_name in schemas:
            schema = Schema(name=schema_name)

            table_names = self.get_tables(database, schema_name)
            for table_name in table_names:
                columns = self.get_columns(database, schema_name, table_name)
                table = Table(name=table_name, schema=schema_name, columns=columns)
                schema.tables.append(table)

            if schema.tables:  # Only add schemas with tables
                db.schemas.append(schema)

        return db


class PureCodeGenerator:
    """Generates Pure code from introspected database schema."""

    def __init__(self, database: Database, package_prefix: str = "model"):
        self.database = database
        self.package_prefix = package_prefix

    def generate_store(self) -> str:
        """Generate Pure store definition."""
        lines = ["###Relational"]
        lines.append(f"Database {self.package_prefix}::store::{self.database.name}")
        lines.append("(")

        for schema in self.database.schemas:
            lines.append(f"  Schema {schema.name}")
            lines.append("  (")

            for table in schema.tables:
                lines.append(f"    Table {table.name}")
                lines.append("    (")

                col_lines = []
                for col in table.columns:
                    col_lines.append(f"      {col.name} {col.to_pure_type()}")

                lines.append(",\n".join(col_lines))
                lines.append("    )")

            lines.append("  )")

        lines.append(")")
        return "\n".join(lines)

    def generate_classes(self) -> str:
        """Generate Pure class definitions."""
        class_defs = []

        for schema in self.database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()
                lines = [f"Class {self.package_prefix}::domain::{class_name}"]
                lines.append("{")

                for col in table.columns:
                    prop_name = table.get_property_name(col.name)
                    prop_type = col.to_pure_property_type()
                    multiplicity = "[0..1]" if col.is_nullable else "[1]"
                    lines.append(f"  {prop_name}: {prop_type}{multiplicity};")

                lines.append("}")
                class_defs.append("\n".join(lines))

        return "\n\n".join(class_defs)

    def generate_connection(
        self,
        account: str,
        warehouse: str,
        role: str = "ACCOUNTADMIN",
        region: str = "us-east-1",
        auth_type: str = "public_key",
        username: Optional[str] = None,
    ) -> str:
        """Generate Pure connection definition."""
        lines = ["###Connection"]
        lines.append(f"RelationalDatabaseConnection {self.package_prefix}::connection::{self.database.name}Connection")
        lines.append("{")
        lines.append(f"  store: {self.package_prefix}::store::{self.database.name};")
        lines.append("  type: Snowflake;")
        lines.append("  specification: Snowflake")
        lines.append("  {")
        lines.append(f"    name: '{self.database.name}';")
        lines.append(f"    account: '{account}';")
        lines.append(f"    warehouse: '{warehouse}';")
        lines.append(f"    region: '{region}';")
        lines.append(f"    role: '{role}';")
        lines.append("  };")

        if auth_type == "public_key":
            lines.append("  auth: SnowflakePublic")
            lines.append("  {")
            lines.append(f"    publicUserName: '{username or 'LEGEND_USER'}';")
            lines.append("    privateKeyVaultReference: 'SNOWFLAKE_PRIVATE_KEY';")
            lines.append("    passPhraseVaultReference: 'SNOWFLAKE_PASSPHRASE';")
            lines.append("  };")
        else:
            lines.append("  auth: UsernamePassword")
            lines.append("  {")
            lines.append(f"    username: '{username or 'LEGEND_USER'}';")
            lines.append("    passwordVaultReference: 'SNOWFLAKE_PASSWORD';")
            lines.append("  };")

        lines.append("}")
        return "\n".join(lines)

    def generate_mapping(self) -> str:
        """Generate Pure mapping definition."""
        lines = ["###Mapping"]
        lines.append(f"Mapping {self.package_prefix}::mapping::{self.database.name}Mapping")
        lines.append("(")

        mapping_blocks = []
        for schema in self.database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()
                class_path = f"{self.package_prefix}::domain::{class_name}"
                store_path = f"{self.package_prefix}::store::{self.database.name}"

                block_lines = [f"  {class_path}: Relational"]
                block_lines.append("  {")

                # Primary key - use first column as default
                if table.columns:
                    pk_col = table.columns[0].name
                    block_lines.append("    ~primaryKey")
                    block_lines.append("    (")
                    block_lines.append(f"      [{store_path}]{schema.name}.{table.name}.{pk_col}")
                    block_lines.append("    )")

                block_lines.append(f"    ~mainTable [{store_path}]{schema.name}.{table.name}")

                # Property mappings
                prop_mappings = []
                for col in table.columns:
                    prop_name = table.get_property_name(col.name)
                    prop_mappings.append(
                        f"    {prop_name}: [{store_path}]{schema.name}.{table.name}.{col.name}"
                    )

                block_lines.append(",\n".join(prop_mappings))
                block_lines.append("  }")

                mapping_blocks.append("\n".join(block_lines))

        lines.append("\n".join(mapping_blocks))
        lines.append(")")
        return "\n".join(lines)

    def generate_runtime(self) -> str:
        """Generate Pure runtime definition."""
        lines = ["###Runtime"]
        lines.append(f"Runtime {self.package_prefix}::runtime::{self.database.name}Runtime")
        lines.append("{")
        lines.append(f"  mappings:")
        lines.append("  [")
        lines.append(f"    {self.package_prefix}::mapping::{self.database.name}Mapping")
        lines.append("  ];")
        lines.append("  connections:")
        lines.append("  [")
        lines.append(f"    {self.package_prefix}::store::{self.database.name}:")
        lines.append("    [")
        lines.append(f"      connection: {self.package_prefix}::connection::{self.database.name}Connection")
        lines.append("    ]")
        lines.append("  ];")
        lines.append("}")
        return "\n".join(lines)

    def generate_all(
        self,
        account: str,
        warehouse: str,
        role: str = "ACCOUNTADMIN",
        region: str = "us-east-1",
        username: Optional[str] = None,
    ) -> Dict[str, str]:
        """Generate all Pure code artifacts."""
        return {
            "store": self.generate_store(),
            "classes": self.generate_classes(),
            "connection": self.generate_connection(account, warehouse, role, region, "public_key", username),
            "mapping": self.generate_mapping(),
            "runtime": self.generate_runtime(),
        }
