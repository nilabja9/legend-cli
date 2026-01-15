"""Snowflake client for schema introspection with relationship detection."""

import os
import re
from typing import Optional, List, Dict, Any, Set, Tuple
from dataclasses import dataclass, field


@dataclass
class Column:
    """Represents a database column."""
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False

    def to_pure_type(self) -> str:
        """Convert Snowflake type to Pure column type."""
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
            return "BIT"  # Legend Engine uses BIT for boolean columns
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
    """Represents a Snowflake database."""
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


class RelationshipDetector:
    """Detects relationships between tables based on schema analysis."""

    # Common patterns for identifying foreign key columns
    FK_PATTERNS = [
        # Pattern: column ends with _ID and matches TABLE_NAME + _ID
        (r'^(.+)_ID$', '{}_INDEX', 'ID'),
        (r'^(.+)_ID$', '{}', 'ID'),
        (r'^(.+)_ID$', '{}_ID', 'ID'),
        # Pattern: column ends with _KEY
        (r'^(.+)_KEY$', '{}', 'KEY'),
        # Pattern: column ends with _CODE
        (r'^(.+)_CODE$', '{}_INDEX', 'CODE'),
        (r'^(.+)_CODE$', '{}', 'CODE'),
        # Pattern: column is CIK (SEC-specific)
        (r'^CIK$', 'SEC_CIK_INDEX', 'CIK'),
        # Pattern: column is ADSH (SEC-specific)
        (r'^ADSH$', 'SEC_REPORT_INDEX', 'ADSH'),
        # Pattern: GEO_ID
        (r'^GEO_ID$', 'GEOGRAPHY_INDEX', 'GEO_ID'),
    ]

    # Index tables that are typically referenced
    INDEX_TABLE_PATTERNS = ['_INDEX', '_MASTER', '_DIM', '_LOOKUP', '_REF']

    def __init__(self, database: Database):
        self.database = database
        self.table_names = {t.name for t in database.get_all_tables()}
        self.table_columns = {t.name: {c.name for c in t.columns}
                             for t in database.get_all_tables()}

    def detect_relationships(self) -> List[Relationship]:
        """Detect all relationships in the database."""
        relationships = []

        for table in self.database.get_all_tables():
            table_relationships = self._detect_table_relationships(table)
            relationships.extend(table_relationships)
            table.relationships = table_relationships

        # Remove duplicates and self-references
        unique_relationships = self._deduplicate_relationships(relationships)
        self.database.relationships = unique_relationships

        return unique_relationships

    def _detect_table_relationships(self, table: Table) -> List[Relationship]:
        """Detect relationships for a single table."""
        relationships = []

        for column in table.columns:
            # Skip if column is likely the table's own primary key
            if self._is_own_primary_key(table, column):
                continue

            # Try to find a target table for this column
            target = self._find_target_table(table.name, column.name)
            if target:
                target_table, target_column = target

                # Don't create self-references
                if target_table == table.name:
                    continue

                # Determine relationship type and property name
                rel_type, prop_name = self._determine_relationship_type(
                    table.name, column.name, target_table
                )

                relationship = Relationship(
                    source_table=table.name,
                    source_column=column.name,
                    target_table=target_table,
                    target_column=target_column,
                    relationship_type=rel_type,
                    property_name=prop_name,
                )
                relationships.append(relationship)

        return relationships

    def _is_own_primary_key(self, table: Table, column: Column) -> bool:
        """Check if column is the table's own primary key."""
        # If column name matches table name pattern, it's likely own PK
        table_base = table.name.replace('_INDEX', '').replace('_MASTER', '')
        col_base = column.name.replace('_ID', '').replace('_KEY', '').replace('_CODE', '')
        return table_base == col_base

    def _find_target_table(self, source_table: str, column_name: str) -> Optional[Tuple[str, str]]:
        """Find the target table for a potential foreign key column."""
        col_upper = column_name.upper()

        # Try pattern matching
        for pattern, table_format, pk_suffix in self.FK_PATTERNS:
            match = re.match(pattern, col_upper)
            if match:
                # Extract the base name from the column
                if match.groups():
                    base_name = match.group(1)
                else:
                    base_name = col_upper.replace('_ID', '').replace('_KEY', '').replace('_CODE', '')

                # Try to find matching table
                potential_table = table_format.format(base_name)
                if potential_table in self.table_names:
                    # Find the matching column in target table
                    target_col = self._find_matching_column(potential_table, column_name)
                    if target_col:
                        return (potential_table, target_col)

        # Try direct column name matching with index tables
        for table_name in self.table_names:
            if any(p in table_name for p in self.INDEX_TABLE_PATTERNS):
                if column_name in self.table_columns.get(table_name, set()):
                    return (table_name, column_name)

        return None

    def _find_matching_column(self, table_name: str, column_name: str) -> Optional[str]:
        """Find a matching column in the target table."""
        target_columns = self.table_columns.get(table_name, set())

        # Exact match
        if column_name in target_columns:
            return column_name

        # Try common variations
        variations = [
            column_name,
            column_name.replace('_ID', ''),
            'ID',
            column_name.split('_')[0] + '_ID',
        ]

        for var in variations:
            if var in target_columns:
                return var

        # Return first ID-like column as fallback
        for col in target_columns:
            if col.endswith('_ID') or col == 'ID':
                return col

        return None

    def _determine_relationship_type(
        self, source_table: str, source_column: str, target_table: str
    ) -> Tuple[str, str]:
        """Determine relationship type and property name."""
        # If target is an index/master table, it's many_to_one
        is_index_table = any(p in target_table for p in self.INDEX_TABLE_PATTERNS)

        if is_index_table:
            rel_type = 'many_to_one'
            # Property name from target table (e.g., COMPANY_INDEX -> company)
            prop_name = self._get_property_name_from_table(target_table)
        else:
            rel_type = 'many_to_one'
            prop_name = self._get_property_name_from_column(source_column)

        return rel_type, prop_name

    def _get_property_name_from_table(self, table_name: str) -> str:
        """Get association property name from table name."""
        # Remove common suffixes
        name = table_name
        for suffix in ['_INDEX', '_MASTER', '_DIM', '_LOOKUP', '_REF']:
            name = name.replace(suffix, '')

        # Convert to camelCase
        parts = name.lower().split('_')
        return parts[0] + ''.join(word.capitalize() for word in parts[1:])

    def _get_property_name_from_column(self, column_name: str) -> str:
        """Get association property name from column name."""
        # Remove _ID, _KEY suffixes
        name = column_name
        for suffix in ['_ID', '_KEY', '_CODE']:
            name = name.replace(suffix, '')

        # Convert to camelCase
        parts = name.lower().split('_')
        return parts[0] + ''.join(word.capitalize() for word in parts[1:])

    def _deduplicate_relationships(self, relationships: List[Relationship]) -> List[Relationship]:
        """Remove duplicate relationships."""
        seen = set()
        unique = []

        for rel in relationships:
            key = (rel.source_table, rel.source_column, rel.target_table, rel.target_column)
            if key not in seen:
                seen.add(key)
                unique.append(rel)

        return unique


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

    def introspect_database(
        self,
        database: str,
        schema_filter: Optional[str] = None,
        detect_relationships: bool = True
    ) -> Database:
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

        # Detect relationships
        if detect_relationships:
            detector = RelationshipDetector(db)
            detector.detect_relationships()

        return db


class PureCodeGenerator:
    """Generates Pure code from introspected database schema."""

    def __init__(self, database: Database, package_prefix: str = "model"):
        self.database = database
        self.package_prefix = package_prefix
        # Build lookup for table -> class name
        self.table_to_class = {}
        for table in database.get_all_tables():
            self.table_to_class[table.name] = table.get_class_name()

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

    def generate_classes(self, docs: Optional[Dict[str, Any]] = None) -> str:
        """Generate Pure class definitions (without association properties - those come from Association elements).

        Args:
            docs: Optional dictionary mapping class names to ClassDocumentation objects.
                  If provided, doc.doc tagged values will be added to classes and properties.
        """
        class_defs = []

        for schema in self.database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()

                # Get class documentation if available
                class_doc = ""
                attr_docs = {}
                if docs and class_name in docs:
                    class_doc_obj = docs[class_name]
                    class_doc = getattr(class_doc_obj, 'class_doc', '') or ''
                    attr_docs = {k: getattr(v, 'doc', '') for k, v in getattr(class_doc_obj, 'attributes', {}).items()}

                # Class declaration with optional doc.doc
                if class_doc:
                    escaped_doc = self._escape_doc_string(class_doc)
                    lines = [f"Class {{meta::pure::profiles::doc.doc = '{escaped_doc}'}} {self.package_prefix}::domain::{class_name}"]
                else:
                    lines = [f"Class {self.package_prefix}::domain::{class_name}"]

                lines.append("{")

                # Regular properties only (no association properties)
                for col in table.columns:
                    prop_name = table.get_property_name(col.name)
                    prop_type = col.to_pure_property_type()
                    multiplicity = "[0..1]" if col.is_nullable else "[1]"

                    # Property with optional doc.doc
                    prop_doc = attr_docs.get(prop_name, '')
                    if prop_doc:
                        escaped_prop_doc = self._escape_doc_string(prop_doc)
                        lines.append(f"  {{meta::pure::profiles::doc.doc = '{escaped_prop_doc}'}} {prop_name}: {prop_type}{multiplicity};")
                    else:
                        lines.append(f"  {prop_name}: {prop_type}{multiplicity};")

                lines.append("}")
                class_defs.append("\n".join(lines))

        return "\n\n".join(class_defs)

    def _escape_doc_string(self, doc: str) -> str:
        """Escape a documentation string for use in Pure code.

        Escapes single quotes and removes newlines to ensure valid Pure syntax.
        """
        if not doc:
            return ""
        # Escape single quotes
        escaped = doc.replace("'", "\\'")
        # Remove or replace newlines
        escaped = escaped.replace("\n", " ").replace("\r", "")
        # Trim excess whitespace
        escaped = " ".join(escaped.split())
        return escaped

    def generate_associations(self) -> str:
        """Generate Pure Association definitions."""
        if not self.database.relationships:
            return ""

        association_defs = []
        seen_associations = set()

        for rel in self.database.relationships:
            source_class = self.table_to_class.get(rel.source_table)
            target_class = self.table_to_class.get(rel.target_table)

            if not source_class or not target_class:
                continue

            # Create a unique key for this association to avoid duplicates
            assoc_key = (source_class, target_class, rel.property_name)
            if assoc_key in seen_associations:
                continue
            seen_associations.add(assoc_key)

            # Association name combines both classes
            assoc_name = f"{source_class}_{target_class}_{rel.property_name}"

            # Generate reverse property name (plural of source class)
            reverse_prop = rel.get_reverse_property_name(rel.source_table)

            lines = [f"Association {self.package_prefix}::domain::{assoc_name}"]
            lines.append("{")
            # Source side (many) - the table that has the FK
            lines.append(f"  {reverse_prop}: {self.package_prefix}::domain::{source_class}[*];")
            # Target side (one) - the referenced table
            lines.append(f"  {rel.property_name}: {self.package_prefix}::domain::{target_class}[0..1];")
            lines.append("}")

            association_defs.append("\n".join(lines))

        return "\n\n".join(association_defs)

    def generate_connection(
        self,
        account: str,
        warehouse: str,
        role: str = "ACCOUNTADMIN",
        region: Optional[str] = None,
        auth_type: str = "keypair",
        username: Optional[str] = None,
        private_key_vault_ref: str = "SNOWFLAKE_PRIVATE_KEY",
        passphrase_vault_ref: str = "SNOWFLAKE_PASSPHRASE",
        password_vault_ref: str = "SNOWFLAKE_PASSWORD",
    ) -> str:
        """Generate Pure connection definition.

        Args:
            account: Snowflake account identifier
            warehouse: Snowflake warehouse name
            role: Snowflake role (default: ACCOUNTADMIN)
            region: Snowflake region (default: us-east-1)
            auth_type: Authentication type - 'keypair' for SnowflakePublic or 'password' for UsernamePassword
            username: Snowflake username for the connection
            private_key_vault_ref: Vault reference name for the private key (keypair auth)
            passphrase_vault_ref: Vault reference name for the key passphrase (keypair auth)
            password_vault_ref: Vault reference name for password (password auth)
        """
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
        # Region is required by Legend Engine; use empty string for standard public endpoint
        lines.append(f"    region: '{region or ''}';")
        lines.append(f"    role: '{role}';")
        lines.append("  };")

        if auth_type == "keypair":
            # Key-pair authentication (recommended for production)
            lines.append("  auth: SnowflakePublic")
            lines.append("  {")
            lines.append(f"    publicUserName: '{username or 'LEGEND_USER'}';")
            lines.append(f"    privateKeyVaultReference: '{private_key_vault_ref}';")
            lines.append(f"    passPhraseVaultReference: '{passphrase_vault_ref}';")
            lines.append("  };")
        else:
            # Middle-tier username/password authentication
            # Uses Legend's credential vault to retrieve username and password
            lines.append("  auth: MiddleTierUserNamePassword")
            lines.append("  {")
            lines.append(f"    vaultReference: '{password_vault_ref}';")
            lines.append("  };")

        lines.append("}")
        return "\n".join(lines)

    def generate_mapping(self) -> str:
        """Generate Pure mapping definition with association mappings."""
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

                # Primary key
                if table.columns:
                    pk_col = table.primary_key_columns[0] if table.primary_key_columns else table.columns[0].name
                    block_lines.append("    ~primaryKey")
                    block_lines.append("    (")
                    block_lines.append(f"      [{store_path}]{schema.name}.{table.name}.{pk_col}")
                    block_lines.append("    )")

                block_lines.append(f"    ~mainTable [{store_path}]{schema.name}.{table.name}")

                # Property mappings only (no association mappings here)
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

        # Add association mappings
        if self.database.relationships:
            lines.append("")
            seen_associations = set()
            for rel in self.database.relationships:
                source_class = self.table_to_class.get(rel.source_table)
                target_class = self.table_to_class.get(rel.target_table)

                if not source_class or not target_class:
                    continue

                # Create a unique key for this association
                assoc_key = (source_class, target_class, rel.property_name)
                if assoc_key in seen_associations:
                    continue
                seen_associations.add(assoc_key)

                assoc_name = f"{source_class}_{target_class}_{rel.property_name}"
                assoc_path = f"{self.package_prefix}::domain::{assoc_name}"
                store_path = f"{self.package_prefix}::store::{self.database.name}"
                join_name = f"{rel.source_table}_{rel.target_table}"

                lines.append(f"  {assoc_path}: Relational")
                lines.append("  {")
                lines.append(f"    AssociationMapping")
                lines.append("    (")
                lines.append(f"      {rel.property_name}: [{store_path}]@{join_name}")
                lines.append("    )")
                lines.append("  }")

        lines.append(")")
        return "\n".join(lines)

    def generate_store_with_joins(self) -> str:
        """Generate Pure store definition including join definitions."""
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

        # Add Join definitions
        if self.database.relationships:
            lines.append("")
            for rel in self.database.relationships:
                source_table = self.database.get_table_by_name(rel.source_table)
                target_table = self.database.get_table_by_name(rel.target_table)
                if source_table and target_table:
                    join_name = f"{rel.source_table}_{rel.target_table}"
                    lines.append(f"  Join {join_name}({source_table.schema}.{rel.source_table}.{rel.source_column} = {target_table.schema}.{rel.target_table}.{rel.target_column})")

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
        region: Optional[str] = None,
        username: Optional[str] = None,
        auth_type: str = "keypair",
        private_key_vault_ref: str = "SNOWFLAKE_PRIVATE_KEY",
        passphrase_vault_ref: str = "SNOWFLAKE_PASSPHRASE",
        password_vault_ref: str = "SNOWFLAKE_PASSWORD",
        docs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """Generate all Pure code artifacts.

        Args:
            account: Snowflake account identifier
            warehouse: Snowflake warehouse name
            role: Snowflake role
            region: Snowflake region
            username: Snowflake username for the connection
            auth_type: Authentication type - 'keypair' or 'password'
            private_key_vault_ref: Vault reference for private key
            passphrase_vault_ref: Vault reference for passphrase
            password_vault_ref: Vault reference for password
            docs: Optional dict mapping class names to ClassDocumentation for doc.doc generation
        """
        artifacts = {
            "store": self.generate_store_with_joins(),  # Use store with joins
            "classes": self.generate_classes(docs=docs),
            "connection": self.generate_connection(
                account=account,
                warehouse=warehouse,
                role=role,
                region=region,
                auth_type=auth_type,
                username=username,
                private_key_vault_ref=private_key_vault_ref,
                passphrase_vault_ref=passphrase_vault_ref,
                password_vault_ref=password_vault_ref,
            ),
            "mapping": self.generate_mapping(),
            "runtime": self.generate_runtime(),
        }

        # Add associations if there are relationships
        associations = self.generate_associations()
        if associations:
            artifacts["associations"] = associations

        return artifacts

    def get_relationship_summary(self) -> List[Dict[str, str]]:
        """Get a summary of detected relationships."""
        return [
            {
                "source": f"{rel.source_table}.{rel.source_column}",
                "target": f"{rel.target_table}.{rel.target_column}",
                "type": rel.relationship_type,
                "property": rel.property_name,
            }
            for rel in self.database.relationships
        ]
