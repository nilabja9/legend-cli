"""Relationship detection between database tables."""

import re
from typing import Optional, List, Set, Tuple

from .models import Database, Table, Column, Relationship


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
        seen: Set[Tuple[str, str, str, str]] = set()
        unique = []

        for rel in relationships:
            key = (rel.source_table, rel.source_column, rel.target_table, rel.target_column)
            if key not in seen:
                seen.add(key)
                unique.append(rel)

        return unique
