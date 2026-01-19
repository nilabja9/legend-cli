"""LLM-based relationship analyzer for database schemas.

Discovers probable relationships between tables using Claude to analyze
schema structure, column names, and data patterns when explicit foreign
key constraints are not defined.
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from legend_cli.claude_client import ClaudeClient
from legend_cli.database.models import Database, Relationship

logger = logging.getLogger(__name__)


# Prompt template for relationship discovery
RELATIONSHIP_DISCOVERY_PROMPT = '''You are a database schema analyst. Analyze the following database schema and identify probable foreign key relationships between tables.

DATABASE SCHEMA:
{schema_description}

ANALYSIS INSTRUCTIONS:
1. Look for columns that likely reference other tables based on:
   - Column naming patterns (e.g., user_id likely references users table)
   - Data type compatibility between columns
   - Common foreign key patterns (_id, _key, _code suffixes)
   - Business domain knowledge (e.g., order -> customer, trade -> instrument)

2. For each relationship you identify, determine:
   - Source table and column (the table with the foreign key)
   - Target table and column (the referenced/parent table)
   - Relationship type (usually many_to_one for FK relationships)
   - A property name for the association (camelCase, singular for the target)

3. Only include relationships where you have moderate to high confidence.

4. Consider common patterns in the domain:
   - Trading systems: trades -> instruments, orders -> clients, positions -> accounts
   - Financial systems: transactions -> accounts, settlements -> trades
   - General patterns: any table with [name]_id likely references [name] table

OUTPUT FORMAT:
Return a JSON array of relationship objects. Each object should have:
- source_table: string (table containing the foreign key)
- source_column: string (the FK column name)
- target_table: string (the referenced table)
- target_column: string (the referenced column, usually the primary key)
- relationship_type: string ("many_to_one" for most FK relationships)
- property_name: string (camelCase name for the association property)
- confidence: number (0.0 to 1.0)
- reasoning: string (brief explanation)

Example output:
[
  {{
    "source_table": "trade",
    "source_column": "instrument_id",
    "target_table": "instrument",
    "target_column": "id",
    "relationship_type": "many_to_one",
    "property_name": "instrument",
    "confidence": 0.9,
    "reasoning": "Column name 'instrument_id' follows standard FK naming convention referencing 'instrument' table"
  }}
]

Return ONLY the JSON array, no other text.'''


@dataclass
class DiscoveredRelationship:
    """A relationship discovered by LLM analysis."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str
    property_name: str
    confidence: float
    reasoning: str

    def to_relationship(self) -> Relationship:
        """Convert to standard Relationship model."""
        return Relationship(
            source_table=self.source_table,
            source_column=self.source_column,
            target_table=self.target_table,
            target_column=self.target_column,
            relationship_type=self.relationship_type,
            property_name=self.property_name,
        )


class RelationshipAnalyzer:
    """Discovers relationships between tables using LLM analysis.

    When a database lacks explicit foreign key constraints, this analyzer
    uses Claude to infer probable relationships based on column names,
    data types, and domain patterns.
    """

    def __init__(self, claude_client: Optional[ClaudeClient] = None):
        """Initialize the analyzer.

        Args:
            claude_client: Optional Claude client instance. If not provided,
                          creates a new one using default settings.
        """
        self.claude = claude_client or ClaudeClient()

    def discover_relationships(
        self,
        database: Database,
        confidence_threshold: float = 0.6,
    ) -> List[DiscoveredRelationship]:
        """Discover probable relationships in the database schema.

        Args:
            database: Database model with schema information
            confidence_threshold: Minimum confidence to include relationship

        Returns:
            List of discovered relationships above the confidence threshold
        """
        logger.info("Starting LLM-based relationship discovery for %s", database.name)

        # Build schema description for the prompt
        schema_description = self._build_schema_description(database)

        # Call Claude for analysis
        prompt = RELATIONSHIP_DISCOVERY_PROMPT.format(schema_description=schema_description)

        try:
            response = self.claude.client.messages.create(
                model=self.claude.model,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )

            response_text = response.content[0].text.strip()
            logger.debug("LLM response: %s", response_text[:500])

            # Parse response
            relationships = self._parse_response(response_text)

            # Filter by confidence
            filtered = [
                r for r in relationships
                if r.confidence >= confidence_threshold
            ]

            logger.info(
                "Discovered %d relationships (%d above %.1f confidence threshold)",
                len(relationships), len(filtered), confidence_threshold
            )

            return filtered

        except Exception as e:
            logger.error("Failed to discover relationships: %s", e)
            return []

    def discover_and_update_database(
        self,
        database: Database,
        confidence_threshold: float = 0.6,
    ) -> List[Relationship]:
        """Discover relationships and update the database model.

        Args:
            database: Database model to update
            confidence_threshold: Minimum confidence threshold

        Returns:
            List of Relationship objects added to the database
        """
        discovered = self.discover_relationships(database, confidence_threshold)

        # Convert to standard relationships and validate
        relationships = []
        table_names = {t.name.upper() for t in database.get_all_tables()}

        for disc in discovered:
            # Validate that both tables exist
            if disc.source_table.upper() not in table_names:
                logger.warning(
                    "Skipping relationship: source table '%s' not found",
                    disc.source_table
                )
                continue

            if disc.target_table.upper() not in table_names:
                logger.warning(
                    "Skipping relationship: target table '%s' not found",
                    disc.target_table
                )
                continue

            # Find actual table names (case-sensitive)
            source_table = self._find_table_name(database, disc.source_table)
            target_table = self._find_table_name(database, disc.target_table)

            if not source_table or not target_table:
                continue

            rel = Relationship(
                source_table=source_table,
                source_column=disc.source_column,
                target_table=target_table,
                target_column=disc.target_column,
                relationship_type=disc.relationship_type,
                property_name=disc.property_name,
            )
            relationships.append(rel)

        # Update database
        database.relationships = relationships

        return relationships

    def _build_schema_description(self, database: Database) -> str:
        """Build a text description of the database schema for the prompt."""
        lines = [f"Database: {database.name}\n"]

        for schema in database.schemas:
            lines.append(f"Schema: {schema.name}")

            for table in schema.tables:
                lines.append(f"\n  Table: {table.name}")
                lines.append("  Columns:")

                for col in table.columns:
                    pk_marker = " [PK]" if col.is_primary_key else ""
                    nullable = " (nullable)" if col.is_nullable else " (not null)"
                    lines.append(f"    - {col.name}: {col.data_type}{pk_marker}{nullable}")

                if table.primary_key_columns:
                    lines.append(f"  Primary Key: {', '.join(table.primary_key_columns)}")

        return "\n".join(lines)

    def _parse_response(self, response_text: str) -> List[DiscoveredRelationship]:
        """Parse the LLM response into DiscoveredRelationship objects."""
        # Try to extract JSON from the response
        try:
            # First try direct JSON parsing
            data = json.loads(response_text)
        except json.JSONDecodeError:
            # Try to find JSON array in the response
            match = re.search(r'\[[\s\S]*\]', response_text)
            if match:
                try:
                    data = json.loads(match.group())
                except json.JSONDecodeError:
                    logger.error("Failed to parse JSON from response")
                    return []
            else:
                logger.error("No JSON array found in response")
                return []

        if not isinstance(data, list):
            logger.error("Response is not a list")
            return []

        relationships = []
        for item in data:
            try:
                rel = DiscoveredRelationship(
                    source_table=item.get("source_table", ""),
                    source_column=item.get("source_column", ""),
                    target_table=item.get("target_table", ""),
                    target_column=item.get("target_column", "id"),
                    relationship_type=item.get("relationship_type", "many_to_one"),
                    property_name=item.get("property_name", ""),
                    confidence=float(item.get("confidence", 0.5)),
                    reasoning=item.get("reasoning", ""),
                )

                # Validate required fields
                if rel.source_table and rel.source_column and rel.target_table and rel.property_name:
                    relationships.append(rel)
                else:
                    logger.warning("Skipping incomplete relationship: %s", item)

            except (KeyError, ValueError, TypeError) as e:
                logger.warning("Failed to parse relationship item: %s - %s", item, e)

        return relationships

    def _find_table_name(self, database: Database, table_name: str) -> Optional[str]:
        """Find the actual table name (case-sensitive) from the database."""
        for table in database.get_all_tables():
            if table.name.upper() == table_name.upper():
                return table.name
        return None


def discover_relationships(
    database: Database,
    confidence_threshold: float = 0.6,
    claude_client: Optional[ClaudeClient] = None,
) -> List[Relationship]:
    """Convenience function to discover relationships in a database.

    Args:
        database: Database model
        confidence_threshold: Minimum confidence threshold
        claude_client: Optional Claude client

    Returns:
        List of discovered Relationship objects
    """
    analyzer = RelationshipAnalyzer(claude_client=claude_client)
    return analyzer.discover_and_update_database(database, confidence_threshold)
