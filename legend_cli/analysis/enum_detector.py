"""Enumeration detector for identifying enum candidates in database schemas."""

import json
from typing import Any, Callable, Dict, List, Optional, Set

from legend_cli.analysis.models import AnalysisSource, EnumerationCandidate
from legend_cli.claude_client import ClaudeClient
from legend_cli.database.models import Database, Table
from legend_cli.prompts.enum_templates import (
    ENUM_DETECTION_PROMPT,
    ENUM_DETECTION_SYSTEM_PROMPT,
    ENUM_DOCS_CONTEXT,
    ENUM_WITH_VALUES_PROMPT,
    format_reference_tables,
    format_sample_values,
    format_schema_for_enum_analysis,
    normalize_enum_value,
)


class EnumDetector:
    """Detects enumeration candidates from database schemas.

    Uses pattern-based detection and LLM analysis to identify:
    - Reference/lookup tables with < 50 rows
    - Columns with _TYPE, _STATUS, _CODE suffixes
    - Low cardinality columns (< 20 distinct values)
    - Documentation value lists
    """

    # Maximum distinct values for a column to be considered an enum
    MAX_ENUM_VALUES = 20

    # Maximum rows for a reference table to be considered an enum source
    MAX_REFERENCE_TABLE_ROWS = 50

    # Column patterns that suggest enum values
    ENUM_COLUMN_SUFFIXES = (
        "_TYPE", "_STATUS", "_CODE", "_CATEGORY", "_KIND",
        "_CLASS", "_MODE", "_STATE", "_LEVEL", "_PRIORITY",
        "_METHOD", "_REASON", "_SOURCE", "_CD",
    )

    # Table patterns that suggest reference/lookup tables
    REFERENCE_TABLE_SUFFIXES = (
        "_TYPE", "_STATUS", "_CODE", "_CATEGORY", "_LOOKUP",
        "_REF", "_REFERENCE", "_CODES", "_TYPES", "_ENUM",
        "_LIST", "_VALUES", "_OPTIONS", "_MASTER",
    )

    def __init__(
        self,
        claude_client: Optional[ClaudeClient] = None,
        max_enum_values: int = 20,
        max_reference_rows: int = 50,
    ):
        """Initialize the enum detector.

        Args:
            claude_client: ClaudeClient for LLM-based detection
            max_enum_values: Maximum distinct values to consider as enum
            max_reference_rows: Maximum rows for reference table
        """
        self.claude = claude_client or ClaudeClient()
        self.max_enum_values = max_enum_values
        self.max_reference_rows = max_reference_rows

    def detect(
        self,
        database: Database,
        documentation: Optional[str] = None,
        sample_values: Optional[Dict[str, List[Any]]] = None,
        value_fetcher: Optional[Callable[[str, str], List[Any]]] = None,
        use_llm: bool = True,
    ) -> List[EnumerationCandidate]:
        """Detect enumeration candidates in the database schema.

        Args:
            database: Database object with schemas and tables
            documentation: Optional documentation content for context
            sample_values: Pre-fetched sample values {table.column: [values]}
            value_fetcher: Callback to fetch column values (table_name, column_name) -> [values]
            use_llm: Whether to use LLM for enhanced detection

        Returns:
            List of EnumerationCandidate objects
        """
        candidates = []

        # NOTE: Column pattern detection and reference table detection are DISABLED
        # because they create conflicts:
        # - Reference tables (CLIENT_TYPE, TRADE_STATUS) should be Classes, not Enums
        # - FK columns (CLIENT_TYPE_ID) should be Integer, not Enum types
        #
        # Only cardinality detection and LLM detection are used to find actual
        # enum columns (string columns with low cardinality like 'status', 'type')

        # Step 1: Low cardinality detection (if values available)
        # This detects actual string columns with enum-like values
        if sample_values or value_fetcher:
            cardinality_candidates = self._detect_from_cardinality(
                database, sample_values, value_fetcher
            )
            candidates.extend(cardinality_candidates)

        # Step 2: LLM-based detection (if enabled)
        # LLM can identify enum columns based on naming and context
        if use_llm:
            try:
                llm_candidates = self._detect_with_llm(
                    database, documentation, sample_values
                )
                candidates.extend(llm_candidates)
            except Exception as e:
                print(f"Warning: LLM-based enum detection failed: {e}")

        # Deduplicate and merge
        return self._merge_candidates(candidates)

    def _detect_from_column_patterns(
        self,
        database: Database,
    ) -> List[EnumerationCandidate]:
        """Detect enums from column naming patterns."""
        candidates = []

        for schema in database.schemas:
            for table in schema.tables:
                for col in table.columns:
                    if self._is_enum_column_name(col.name):
                        # Generate enum name from column
                        enum_name = self._generate_enum_name(table, col.name)

                        candidates.append(EnumerationCandidate(
                            name=enum_name,
                            source_table=table.name,
                            source_column=col.name,
                            values=[],  # Values will be filled by LLM or value_fetcher
                            confidence=0.6,
                            description=f"Enumeration for {col.name}",
                            source=AnalysisSource.SCHEMA_PATTERN,
                        ))

        return candidates

    def _detect_reference_tables(
        self,
        database: Database,
    ) -> List[EnumerationCandidate]:
        """Detect enums from reference/lookup tables."""
        candidates = []

        for schema in database.schemas:
            for table in schema.tables:
                if self._is_reference_table_name(table.name):
                    # Look for code and description columns
                    code_col = self._find_code_column(table)
                    if code_col:
                        enum_name = self._generate_enum_name_from_table(table)

                        candidates.append(EnumerationCandidate(
                            name=enum_name,
                            source_table=table.name,
                            source_column=code_col,
                            values=[],  # Values should be fetched from table
                            confidence=0.8,
                            description=f"Enumeration from reference table {table.name}",
                            source=AnalysisSource.SCHEMA_PATTERN,
                        ))

        return candidates

    def _detect_from_cardinality(
        self,
        database: Database,
        sample_values: Optional[Dict[str, List[Any]]],
        value_fetcher: Optional[Callable[[str, str], List[Any]]],
    ) -> List[EnumerationCandidate]:
        """Detect enums from low cardinality columns."""
        candidates = []

        for schema in database.schemas:
            for table in schema.tables:
                for col in table.columns:
                    # Get values
                    key = f"{table.name}.{col.name}"
                    values = None

                    if sample_values and key in sample_values:
                        values = sample_values[key]
                    elif value_fetcher:
                        try:
                            values = value_fetcher(table.name, col.name)
                        except Exception:
                            continue

                    if values is None:
                        continue

                    # Check cardinality
                    unique_values = list(set(v for v in values if v is not None))
                    if len(unique_values) > 0 and len(unique_values) <= self.max_enum_values:
                        # Low cardinality - potential enum
                        enum_name = self._generate_enum_name(table, col.name)

                        # Normalize values to enum format
                        normalized = [normalize_enum_value(str(v)) for v in unique_values]

                        candidates.append(EnumerationCandidate(
                            name=enum_name,
                            source_table=table.name,
                            source_column=col.name,
                            values=normalized,
                            confidence=0.7 + (0.2 * (1 - len(unique_values) / self.max_enum_values)),
                            description=f"Low cardinality column ({len(unique_values)} values)",
                            source=AnalysisSource.SCHEMA_PATTERN,
                            value_descriptions={
                                normalize_enum_value(str(v)): str(v)
                                for v in unique_values
                            }
                        ))

        return candidates

    def _detect_with_llm(
        self,
        database: Database,
        documentation: Optional[str],
        sample_values: Optional[Dict[str, List[Any]]],
    ) -> List[EnumerationCandidate]:
        """Use LLM to detect enums with enhanced understanding."""
        candidates = []

        # Format schema
        schema_info = format_schema_for_enum_analysis(database)
        reference_tables_info = format_reference_tables(database)

        # Add documentation context
        doc_context = ""
        if documentation:
            doc_context = ENUM_DOCS_CONTEXT.format(doc_content=documentation)

        # Choose prompt based on available data
        if sample_values:
            sample_info = format_sample_values(sample_values)
            prompt = ENUM_WITH_VALUES_PROMPT.format(
                schema_info=schema_info,
                sample_values=sample_info,
                doc_context=doc_context,
            )
        else:
            prompt = ENUM_DETECTION_PROMPT.format(
                schema_info=schema_info,
                reference_tables_info=reference_tables_info,
                doc_context=doc_context,
            )

        # Call LLM
        response = self.claude.client.messages.create(
            model=self.claude.model,
            max_tokens=4096,
            system=ENUM_DETECTION_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = response.content[0].text.strip()

        # Parse response
        candidates = self._parse_llm_response(response_text)

        return candidates

    def _parse_llm_response(self, response_text: str) -> List[EnumerationCandidate]:
        """Parse LLM JSON response into EnumerationCandidate objects."""
        candidates = []

        # Handle markdown code blocks
        json_text = response_text
        if "```json" in response_text:
            start = response_text.find("```json") + 7
            end = response_text.find("```", start)
            json_text = response_text[start:end].strip()
        elif "```" in response_text:
            start = response_text.find("```") + 3
            end = response_text.find("```", start)
            json_text = response_text[start:end].strip()

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse enum detection response: {e}")
            return candidates

        if not isinstance(data, list):
            data = [data]

        for item in data:
            if isinstance(item, dict):
                try:
                    candidates.append(EnumerationCandidate(
                        name=item.get("name", ""),
                        source_table=item.get("source_table", ""),
                        source_column=item.get("source_column", ""),
                        values=item.get("values", []),
                        confidence=float(item.get("confidence", 0.5)),
                        description=item.get("description"),
                        source=AnalysisSource.LLM_INFERENCE,
                        value_descriptions=item.get("value_descriptions", {}),
                    ))
                except (ValueError, TypeError) as e:
                    print(f"Warning: Skipping invalid enum item: {e}")
                    continue

        return candidates

    def _is_enum_column_name(self, column_name: str) -> bool:
        """Check if column name suggests an enumeration."""
        name_upper = column_name.upper()
        return any(name_upper.endswith(suffix) for suffix in self.ENUM_COLUMN_SUFFIXES)

    def _is_reference_table_name(self, table_name: str) -> bool:
        """Check if table name suggests a reference/lookup table."""
        name_upper = table_name.upper()
        return any(name_upper.endswith(suffix) for suffix in self.REFERENCE_TABLE_SUFFIXES)

    def _find_code_column(self, table: Table) -> Optional[str]:
        """Find the code column in a reference table."""
        # Look for columns named CODE, CD, TYPE, etc.
        code_patterns = ("CODE", "CD", "TYPE", "ID", "KEY", "VALUE")

        for col in table.columns:
            col_upper = col.name.upper()
            for pattern in code_patterns:
                if pattern in col_upper or col_upper == pattern:
                    return col.name

        # Fallback: first non-ID column
        for col in table.columns:
            if not col.name.upper().endswith("_ID") and col.name.upper() != "ID":
                return col.name

        return None

    def _generate_enum_name(self, table: Table, column_name: str) -> str:
        """Generate enum name from table and column."""
        # Remove common suffixes
        name = column_name.upper()
        for suffix in ("_TYPE", "_STATUS", "_CODE", "_CD", "_CATEGORY"):
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break

        # Convert to PascalCase
        parts = name.split("_")
        pascal = "".join(p.capitalize() for p in parts if p)

        # Add suffix if needed for clarity
        if not any(pascal.endswith(s) for s in ("Status", "Type", "Category", "Code")):
            # Use column suffix as type hint
            col_upper = column_name.upper()
            if "STATUS" in col_upper:
                pascal += "Status"
            elif "TYPE" in col_upper:
                pascal += "Type"
            elif "CATEGORY" in col_upper:
                pascal += "Category"

        return pascal

    def _generate_enum_name_from_table(self, table: Table) -> str:
        """Generate enum name from reference table name."""
        name = table.name.upper()

        # Remove common table suffixes
        for suffix in self.REFERENCE_TABLE_SUFFIXES:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break

        # Convert to PascalCase
        parts = name.split("_")
        return "".join(p.capitalize() for p in parts if p)

    def _merge_candidates(
        self,
        candidates: List[EnumerationCandidate],
    ) -> List[EnumerationCandidate]:
        """Merge and deduplicate enumeration candidates."""
        if not candidates:
            return []

        # Group by (source_table, source_column) or name
        by_source: Dict[str, List[EnumerationCandidate]] = {}

        for cand in candidates:
            key = f"{cand.source_table}.{cand.source_column}"
            if key not in by_source:
                by_source[key] = []
            by_source[key].append(cand)

        # Merge each group
        merged = []
        for key, group in by_source.items():
            if len(group) == 1:
                merged.append(group[0])
            else:
                # Merge values and descriptions
                all_values: Set[str] = set()
                all_descriptions: Dict[str, str] = {}
                best_name = group[0].name
                best_confidence = 0.0
                best_description = None

                for cand in group:
                    all_values.update(cand.values)
                    all_descriptions.update(cand.value_descriptions)

                    if cand.confidence > best_confidence:
                        best_confidence = cand.confidence
                        best_name = cand.name
                        best_description = cand.description

                merged.append(EnumerationCandidate(
                    name=best_name,
                    source_table=group[0].source_table,
                    source_column=group[0].source_column,
                    values=sorted(list(all_values)),
                    confidence=best_confidence,
                    description=best_description,
                    source=AnalysisSource.LLM_INFERENCE,  # Mixed source
                    value_descriptions=all_descriptions,
                ))

        return merged
