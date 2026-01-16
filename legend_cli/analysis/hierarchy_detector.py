"""Hierarchy (inheritance) detector for class inheritance opportunities."""

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from legend_cli.analysis.models import AnalysisSource, InheritanceOpportunity
from legend_cli.claude_client import ClaudeClient
from legend_cli.database.models import Database, Table
from legend_cli.prompts.hierarchy_templates import (
    HIERARCHY_DETECTION_PROMPT,
    HIERARCHY_DETECTION_SYSTEM_PROMPT,
    HIERARCHY_WITH_DOCS_CONTEXT,
    format_schema_for_hierarchy_analysis,
    format_table_comparison,
)


@dataclass
class ColumnOverlap:
    """Represents column overlap between two tables."""

    table1: Table
    table2: Table
    shared_columns: Set[str]
    only_table1: Set[str]
    only_table2: Set[str]
    overlap_percentage: float


class HierarchyDetector:
    """Detects class inheritance opportunities from database schemas.

    Uses both pattern-based detection and LLM analysis to identify:
    - Tables with high column overlap (potential base/derived classes)
    - Type discriminator columns (*_TYPE, *_CATEGORY)
    - Naming conventions suggesting hierarchies
    - Documentation-based "is-a" relationships
    """

    # Column overlap threshold for hierarchy suggestion
    MIN_OVERLAP_PERCENTAGE = 0.70

    # Discriminator column patterns
    DISCRIMINATOR_PATTERNS = (
        "_TYPE", "_CATEGORY", "_KIND", "_CLASS", "_SUBTYPE",
        "TYPE_", "CATEGORY_", "KIND_", "CLASS_",
    )

    # Base class naming patterns (suffix removed from derived)
    BASE_CLASS_INDICATORS = (
        "Base", "Abstract", "Parent", "Master", "Core",
    )

    def __init__(
        self,
        claude_client: Optional[ClaudeClient] = None,
        min_overlap: float = 0.70,
    ):
        """Initialize the hierarchy detector.

        Args:
            claude_client: ClaudeClient for LLM-based detection
            min_overlap: Minimum column overlap percentage for pattern detection
        """
        self.claude = claude_client or ClaudeClient()
        self.min_overlap = min_overlap

    def detect(
        self,
        database: Database,
        documentation: Optional[str] = None,
        use_llm: bool = True,
    ) -> List[InheritanceOpportunity]:
        """Detect inheritance opportunities in the database schema.

        Args:
            database: Database object with schemas and tables
            documentation: Optional documentation content for context
            use_llm: Whether to use LLM for enhanced detection

        Returns:
            List of InheritanceOpportunity objects
        """
        opportunities = []

        # Step 1: Pattern-based detection
        pattern_opportunities = self._detect_from_patterns(database)
        opportunities.extend(pattern_opportunities)

        # Step 2: Column overlap detection
        overlap_opportunities = self._detect_from_column_overlap(database)
        opportunities.extend(overlap_opportunities)

        # Step 3: LLM-based detection (if enabled and client available)
        if use_llm:
            try:
                llm_opportunities = self._detect_with_llm(database, documentation)
                opportunities.extend(llm_opportunities)
            except Exception as e:
                # Log warning but don't fail
                print(f"Warning: LLM-based hierarchy detection failed: {e}")

        # Deduplicate and merge
        return self._merge_opportunities(opportunities)

    def _detect_from_patterns(self, database: Database) -> List[InheritanceOpportunity]:
        """Detect hierarchies from naming patterns and discriminator columns."""
        opportunities = []

        tables = self._get_all_tables(database)

        # Look for discriminator columns
        for table in tables:
            discriminator = self._find_discriminator_column(table)
            if discriminator:
                # This table might be a polymorphic base
                opportunities.append(InheritanceOpportunity(
                    base_class_name=table.get_class_name(),
                    base_class_properties=[col.name for col in table.columns],
                    derived_classes=[],  # LLM will help identify these
                    discriminator_column=discriminator,
                    confidence=0.6,
                    reasoning=f"Table has discriminator column: {discriminator}",
                    source=AnalysisSource.SCHEMA_PATTERN,
                ))

        # Look for naming patterns (e.g., SavingsAccount, CheckingAccount -> Account)
        pattern_groups = self._group_by_naming_pattern(tables)
        for base_name, derived_tables in pattern_groups.items():
            if len(derived_tables) >= 2:  # At least 2 derived classes
                # Find common columns
                common_cols = self._find_common_columns(derived_tables)
                if len(common_cols) >= 3:  # At least 3 common columns
                    opportunities.append(InheritanceOpportunity(
                        base_class_name=base_name,
                        base_class_properties=list(common_cols),
                        derived_classes=[t.get_class_name() for t in derived_tables],
                        confidence=0.7,
                        reasoning=f"Naming pattern suggests hierarchy with {len(derived_tables)} derived classes",
                        source=AnalysisSource.SCHEMA_PATTERN,
                        derived_class_properties={
                            t.get_class_name(): [
                                c.name for c in t.columns
                                if c.name.upper() not in {cc.upper() for cc in common_cols}
                            ]
                            for t in derived_tables
                        }
                    ))

        return opportunities

    def _detect_from_column_overlap(self, database: Database) -> List[InheritanceOpportunity]:
        """Detect hierarchies from tables with high column overlap."""
        opportunities = []

        tables = self._get_all_tables(database)
        overlaps = self._calculate_overlaps(tables)

        # Find tables with high overlap
        high_overlap = [o for o in overlaps if o.overlap_percentage >= self.min_overlap]

        # Group overlapping tables to find potential hierarchies
        processed = set()
        for overlap in sorted(high_overlap, key=lambda x: -x.overlap_percentage):
            t1_name = overlap.table1.name
            t2_name = overlap.table2.name

            if t1_name in processed and t2_name in processed:
                continue

            # Determine base vs derived based on column count
            if len(overlap.table1.columns) <= len(overlap.table2.columns):
                base_table = overlap.table1
                derived_table = overlap.table2
            else:
                base_table = overlap.table2
                derived_table = overlap.table1

            opportunities.append(InheritanceOpportunity(
                base_class_name=base_table.get_class_name(),
                base_class_properties=[c.name for c in base_table.columns
                                       if c.name.upper() in overlap.shared_columns],
                derived_classes=[derived_table.get_class_name()],
                confidence=min(0.9, overlap.overlap_percentage + 0.1),
                reasoning=f"{overlap.overlap_percentage:.0%} column overlap between tables",
                source=AnalysisSource.SCHEMA_PATTERN,
                derived_class_properties={
                    derived_table.get_class_name(): [
                        c.name for c in derived_table.columns
                        if c.name.upper() in overlap.only_table2
                    ]
                }
            ))

            processed.add(t1_name)
            processed.add(t2_name)

        return opportunities

    def _detect_with_llm(
        self,
        database: Database,
        documentation: Optional[str],
    ) -> List[InheritanceOpportunity]:
        """Use LLM to detect hierarchies with enhanced understanding."""
        opportunities = []

        # Format schema for prompt
        schema_info = format_schema_for_hierarchy_analysis(database)

        # Add documentation context if available
        doc_context = ""
        if documentation:
            doc_context = HIERARCHY_WITH_DOCS_CONTEXT.format(doc_content=documentation)

        # Build prompt
        prompt = HIERARCHY_DETECTION_PROMPT.format(
            schema_info=schema_info,
            doc_context=doc_context,
        )

        # Call LLM
        response = self.claude.client.messages.create(
            model=self.claude.model,
            max_tokens=4096,
            system=HIERARCHY_DETECTION_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = response.content[0].text.strip()

        # Parse response
        opportunities = self._parse_llm_response(response_text)

        return opportunities

    def _parse_llm_response(self, response_text: str) -> List[InheritanceOpportunity]:
        """Parse LLM JSON response into InheritanceOpportunity objects."""
        opportunities = []

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
            print(f"Warning: Failed to parse hierarchy detection response: {e}")
            return opportunities

        if not isinstance(data, list):
            data = [data]

        for item in data:
            if isinstance(item, dict):
                try:
                    opportunities.append(InheritanceOpportunity(
                        base_class_name=item.get("base_class_name", ""),
                        base_class_properties=item.get("base_class_properties", []),
                        derived_classes=item.get("derived_classes", []),
                        discriminator_column=item.get("discriminator_column"),
                        confidence=float(item.get("confidence", 0.5)),
                        reasoning=item.get("reasoning", ""),
                        source=AnalysisSource.LLM_INFERENCE,
                        derived_class_properties=item.get("derived_class_properties", {}),
                    ))
                except (ValueError, TypeError) as e:
                    print(f"Warning: Skipping invalid hierarchy item: {e}")
                    continue

        return opportunities

    def _get_all_tables(self, database: Database) -> List[Table]:
        """Get all tables from all schemas."""
        tables = []
        for schema in database.schemas:
            tables.extend(schema.tables)
        return tables

    def _find_discriminator_column(self, table: Table) -> Optional[str]:
        """Find a discriminator column in the table."""
        for col in table.columns:
            col_upper = col.name.upper()
            for pattern in self.DISCRIMINATOR_PATTERNS:
                if col_upper.endswith(pattern) or col_upper.startswith(pattern):
                    return col.name
        return None

    def _group_by_naming_pattern(self, tables: List[Table]) -> Dict[str, List[Table]]:
        """Group tables by potential base class name from naming patterns.

        Examples:
            SavingsAccount, CheckingAccount -> Account
            DigitalProduct, PhysicalProduct -> Product
        """
        groups: Dict[str, List[Table]] = {}

        for table in tables:
            class_name = table.get_class_name()

            # Look for common suffixes
            for other in tables:
                if table == other:
                    continue

                other_name = other.get_class_name()

                # Check if one is a suffix of the other
                base_name = self._find_common_suffix(class_name, other_name)
                if base_name and len(base_name) >= 3:
                    if base_name not in groups:
                        groups[base_name] = []
                    if table not in groups[base_name]:
                        groups[base_name].append(table)
                    if other not in groups[base_name]:
                        groups[base_name].append(other)

        return groups

    def _find_common_suffix(self, name1: str, name2: str) -> Optional[str]:
        """Find common suffix between two class names."""
        # Reverse strings to find common suffix
        rev1 = name1[::-1]
        rev2 = name2[::-1]

        common = ""
        for c1, c2 in zip(rev1, rev2):
            if c1 == c2:
                common += c1
            else:
                break

        suffix = common[::-1]

        # Must be a word boundary (start with uppercase)
        if suffix and suffix[0].isupper() and len(suffix) >= 3:
            return suffix

        return None

    def _find_common_columns(self, tables: List[Table]) -> Set[str]:
        """Find columns common to all tables."""
        if not tables:
            return set()

        # Start with first table's columns
        common = {col.name.upper() for col in tables[0].columns}

        # Intersect with all other tables
        for table in tables[1:]:
            table_cols = {col.name.upper() for col in table.columns}
            common &= table_cols

        return common

    def _calculate_overlaps(self, tables: List[Table]) -> List[ColumnOverlap]:
        """Calculate column overlaps between all table pairs."""
        overlaps = []

        for i, t1 in enumerate(tables):
            for t2 in tables[i + 1:]:
                cols1 = {col.name.upper() for col in t1.columns}
                cols2 = {col.name.upper() for col in t2.columns}

                shared = cols1 & cols2
                if not shared:
                    continue

                overlap_pct = len(shared) / min(len(cols1), len(cols2))

                overlaps.append(ColumnOverlap(
                    table1=t1,
                    table2=t2,
                    shared_columns=shared,
                    only_table1=cols1 - cols2,
                    only_table2=cols2 - cols1,
                    overlap_percentage=overlap_pct,
                ))

        return overlaps

    def _merge_opportunities(
        self,
        opportunities: List[InheritanceOpportunity],
    ) -> List[InheritanceOpportunity]:
        """Merge and deduplicate inheritance opportunities."""
        if not opportunities:
            return []

        # Group by base class name
        by_base: Dict[str, List[InheritanceOpportunity]] = {}
        for opp in opportunities:
            if opp.base_class_name not in by_base:
                by_base[opp.base_class_name] = []
            by_base[opp.base_class_name].append(opp)

        # Merge each group
        merged = []
        for base_name, group in by_base.items():
            if len(group) == 1:
                merged.append(group[0])
            else:
                # Combine all derived classes and properties
                all_derived = set()
                all_base_props = set()
                all_derived_props: Dict[str, List[str]] = {}
                discriminator = None
                best_confidence = 0.0
                reasonings = []

                for opp in group:
                    all_derived.update(opp.derived_classes)
                    all_base_props.update(opp.base_class_properties)

                    for dc, props in opp.derived_class_properties.items():
                        if dc not in all_derived_props:
                            all_derived_props[dc] = []
                        all_derived_props[dc].extend(props)

                    if opp.discriminator_column:
                        discriminator = opp.discriminator_column

                    if opp.confidence > best_confidence:
                        best_confidence = opp.confidence

                    if opp.reasoning:
                        reasonings.append(opp.reasoning)

                merged.append(InheritanceOpportunity(
                    base_class_name=base_name,
                    base_class_properties=list(all_base_props),
                    derived_classes=list(all_derived),
                    discriminator_column=discriminator,
                    confidence=best_confidence,
                    reasoning="; ".join(reasonings[:3]),  # Keep first 3 reasons
                    source=AnalysisSource.LLM_INFERENCE,  # Mixed source
                    derived_class_properties={
                        dc: list(set(props))
                        for dc, props in all_derived_props.items()
                    },
                ))

        return merged
