"""Derived property analyzer for detecting computed properties from SQL patterns."""

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from legend_cli.analysis.models import AnalysisSource, DerivedPropertySuggestion
from legend_cli.claude_client import ClaudeClient
from legend_cli.database.models import Database, Relationship, Table
from legend_cli.prompts.derived_templates import (
    DERIVED_DOCS_CONTEXT,
    DERIVED_FROM_SCHEMA_PROMPT,
    DERIVED_FROM_SQL_PROMPT,
    DERIVED_PROPERTY_SYSTEM_PROMPT,
    format_relationships_for_derived,
    format_schema_for_derived,
    format_sql_for_derived,
)


@dataclass
class SqlAggregation:
    """Represents an aggregation found in SQL."""

    table: str
    alias: Optional[str]
    function: str  # SUM, COUNT, AVG, etc.
    column: str
    alias_name: Optional[str]


@dataclass
class SqlCalculation:
    """Represents a calculation found in SQL."""

    table: str
    expression: str
    columns: List[str]
    alias_name: Optional[str]


class DerivedAnalyzer:
    """Analyzes SQL patterns and schemas to identify derived (computed) properties.

    Detects patterns like:
    - SQL aggregations (COUNT, SUM, AVG)
    - Calculated fields (price * quantity)
    - Date calculations (DATEDIFF)
    - String operations (CONCAT)
    """

    # SQL aggregation functions that map to Pure
    AGGREGATION_FUNCTIONS = {
        "SUM": ("->sum()", "Float"),
        "COUNT": ("->size()", "Integer"),
        "AVG": ("->average()", "Float"),
        "MIN": ("->min()", "Float"),  # Type depends on column
        "MAX": ("->max()", "Float"),
    }

    # Common derived property patterns based on column names
    SEMANTIC_PATTERNS = {
        "fullName": {
            "requires": [("FIRST_NAME", "FIRSTNAME"), ("LAST_NAME", "LASTNAME")],
            "expression": "$this.firstName + ' ' + $this.lastName",
            "return_type": "String",
            "description": "Full name combining first and last name",
        },
        "age": {
            "requires": [("BIRTH_DATE", "DOB", "DATE_OF_BIRTH", "BIRTHDATE")],
            "expression": "$this.birthDate->dateDiff(today(), DurationUnit.YEARS)",
            "return_type": "Integer",
            "description": "Age calculated from birth date",
        },
        "isExpired": {
            "requires": [("EXPIRY_DATE", "EXPIRATION_DATE", "VALID_UNTIL")],
            "expression": "$this.expiryDate < today()",
            "return_type": "Boolean",
            "description": "Whether the item has expired",
        },
        "durationDays": {
            "requires": [("START_DATE", "BEGIN_DATE"), ("END_DATE", "FINISH_DATE")],
            "expression": "$this.endDate->dateDiff($this.startDate, DurationUnit.DAYS)",
            "return_type": "Integer",
            "description": "Duration in days between start and end dates",
        },
    }

    def __init__(
        self,
        claude_client: Optional[ClaudeClient] = None,
    ):
        """Initialize the derived property analyzer.

        Args:
            claude_client: ClaudeClient for LLM-based analysis
        """
        self.claude = claude_client or ClaudeClient()

    def analyze(
        self,
        database: Database,
        sql_queries: Optional[List[str]] = None,
        documentation: Optional[str] = None,
        use_llm: bool = True,
    ) -> List[DerivedPropertySuggestion]:
        """Analyze schema and SQL to identify derived property opportunities.

        Args:
            database: Database object with schemas and tables
            sql_queries: Optional SQL queries to analyze
            documentation: Optional documentation for context
            use_llm: Whether to use LLM for enhanced analysis

        Returns:
            List of DerivedPropertySuggestion objects
        """
        suggestions = []

        # Step 1: Pattern-based derived properties from column names
        pattern_suggestions = self._analyze_semantic_patterns(database)
        suggestions.extend(pattern_suggestions)

        # Step 2: Relationship-based aggregations
        relationship_suggestions = self._analyze_relationships(database)
        suggestions.extend(relationship_suggestions)

        # Step 3: SQL-based derived properties
        if sql_queries:
            sql_suggestions = self._analyze_sql_patterns(sql_queries, database)
            suggestions.extend(sql_suggestions)

        # Step 4: LLM-based analysis
        if use_llm:
            try:
                llm_suggestions = self._analyze_with_llm(
                    database, sql_queries, documentation
                )
                suggestions.extend(llm_suggestions)
            except Exception as e:
                print(f"Warning: LLM-based derived property analysis failed: {e}")

        # Deduplicate
        return self._deduplicate(suggestions)

    def _analyze_semantic_patterns(
        self,
        database: Database,
    ) -> List[DerivedPropertySuggestion]:
        """Generate derived properties from column naming patterns."""
        suggestions = []

        for schema in database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()
                col_names_upper = {col.name.upper() for col in table.columns}

                for prop_name, pattern_info in self.SEMANTIC_PATTERNS.items():
                    # Check if required columns exist
                    required = pattern_info["requires"]
                    all_found = True

                    for req_group in required:
                        if isinstance(req_group, tuple):
                            # Any of these columns
                            found = any(
                                any(r in col_name for r in req_group)
                                for col_name in col_names_upper
                            )
                        else:
                            found = any(req_group in col_name for col_name in col_names_upper)

                        if not found:
                            all_found = False
                            break

                    if all_found:
                        # Map column names to property names
                        expression = self._map_expression_to_properties(
                            pattern_info["expression"], table
                        )

                        suggestions.append(DerivedPropertySuggestion(
                            class_name=class_name,
                            property_name=prop_name,
                            expression=expression,
                            return_type=pattern_info["return_type"],
                            multiplicity="[1]",
                            description=pattern_info["description"],
                            confidence=0.7,
                            source=AnalysisSource.SCHEMA_PATTERN,
                        ))

        return suggestions

    def _analyze_relationships(
        self,
        database: Database,
    ) -> List[DerivedPropertySuggestion]:
        """Generate aggregation suggestions from relationships."""
        suggestions = []

        if not database.relationships:
            return suggestions

        # Build table lookup
        table_lookup = {}
        for schema in database.schemas:
            for table in schema.tables:
                table_lookup[table.name.upper()] = table

        # Group relationships by target (the "one" side in many-to-one)
        target_relationships: Dict[str, List[Relationship]] = {}
        for rel in database.relationships:
            target = rel.target_table.upper()
            if target not in target_relationships:
                target_relationships[target] = []
            target_relationships[target].append(rel)

        # Generate count properties for "one" side
        for target_table, rels in target_relationships.items():
            target = table_lookup.get(target_table)
            if not target:
                continue

            for rel in rels:
                source = table_lookup.get(rel.source_table.upper())
                if not source:
                    continue

                # Generate itemCount property
                source_class = source.get_class_name()
                target_class = target.get_class_name()

                # Pluralize source name
                source_prop = source_class[0].lower() + source_class[1:]
                if not source_prop.endswith("s"):
                    source_prop += "s"

                suggestions.append(DerivedPropertySuggestion(
                    class_name=target_class,
                    property_name=f"{source_prop}Count",
                    expression=f"$this.{source_prop}->size()",
                    return_type="Integer",
                    multiplicity="[1]",
                    description=f"Count of associated {source_class} records",
                    confidence=0.6,
                    source=AnalysisSource.SCHEMA_PATTERN,
                ))

        return suggestions

    def _analyze_sql_patterns(
        self,
        sql_queries: List[str],
        database: Database,
    ) -> List[DerivedPropertySuggestion]:
        """Extract derived properties from SQL SELECT expressions."""
        suggestions = []

        # Build table lookup
        table_lookup = {}
        for schema in database.schemas:
            for table in schema.tables:
                table_lookup[table.name.upper()] = table

        for query in sql_queries:
            # Extract aggregations
            aggregations = self._extract_aggregations(query)
            for agg in aggregations:
                table = table_lookup.get(agg.table.upper())
                if not table:
                    continue

                pure_method, return_type = self.AGGREGATION_FUNCTIONS.get(
                    agg.function.upper(), ("->sum()", "Float")
                )

                prop_name = agg.alias_name or f"{agg.column.lower()}{agg.function.title()}"
                prop_name = self._to_camel_case(prop_name)

                suggestions.append(DerivedPropertySuggestion(
                    class_name=table.get_class_name(),
                    property_name=prop_name,
                    expression=f"$this.{self._to_camel_case(agg.column)}{pure_method}",
                    return_type=return_type,
                    multiplicity="[1]",
                    description=f"{agg.function} aggregation of {agg.column}",
                    confidence=0.8,
                    source=AnalysisSource.SQL_PATTERN,
                    source_sql=query[:200],
                ))

            # Extract calculations
            calculations = self._extract_calculations(query)
            for calc in calculations:
                table = table_lookup.get(calc.table.upper())
                if not table:
                    continue

                # Convert SQL calculation to Pure
                pure_expr = self._convert_calculation_to_pure(calc.expression, table)
                if not pure_expr:
                    continue

                prop_name = calc.alias_name or "calculated"
                prop_name = self._to_camel_case(prop_name)

                suggestions.append(DerivedPropertySuggestion(
                    class_name=table.get_class_name(),
                    property_name=prop_name,
                    expression=pure_expr,
                    return_type="Float",  # Assume numeric calculation
                    multiplicity="[1]",
                    description=f"Calculated: {calc.expression}",
                    confidence=0.7,
                    source=AnalysisSource.SQL_PATTERN,
                    source_sql=query[:200],
                ))

        return suggestions

    def _analyze_with_llm(
        self,
        database: Database,
        sql_queries: Optional[List[str]],
        documentation: Optional[str],
    ) -> List[DerivedPropertySuggestion]:
        """Use LLM to identify derived properties."""
        suggestions = []

        # Format schema
        schema_info = format_schema_for_derived(database)
        relationship_info = format_relationships_for_derived(database)

        # Build prompt based on available data
        if sql_queries:
            sql_info = format_sql_for_derived(sql_queries)
            prompt = DERIVED_FROM_SQL_PROMPT.format(
                sql_queries=sql_info,
                schema_info=schema_info,
                relationship_info=relationship_info,
            )
        else:
            doc_context = ""
            if documentation:
                doc_context = DERIVED_DOCS_CONTEXT.format(doc_content=documentation)

            prompt = DERIVED_FROM_SCHEMA_PROMPT.format(
                schema_info=schema_info,
                relationship_info=relationship_info,
                doc_context=doc_context,
            )

        # Call LLM
        response = self.claude.client.messages.create(
            model=self.claude.model,
            max_tokens=4096,
            system=DERIVED_PROPERTY_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = response.content[0].text.strip()

        # Parse response
        suggestions = self._parse_llm_response(response_text)

        return suggestions

    def _parse_llm_response(self, response_text: str) -> List[DerivedPropertySuggestion]:
        """Parse LLM JSON response into DerivedPropertySuggestion objects."""
        suggestions = []

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
            print(f"Warning: Failed to parse derived property response: {e}")
            return suggestions

        if not isinstance(data, list):
            data = [data]

        for item in data:
            if isinstance(item, dict):
                try:
                    suggestions.append(DerivedPropertySuggestion(
                        class_name=item.get("class_name", ""),
                        property_name=item.get("property_name", ""),
                        expression=item.get("expression", ""),
                        return_type=item.get("return_type", "String"),
                        multiplicity=item.get("multiplicity", "[1]"),
                        description=item.get("description"),
                        confidence=float(item.get("confidence", 0.5)),
                        source=AnalysisSource.LLM_INFERENCE,
                        source_sql=item.get("source_sql"),
                    ))
                except (ValueError, TypeError) as e:
                    print(f"Warning: Skipping invalid derived property item: {e}")
                    continue

        return suggestions

    def _extract_aggregations(self, sql: str) -> List[SqlAggregation]:
        """Extract aggregation functions from SQL."""
        aggregations = []

        # Pattern for aggregation functions
        agg_pattern = r"(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\w+(?:\.\w+)?)\s*\)(?:\s+AS\s+(\w+))?"

        # Find FROM table
        from_match = re.search(r"FROM\s+(\w+(?:\.\w+)?)", sql, re.IGNORECASE)
        if not from_match:
            return aggregations

        table = from_match.group(1).split(".")[-1]

        for match in re.finditer(agg_pattern, sql, re.IGNORECASE):
            function = match.group(1).upper()
            column = match.group(2).split(".")[-1]  # Remove table prefix
            alias = match.group(3)

            aggregations.append(SqlAggregation(
                table=table,
                alias=None,
                function=function,
                column=column,
                alias_name=alias,
            ))

        return aggregations

    def _extract_calculations(self, sql: str) -> List[SqlCalculation]:
        """Extract arithmetic calculations from SQL SELECT."""
        calculations = []

        # Find FROM table
        from_match = re.search(r"FROM\s+(\w+(?:\.\w+)?)", sql, re.IGNORECASE)
        if not from_match:
            return calculations

        table = from_match.group(1).split(".")[-1]

        # Pattern for simple arithmetic in SELECT
        calc_pattern = r"(\w+)\s*([+\-*/])\s*(\w+)(?:\s+AS\s+(\w+))?"

        # Extract SELECT clause
        select_match = re.search(r"SELECT\s+(.+?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return calculations

        select_clause = select_match.group(1)

        for match in re.finditer(calc_pattern, select_clause, re.IGNORECASE):
            col1 = match.group(1)
            op = match.group(2)
            col2 = match.group(3)
            alias = match.group(4)

            # Skip if not column references
            if col1.upper() in ("SUM", "COUNT", "AVG", "MIN", "MAX"):
                continue

            calculations.append(SqlCalculation(
                table=table,
                expression=f"{col1} {op} {col2}",
                columns=[col1, col2],
                alias_name=alias,
            ))

        return calculations

    def _convert_calculation_to_pure(
        self,
        expression: str,
        table: Table,
    ) -> Optional[str]:
        """Convert SQL calculation to Pure expression."""
        # Build column mapping
        col_to_prop = {
            col.name.upper(): table.get_property_name(col.name)
            for col in table.columns
        }

        pure_expr = expression

        # Replace column references
        for col_name, prop_name in col_to_prop.items():
            pure_expr = re.sub(
                rf"\b{col_name}\b",
                f"$this.{prop_name}",
                pure_expr,
                flags=re.IGNORECASE
            )

        return pure_expr

    def _map_expression_to_properties(
        self,
        expression: str,
        table: Table,
    ) -> str:
        """Map generic expression placeholders to actual property names."""
        result = expression

        # Build property lookup
        for col in table.columns:
            prop_name = table.get_property_name(col.name)
            col_upper = col.name.upper()

            # Replace common placeholders
            if "FIRST" in col_upper and "NAME" in col_upper:
                result = result.replace("firstName", prop_name)
            if "LAST" in col_upper and "NAME" in col_upper:
                result = result.replace("lastName", prop_name)
            if "BIRTH" in col_upper or "DOB" in col_upper:
                result = result.replace("birthDate", prop_name)
            if "START" in col_upper and "DATE" in col_upper:
                result = result.replace("startDate", prop_name)
            if "END" in col_upper and "DATE" in col_upper:
                result = result.replace("endDate", prop_name)
            if "EXPIR" in col_upper and "DATE" in col_upper:
                result = result.replace("expiryDate", prop_name)

        return result

    def _to_camel_case(self, name: str) -> str:
        """Convert snake_case or UPPER_CASE to camelCase."""
        parts = name.lower().split("_")
        return parts[0] + "".join(p.capitalize() for p in parts[1:])

    def _deduplicate(
        self,
        suggestions: List[DerivedPropertySuggestion],
    ) -> List[DerivedPropertySuggestion]:
        """Remove duplicate derived property suggestions."""
        seen = set()
        unique = []

        for s in suggestions:
            key = (s.class_name, s.property_name)

            if key not in seen:
                seen.add(key)
                unique.append(s)
            else:
                # If duplicate has higher confidence, replace
                for i, existing in enumerate(unique):
                    if (existing.class_name, existing.property_name) == key:
                        if s.confidence > existing.confidence:
                            unique[i] = s
                        break

        return unique
