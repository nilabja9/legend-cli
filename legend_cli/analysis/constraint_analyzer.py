"""Constraint analyzer for generating Pure constraints from schemas and documentation."""

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from legend_cli.analysis.models import AnalysisSource, ConstraintSuggestion
from legend_cli.claude_client import ClaudeClient
from legend_cli.database.models import Database, Table
from legend_cli.prompts.constraint_templates import (
    CONSTRAINT_DOCS_CONTEXT,
    CONSTRAINT_FROM_SCHEMA_PROMPT,
    CONSTRAINT_FROM_SQL_PROMPT,
    CONSTRAINT_GENERATION_SYSTEM_PROMPT,
    format_db_constraints,
    format_schema_for_constraints,
    format_sql_for_constraints,
)


@dataclass
class DatabaseConstraint:
    """Represents a constraint from the database metadata."""

    table: str
    constraint_type: str  # CHECK, UNIQUE, NOT NULL
    definition: str
    columns: List[str]


class ConstraintAnalyzer:
    """Analyzes schemas and documentation to generate Pure constraints.

    Identifies constraints from:
    - Database CHECK constraints and UNIQUE constraints
    - Business rules extracted from documentation
    - SQL WHERE clause patterns
    - Common validation patterns (date ranges, positive values)
    """

    # Common constraint patterns based on column semantics
    SEMANTIC_PATTERNS = {
        # Positive value constraints
        "positive": {
            "patterns": ["AMOUNT", "PRICE", "COST", "TOTAL", "BALANCE", "FEE", "VALUE"],
            "expression": "$this.{property} > 0",
            "description": "{property} must be positive",
        },
        # Non-negative constraints
        "non_negative": {
            "patterns": ["COUNT", "QTY", "QUANTITY", "NUM", "NUMBER"],
            "expression": "$this.{property} >= 0",
            "description": "{property} must be non-negative",
        },
        # Percentage constraints
        "percentage": {
            "patterns": ["PERCENT", "RATE", "PCT", "PERCENTAGE"],
            "expression": "$this.{property} >= 0 && $this.{property} <= 100",
            "description": "{property} must be between 0 and 100",
        },
        # Length constraints for codes
        "code_length": {
            "patterns": ["_CODE", "_CD"],
            "expression": "$this.{property}->length() >= 1",
            "description": "{property} must not be empty",
        },
    }

    # Date range patterns (pairs of columns)
    DATE_RANGE_PATTERNS = [
        ("START_DATE", "END_DATE"),
        ("BEGIN_DATE", "END_DATE"),
        ("FROM_DATE", "TO_DATE"),
        ("EFFECTIVE_DATE", "EXPIRY_DATE"),
        ("VALID_FROM", "VALID_TO"),
        ("START_TIME", "END_TIME"),
    ]

    def __init__(
        self,
        claude_client: Optional[ClaudeClient] = None,
    ):
        """Initialize the constraint analyzer.

        Args:
            claude_client: ClaudeClient for LLM-based analysis
        """
        self.claude = claude_client or ClaudeClient()

    def analyze(
        self,
        database: Database,
        documentation: Optional[str] = None,
        sql_queries: Optional[List[str]] = None,
        db_constraints: Optional[List[DatabaseConstraint]] = None,
        use_llm: bool = True,
    ) -> List[ConstraintSuggestion]:
        """Analyze schema and documentation to generate constraint suggestions.

        Args:
            database: Database object with schemas and tables
            documentation: Optional documentation/business rules
            sql_queries: Optional SQL queries to analyze for patterns
            db_constraints: Optional database constraints from metadata
            use_llm: Whether to use LLM for enhanced analysis

        Returns:
            List of ConstraintSuggestion objects
        """
        suggestions = []

        # Step 1: Semantic pattern-based constraints
        pattern_constraints = self._analyze_semantic_patterns(database)
        suggestions.extend(pattern_constraints)

        # Step 2: Date range constraints
        date_constraints = self._analyze_date_ranges(database)
        suggestions.extend(date_constraints)

        # Step 3: Database constraint conversion
        if db_constraints:
            db_suggestions = self._convert_db_constraints(db_constraints, database)
            suggestions.extend(db_suggestions)

        # Step 4: SQL pattern analysis
        if sql_queries:
            sql_constraints = self._analyze_sql_patterns(sql_queries, database)
            suggestions.extend(sql_constraints)

        # Step 5: LLM-based analysis
        if use_llm:
            try:
                llm_constraints = self._analyze_with_llm(
                    database, documentation, sql_queries, db_constraints
                )
                suggestions.extend(llm_constraints)
            except Exception as e:
                print(f"Warning: LLM-based constraint analysis failed: {e}")

        # Deduplicate
        return self._deduplicate(suggestions)

    def _analyze_semantic_patterns(
        self,
        database: Database,
    ) -> List[ConstraintSuggestion]:
        """Generate constraints from column naming patterns."""
        suggestions = []

        for schema in database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()

                for col in table.columns:
                    col_upper = col.name.upper()
                    prop_name = table.get_property_name(col.name)

                    for pattern_type, pattern_info in self.SEMANTIC_PATTERNS.items():
                        for pattern in pattern_info["patterns"]:
                            if pattern in col_upper:
                                # Generate constraint
                                constraint_name = f"{prop_name}{pattern_type.replace('_', '').title()}"

                                suggestions.append(ConstraintSuggestion(
                                    class_name=class_name,
                                    constraint_name=constraint_name,
                                    expression=pattern_info["expression"].format(property=prop_name),
                                    description=pattern_info["description"].format(property=prop_name),
                                    confidence=0.6,
                                    source=AnalysisSource.SCHEMA_PATTERN,
                                ))
                                break  # Only one constraint per column per pattern type

        return suggestions

    def _analyze_date_ranges(
        self,
        database: Database,
    ) -> List[ConstraintSuggestion]:
        """Generate date range constraints for date column pairs."""
        suggestions = []

        for schema in database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()
                col_names = {col.name.upper(): col for col in table.columns}

                for start_pattern, end_pattern in self.DATE_RANGE_PATTERNS:
                    start_col = None
                    end_col = None

                    # Find matching columns
                    for col_name, col in col_names.items():
                        if start_pattern in col_name or col_name.endswith(start_pattern):
                            start_col = col
                        if end_pattern in col_name or col_name.endswith(end_pattern):
                            end_col = col

                    if start_col and end_col:
                        start_prop = table.get_property_name(start_col.name)
                        end_prop = table.get_property_name(end_col.name)

                        # Handle nullable end date
                        if end_col.is_nullable:
                            expression = (
                                f"$this.{end_prop}->isEmpty() || "
                                f"$this.{end_prop} >= $this.{start_prop}"
                            )
                        else:
                            expression = f"$this.{end_prop} >= $this.{start_prop}"

                        suggestions.append(ConstraintSuggestion(
                            class_name=class_name,
                            constraint_name=f"{start_prop}{end_prop}Valid",
                            expression=expression,
                            description=f"{end_prop} must be after or equal to {start_prop}",
                            confidence=0.8,
                            source=AnalysisSource.SCHEMA_PATTERN,
                        ))

        return suggestions

    def _convert_db_constraints(
        self,
        db_constraints: List[DatabaseConstraint],
        database: Database,
    ) -> List[ConstraintSuggestion]:
        """Convert database constraints to Pure constraints."""
        suggestions = []

        # Build table lookup
        table_lookup = {}
        for schema in database.schemas:
            for table in schema.tables:
                table_lookup[table.name.upper()] = table

        for constraint in db_constraints:
            table = table_lookup.get(constraint.table.upper())
            if not table:
                continue

            class_name = table.get_class_name()

            if constraint.constraint_type == "CHECK":
                # Try to convert CHECK constraint to Pure
                pure_expr = self._convert_check_to_pure(
                    constraint.definition, table
                )
                if pure_expr:
                    suggestions.append(ConstraintSuggestion(
                        class_name=class_name,
                        constraint_name=self._generate_constraint_name(constraint),
                        expression=pure_expr,
                        description=f"Database CHECK: {constraint.definition}",
                        confidence=0.9,
                        source=AnalysisSource.DATABASE_CONSTRAINT,
                        source_sql=constraint.definition,
                    ))

            elif constraint.constraint_type == "UNIQUE":
                # Generate uniqueness check (usually handled differently in Pure)
                # Include as informational
                columns = ", ".join(constraint.columns)
                suggestions.append(ConstraintSuggestion(
                    class_name=class_name,
                    constraint_name=f"unique{columns.replace(', ', 'And').title()}",
                    expression=f"/* UNIQUE constraint on: {columns} */",
                    description=f"Unique constraint on {columns}",
                    confidence=0.5,  # Lower confidence as Pure handles uniqueness differently
                    source=AnalysisSource.DATABASE_CONSTRAINT,
                ))

        return suggestions

    def _analyze_sql_patterns(
        self,
        sql_queries: List[str],
        database: Database,
    ) -> List[ConstraintSuggestion]:
        """Extract constraints from SQL WHERE clause patterns."""
        suggestions = []

        # Build table lookup
        table_lookup = {}
        for schema in database.schemas:
            for table in schema.tables:
                table_lookup[table.name.upper()] = table

        for query in sql_queries:
            # Extract WHERE clauses
            where_patterns = self._extract_where_patterns(query)

            for table_name, condition in where_patterns:
                table = table_lookup.get(table_name.upper())
                if not table:
                    continue

                class_name = table.get_class_name()

                # Try to convert SQL condition to Pure
                pure_expr = self._convert_sql_condition_to_pure(condition, table)
                if pure_expr:
                    suggestions.append(ConstraintSuggestion(
                        class_name=class_name,
                        constraint_name=self._generate_constraint_name_from_sql(condition),
                        expression=pure_expr,
                        description=f"Derived from SQL: {condition[:100]}",
                        confidence=0.7,
                        source=AnalysisSource.SQL_PATTERN,
                        source_sql=condition,
                    ))

        return suggestions

    def _analyze_with_llm(
        self,
        database: Database,
        documentation: Optional[str],
        sql_queries: Optional[List[str]],
        db_constraints: Optional[List[DatabaseConstraint]],
    ) -> List[ConstraintSuggestion]:
        """Use LLM to analyze and generate constraints."""
        suggestions = []

        # Format schema
        schema_info = format_schema_for_constraints(database)

        # Format DB constraints if available
        db_info = ""
        if db_constraints:
            db_info = format_db_constraints([
                {"table": c.table, "type": c.constraint_type, "definition": c.definition}
                for c in db_constraints
            ])

        # Build prompt based on available data
        if sql_queries:
            sql_info = format_sql_for_constraints(sql_queries)
            prompt = CONSTRAINT_FROM_SQL_PROMPT.format(
                sql_queries=sql_info,
                schema_info=schema_info,
            )
        else:
            doc_context = ""
            if documentation:
                doc_context = CONSTRAINT_DOCS_CONTEXT.format(doc_content=documentation)

            prompt = CONSTRAINT_FROM_SCHEMA_PROMPT.format(
                schema_info=schema_info,
                db_constraints_info=db_info,
                doc_context=doc_context,
            )

        # Call LLM
        response = self.claude.client.messages.create(
            model=self.claude.model,
            max_tokens=4096,
            system=CONSTRAINT_GENERATION_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = response.content[0].text.strip()

        # Parse response
        suggestions = self._parse_llm_response(response_text)

        return suggestions

    def _parse_llm_response(self, response_text: str) -> List[ConstraintSuggestion]:
        """Parse LLM JSON response into ConstraintSuggestion objects."""
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
            print(f"Warning: Failed to parse constraint analysis response: {e}")
            return suggestions

        if not isinstance(data, list):
            data = [data]

        for item in data:
            if isinstance(item, dict):
                try:
                    suggestions.append(ConstraintSuggestion(
                        class_name=item.get("class_name", ""),
                        constraint_name=item.get("constraint_name", ""),
                        expression=item.get("expression", ""),
                        description=item.get("description", ""),
                        confidence=float(item.get("confidence", 0.5)),
                        source=AnalysisSource.LLM_INFERENCE,
                        source_sql=item.get("source_sql"),
                    ))
                except (ValueError, TypeError) as e:
                    print(f"Warning: Skipping invalid constraint item: {e}")
                    continue

        return suggestions

    def _convert_check_to_pure(
        self,
        check_definition: str,
        table: Table,
    ) -> Optional[str]:
        """Convert SQL CHECK constraint to Pure expression."""
        # Basic conversion patterns
        expr = check_definition.strip()

        # Remove CHECK keyword if present
        expr = re.sub(r"^\s*CHECK\s*\(", "", expr, flags=re.IGNORECASE)
        expr = re.sub(r"\)\s*$", "", expr)

        # Build column to property mapping
        col_to_prop = {
            col.name.upper(): table.get_property_name(col.name)
            for col in table.columns
        }

        # Replace column references with $this.property
        for col_name, prop_name in col_to_prop.items():
            # Match word boundaries
            expr = re.sub(
                rf"\b{col_name}\b",
                f"$this.{prop_name}",
                expr,
                flags=re.IGNORECASE
            )

        # Convert SQL operators to Pure
        expr = expr.replace(" AND ", " && ")
        expr = expr.replace(" OR ", " || ")
        expr = expr.replace("<>", "!=")

        # Handle IS NOT NULL -> ->isNotEmpty()
        expr = re.sub(
            r"\$this\.(\w+)\s+IS\s+NOT\s+NULL",
            r"$this.\1->isNotEmpty()",
            expr,
            flags=re.IGNORECASE
        )

        # Handle IS NULL -> ->isEmpty()
        expr = re.sub(
            r"\$this\.(\w+)\s+IS\s+NULL",
            r"$this.\1->isEmpty()",
            expr,
            flags=re.IGNORECASE
        )

        # Handle IN lists
        expr = re.sub(
            r"\$this\.(\w+)\s+IN\s*\(([^)]+)\)",
            r"$this.\1->in([\2])",
            expr,
            flags=re.IGNORECASE
        )

        # Handle BETWEEN
        expr = re.sub(
            r"\$this\.(\w+)\s+BETWEEN\s+(\S+)\s+AND\s+(\S+)",
            r"$this.\1 >= \2 && $this.\1 <= \3",
            expr,
            flags=re.IGNORECASE
        )

        return expr

    def _convert_sql_condition_to_pure(
        self,
        condition: str,
        table: Table,
    ) -> Optional[str]:
        """Convert SQL WHERE condition to Pure expression."""
        # Use same logic as CHECK conversion
        return self._convert_check_to_pure(condition, table)

    def _extract_where_patterns(
        self,
        sql: str,
    ) -> List[Tuple[str, str]]:
        """Extract WHERE clause patterns from SQL.

        Returns list of (table_name, condition) tuples.
        """
        results = []

        # Simple pattern matching for WHERE clauses
        # This is a basic implementation - a proper SQL parser would be better

        # Find FROM table
        from_match = re.search(
            r"FROM\s+(\w+(?:\.\w+)?)\s+",
            sql,
            re.IGNORECASE
        )

        if not from_match:
            return results

        table_name = from_match.group(1).split(".")[-1]

        # Find WHERE clause
        where_match = re.search(
            r"WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)",
            sql,
            re.IGNORECASE | re.DOTALL
        )

        if where_match:
            conditions = where_match.group(1).strip()

            # Split by AND (simple split, not handling nested conditions)
            for cond in re.split(r"\s+AND\s+", conditions, flags=re.IGNORECASE):
                cond = cond.strip()
                # Skip conditions with parameters or subqueries
                if "?" not in cond and "SELECT" not in cond.upper():
                    results.append((table_name, cond))

        return results

    def _generate_constraint_name(self, constraint: DatabaseConstraint) -> str:
        """Generate a constraint name from database constraint."""
        # Try to extract meaningful name from definition
        definition = constraint.definition.lower()

        if ">" in definition:
            return "valuePositive"
        if ">=" in definition:
            return "valueNonNegative"
        if "between" in definition:
            return "valueInRange"
        if "in" in definition:
            return "valueInSet"

        # Fallback
        return f"{constraint.constraint_type.lower()}Constraint"

    def _generate_constraint_name_from_sql(self, condition: str) -> str:
        """Generate constraint name from SQL condition."""
        condition_lower = condition.lower()

        if ">" in condition and ">=" not in condition:
            return "valuePositive"
        if ">=" in condition:
            return "valueNonNegative"
        if "between" in condition_lower:
            return "valueInRange"
        if " in " in condition_lower:
            return "valueInSet"
        if "not null" in condition_lower:
            return "valueRequired"

        return "sqlDerivedConstraint"

    def _deduplicate(
        self,
        suggestions: List[ConstraintSuggestion],
    ) -> List[ConstraintSuggestion]:
        """Remove duplicate constraints."""
        seen = set()
        unique = []

        for s in suggestions:
            # Key by class + expression (normalized)
            key = (s.class_name, s.expression.replace(" ", ""))

            if key not in seen:
                seen.add(key)
                unique.append(s)
            else:
                # If duplicate has higher confidence, replace
                for i, existing in enumerate(unique):
                    existing_key = (existing.class_name, existing.expression.replace(" ", ""))
                    if existing_key == key and s.confidence > existing.confidence:
                        unique[i] = s
                        break

        return unique
