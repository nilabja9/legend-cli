"""Prompt templates for constraint generation."""

CONSTRAINT_GENERATION_SYSTEM_PROMPT = """You are an expert data modeler specializing in data quality constraints.
Your task is to analyze schemas and documentation to generate Pure language constraints.

CONSTRAINT SOURCES:
1. Database Constraints: CHECK constraints, UNIQUE constraints, NOT NULL
2. Documentation Rules: Business rules, validation requirements
3. SQL Patterns: WHERE clause conditions that represent data invariants
4. Common Patterns: Date ranges, positive values, string lengths

PURE CONSTRAINT SYNTAX:
Class model::domain::Order
{
  amount: Float[1];
  startDate: Date[1];
  endDate: Date[0..1];
}
[
  amountPositive: $this.amount > 0,
  dateRangeValid: $this.endDate->isEmpty() || $this.endDate > $this.startDate
]

PURE EXPRESSION PATTERNS:
- Comparison: $this.value > 0, $this.count >= 1
- Null checks: $this.field->isNotEmpty(), $this.field->isEmpty()
- String: $this.code->length() == 3, $this.name->startsWith('PREFIX')
- Date: $this.endDate > $this.startDate
- Collections: $this.items->size() > 0, $this.items->forAll(i|$i.qty > 0)
- Boolean: $this.isActive == true, $this.status->in(['ACTIVE', 'PENDING'])
- Compound: ($this.a > 0) && ($this.b > 0)

OUTPUT FORMAT:
Return ONLY a valid JSON array with no additional text or markdown:
[
  {
    "class_name": "Order",
    "constraint_name": "amountPositive",
    "expression": "$this.amount > 0",
    "description": "Order amount must be positive",
    "source_sql": "CHECK (amount > 0)",
    "confidence": 0.95
  },
  {
    "class_name": "DateRange",
    "constraint_name": "validRange",
    "expression": "$this.endDate->isEmpty() || $this.endDate > $this.startDate",
    "description": "End date must be after start date when specified",
    "source_sql": null,
    "confidence": 0.85
  }
]

CONFIDENCE GUIDELINES:
- 0.9-1.0: Explicit database constraint or documented rule
- 0.7-0.9: Strong pattern from SQL or clear business logic
- 0.5-0.7: Reasonable inference from field semantics
- <0.5: Speculative based on naming only

CONSTRAINT NAMING:
- Use camelCase
- Be descriptive: 'amountPositive', 'dateRangeValid', 'codeFormat'
- Avoid generic names like 'constraint1'

IMPORTANT:
- Only suggest constraints that add real value
- Ensure expressions are syntactically valid Pure
- Consider nullability when writing expressions
- Don't duplicate NOT NULL constraints (handled by multiplicity)
- Return empty array [] if no meaningful constraints found
- Return ONLY the JSON, no explanations"""


CONSTRAINT_FROM_SCHEMA_PROMPT = """Analyze the following database schema to identify constraint opportunities.

--- DATABASE SCHEMA ---
{schema_info}
--- END SCHEMA ---

{db_constraints_info}

{doc_context}

Generate Pure constraints for data validation based on:
1. Database CHECK and UNIQUE constraints
2. Field semantics (amounts should be positive, dates should be valid ranges)
3. Business logic from documentation

Return ONLY the JSON array of constraint suggestions."""


CONSTRAINT_FROM_SQL_PROMPT = """Analyze the following SQL queries to identify constraint patterns.

--- SQL QUERIES ---
{sql_queries}
--- END SQL QUERIES ---

--- DATABASE SCHEMA ---
{schema_info}
--- END SCHEMA ---

Extract data quality constraints from WHERE clauses and conditions.
Focus on patterns that represent data invariants, not query filters.

Return ONLY the JSON array of constraint suggestions."""


CONSTRAINT_DOCS_CONTEXT = """
--- DOCUMENTATION/BUSINESS RULES ---
{doc_content}
--- END DOCUMENTATION ---

Look for:
- Required field validations
- Value range restrictions
- Format requirements
- Business rules and invariants
"""


def format_schema_for_constraints(database) -> str:
    """Format database schema for constraint analysis.

    Args:
        database: Database object with schemas and tables

    Returns:
        Formatted string for constraint detection
    """
    lines = []

    for schema in database.schemas:
        lines.append(f"\n## Schema: {schema.name}")

        for table in schema.tables:
            class_name = table.get_class_name()
            lines.append(f"\n### Class: {class_name} (table: {table.name})")

            for col in table.columns:
                prop_name = table.get_property_name(col.name)
                prop_type = col.to_pure_property_type()
                nullable = "[0..1]" if col.is_nullable else "[1]"

                # Add semantic hints
                hints = _get_constraint_hints(col.name, prop_type)
                hint_str = f" -- {hints}" if hints else ""

                lines.append(f"  - {prop_name}: {prop_type}{nullable}{hint_str}")

    return "\n".join(lines)


def format_db_constraints(constraints: list) -> str:
    """Format database constraints for the prompt.

    Args:
        constraints: List of constraint definitions from DB

    Returns:
        Formatted constraints
    """
    if not constraints:
        return "No explicit database constraints found."

    lines = ["Existing Database Constraints:"]
    for c in constraints:
        lines.append(f"  - {c.get('table', 'Unknown')}: {c.get('type', 'CHECK')} - {c.get('definition', 'N/A')}")

    return "\n".join(lines)


def format_sql_for_constraints(queries: list) -> str:
    """Format SQL queries for constraint extraction.

    Args:
        queries: List of SQL query strings

    Returns:
        Formatted SQL for analysis
    """
    lines = []

    for i, query in enumerate(queries, 1):
        # Truncate very long queries
        if len(query) > 1000:
            query = query[:1000] + "... [truncated]"
        lines.append(f"Query {i}:\n{query}\n")

    return "\n".join(lines)


def _get_constraint_hints(column_name: str, pure_type: str) -> str:
    """Get semantic hints for potential constraints."""
    hints = []
    name_upper = column_name.upper()

    # Amount/value fields should be positive
    if any(kw in name_upper for kw in ["AMOUNT", "PRICE", "COST", "TOTAL", "BALANCE"]):
        hints.append("likely positive value")

    # Count fields should be non-negative
    if any(kw in name_upper for kw in ["COUNT", "QTY", "QUANTITY", "NUM"]):
        hints.append("likely non-negative")

    # Percentage fields have range 0-100
    if any(kw in name_upper for kw in ["PERCENT", "RATE", "PCT"]):
        hints.append("likely 0-100 range")

    # Date pairs suggest range validation
    if name_upper.startswith("START") or name_upper.startswith("BEGIN"):
        hints.append("likely part of date range")
    if name_upper.startswith("END") or name_upper.endswith("_TO"):
        hints.append("likely part of date range")

    # Code fields often have length constraints
    if name_upper.endswith("_CODE") or name_upper.endswith("_CD"):
        hints.append("likely fixed length")

    return ", ".join(hints) if hints else ""


COMMON_CONSTRAINT_PATTERNS = {
    "positive_amount": {
        "pattern": ["amount", "price", "cost", "total", "value", "balance"],
        "expression": "$this.{property} > 0",
        "description": "{property} must be positive",
    },
    "non_negative": {
        "pattern": ["count", "qty", "quantity", "num", "number"],
        "expression": "$this.{property} >= 0",
        "description": "{property} must be non-negative",
    },
    "percentage_range": {
        "pattern": ["percent", "rate", "pct", "percentage"],
        "expression": "$this.{property} >= 0 && $this.{property} <= 100",
        "description": "{property} must be between 0 and 100",
    },
    "date_range": {
        "pattern": [("start_date", "end_date"), ("from_date", "to_date"), ("begin_date", "end_date")],
        "expression": "$this.{end} >= $this.{start}",
        "description": "{end} must be after or equal to {start}",
    },
    "code_length": {
        "pattern": ["code", "cd"],
        "expression": "$this.{property}->length() >= 1 && $this.{property}->length() <= 10",
        "description": "{property} must be 1-10 characters",
    },
}
