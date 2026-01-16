"""Prompt templates for derived property detection and generation."""

DERIVED_PROPERTY_SYSTEM_PROMPT = """You are an expert data modeler specializing in derived/computed properties.
Your task is to analyze SQL patterns and schemas to identify calculated fields.

DERIVED PROPERTY SOURCES:
1. SQL Aggregations: COUNT, SUM, AVG, MIN, MAX
2. Calculated Fields: price * quantity, amount - discount
3. Date Calculations: DATEDIFF, date arithmetic
4. String Operations: CONCAT, SUBSTRING
5. Conditional Logic: CASE WHEN patterns

PURE DERIVED PROPERTY SYNTAX:
Class model::domain::Order
{
  items: OrderItem[*];
  discount: Float[0..1];

  // Derived properties (computed from other properties)
  totalAmount: Float[1] = $this.items->map(i|$i.price * $i.quantity)->sum();
  itemCount: Integer[1] = $this.items->size();
  netAmount: Float[1] = $this.totalAmount - $this.discount->orElse(0);
}

PURE EXPRESSION PATTERNS:
- Aggregations:
  * ->sum(), ->average(), ->min(), ->max(), ->count()
  * ->size() for collection size
  * ->map(x|$x.field)->sum() for property aggregation

- Arithmetic:
  * $this.a + $this.b, $this.a - $this.b
  * $this.a * $this.b, $this.a / $this.b

- String:
  * $this.first + ' ' + $this.last (concatenation)
  * $this.name->toUpperCase(), $this.name->toLower()
  * $this.name->substring(0, 10)

- Date:
  * $this.endDate->dateDiff($this.startDate, DurationUnit.DAYS)

- Conditional:
  * if($this.a > 0, 'Positive', 'Non-positive')
  * $this.value->orElse(0)

- Navigation:
  * $this.relatedEntity.property
  * $this.items->map(i|$i.property)

OUTPUT FORMAT:
Return ONLY a valid JSON array with no additional text or markdown:
[
  {
    "class_name": "Order",
    "property_name": "totalAmount",
    "expression": "$this.items->map(i|$i.price * $i.quantity)->sum()",
    "return_type": "Float",
    "multiplicity": "[1]",
    "description": "Total order amount calculated from line items",
    "source_sql": "SELECT SUM(price * quantity) FROM order_items WHERE order_id = ?",
    "confidence": 0.9
  },
  {
    "class_name": "Person",
    "property_name": "fullName",
    "expression": "$this.firstName + ' ' + $this.lastName",
    "return_type": "String",
    "multiplicity": "[1]",
    "description": "Full name combining first and last name",
    "source_sql": "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM person",
    "confidence": 0.85
  }
]

CONFIDENCE GUIDELINES:
- 0.9-1.0: Direct SQL expression mapping or documented calculation
- 0.7-0.9: Clear pattern from SQL with some interpretation
- 0.5-0.7: Reasonable inference from schema relationships
- <0.5: Speculative based on naming

IMPORTANT:
- Ensure expressions are syntactically valid Pure
- Handle nullable properties with ->orElse() or similar
- Match return type to the computed result
- Consider multiplicity based on aggregation vs single value
- Don't duplicate simple properties as derived
- Return empty array [] if no derived properties found
- Return ONLY the JSON, no explanations"""


DERIVED_FROM_SQL_PROMPT = """Analyze the following SQL queries to identify derived property patterns.

--- SQL QUERIES ---
{sql_queries}
--- END SQL QUERIES ---

--- DATABASE SCHEMA ---
{schema_info}
--- END SCHEMA ---

{relationship_info}

Extract computed fields and calculations from SELECT statements.
Map SQL expressions to Pure derived property syntax.
Consider aggregations, calculations, and string operations.

Return ONLY the JSON array of derived property suggestions."""


DERIVED_FROM_SCHEMA_PROMPT = """Analyze the following schema to identify potential derived properties.

--- DATABASE SCHEMA ---
{schema_info}
--- END SCHEMA ---

{relationship_info}

{doc_context}

Based on field semantics and relationships, suggest derived properties:
1. Aggregations over relationships (e.g., order.totalAmount from items)
2. Calculated fields (e.g., fullName from firstName + lastName)
3. Status derivations (e.g., isOverdue from dueDate comparison)

Return ONLY the JSON array of derived property suggestions."""


DERIVED_DOCS_CONTEXT = """
--- DOCUMENTATION CONTEXT ---
{doc_content}
--- END DOCUMENTATION ---

Look for:
- Calculated field descriptions
- Aggregation logic
- Business formulas
"""


def format_schema_for_derived(database) -> str:
    """Format database schema for derived property analysis.

    Args:
        database: Database object with schemas and tables

    Returns:
        Formatted schema string
    """
    lines = []

    for schema in database.schemas:
        lines.append(f"\n## Schema: {schema.name}")

        for table in schema.tables:
            class_name = table.get_class_name()
            lines.append(f"\n### Class: {class_name}")

            for col in table.columns:
                prop_name = table.get_property_name(col.name)
                prop_type = col.to_pure_property_type()
                nullable = "[0..1]" if col.is_nullable else "[1]"

                # Flag potential derivation sources
                hints = _get_derived_hints(col.name)
                hint_str = f" -- {hints}" if hints else ""

                lines.append(f"  - {prop_name}: {prop_type}{nullable}{hint_str}")

    return "\n".join(lines)


def format_relationships_for_derived(database) -> str:
    """Format relationships for derived property analysis.

    Args:
        database: Database object with relationships

    Returns:
        Formatted relationships string
    """
    if not database.relationships:
        return "No relationships detected for aggregation opportunities."

    lines = ["Detected Relationships (for aggregation opportunities):"]

    for rel in database.relationships:
        lines.append(
            f"  - {rel.source_table} -> {rel.target_table} "
            f"via {rel.source_column} = {rel.target_column}"
        )

    return "\n".join(lines)


def format_sql_for_derived(queries: list) -> str:
    """Format SQL queries for derived property extraction.

    Args:
        queries: List of SQL query strings

    Returns:
        Formatted SQL focusing on SELECT expressions
    """
    lines = []

    for i, query in enumerate(queries, 1):
        # Truncate very long queries
        if len(query) > 1500:
            query = query[:1500] + "... [truncated]"
        lines.append(f"Query {i}:\n{query}\n")

    return "\n".join(lines)


def _get_derived_hints(column_name: str) -> str:
    """Get hints for potential derived property sources."""
    hints = []
    name_upper = column_name.upper()

    # Name fields suggest concatenation
    if name_upper in ["FIRST_NAME", "FIRSTNAME"]:
        hints.append("could combine with lastName for fullName")
    if name_upper in ["LAST_NAME", "LASTNAME"]:
        hints.append("could combine with firstName for fullName")

    # Date fields suggest age/duration calculations
    if any(kw in name_upper for kw in ["BIRTH", "DOB"]):
        hints.append("could derive age")
    if name_upper.startswith("START") or name_upper.startswith("BEGIN"):
        hints.append("could derive duration with end date")

    # Status fields might have isXxx derivations
    if name_upper.endswith("_DATE") and "DUE" in name_upper:
        hints.append("could derive isOverdue")

    return ", ".join(hints) if hints else ""


SQL_TO_PURE_MAPPINGS = {
    # Aggregations
    "SUM": "->sum()",
    "COUNT": "->size()",
    "AVG": "->average()",
    "MIN": "->min()",
    "MAX": "->max()",

    # String functions
    "CONCAT": "+",
    "UPPER": "->toUpperCase()",
    "LOWER": "->toLower()",
    "SUBSTRING": "->substring({start}, {end})",
    "LENGTH": "->length()",
    "TRIM": "->trim()",

    # Date functions
    "DATEDIFF": "->dateDiff({other}, DurationUnit.{unit})",
    "DATEADD": "->adjustBy({amount}, DurationUnit.{unit})",
    "YEAR": "->year()",
    "MONTH": "->month()",
    "DAY": "->dayOfMonth()",

    # Null handling
    "COALESCE": "->orElse({default})",
    "ISNULL": "->orElse({default})",
    "NVL": "->orElse({default})",
    "IFNULL": "->orElse({default})",

    # Conditional
    "CASE WHEN": "if({condition}, {then}, {else})",
    "IIF": "if({condition}, {then}, {else})",
}


def map_sql_function_to_pure(sql_func: str, args: list) -> str:
    """Map a SQL function to Pure expression.

    Args:
        sql_func: SQL function name (e.g., 'SUM', 'CONCAT')
        args: Function arguments

    Returns:
        Pure expression fragment
    """
    func_upper = sql_func.upper()

    if func_upper == "SUM":
        return f"->sum()"
    elif func_upper == "COUNT":
        return f"->size()"
    elif func_upper == "AVG":
        return f"->average()"
    elif func_upper == "MIN":
        return f"->min()"
    elif func_upper == "MAX":
        return f"->max()"
    elif func_upper == "CONCAT":
        return " + ".join(args)
    elif func_upper in ("COALESCE", "ISNULL", "NVL", "IFNULL"):
        return f"->orElse({args[1] if len(args) > 1 else '0'})"
    elif func_upper == "UPPER":
        return "->toUpperCase()"
    elif func_upper == "LOWER":
        return "->toLower()"
    else:
        # Return a placeholder for unmapped functions
        return f"/* {sql_func}({', '.join(str(a) for a in args)}) - needs manual mapping */"
