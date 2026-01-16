"""Prompt templates for enumeration detection."""

ENUM_DETECTION_SYSTEM_PROMPT = """You are an expert data modeler specializing in identifying enumeration patterns.
Your task is to analyze database schemas and identify columns that should be modeled as enumerations.

DETECTION STRATEGIES:
1. Reference/Lookup Tables: Small tables (<50 rows) with code/description pattern
2. Column Naming: Columns with _TYPE, _STATUS, _CODE, _CATEGORY suffixes
3. Low Cardinality: Columns with limited distinct values (<20)
4. Documentation: Value lists or allowed values in documentation

ENUMERATION INDICATORS:
- Table names ending in _TYPE, _STATUS, _CODE, _CATEGORY, _LOOKUP, _REF
- Small reference tables with (code, name/description) structure
- Columns that clearly have finite, predefined values
- Business domain enums (OrderStatus, PaymentMethod, AccountType)

ENUM VALUE RULES:
- Convert to valid Pure enum values: UPPER_SNAKE_CASE
- Remove special characters, replace spaces with underscores
- Prefix with letter if starts with number (VALUE_123)
- Keep values meaningful and readable

OUTPUT FORMAT:
Return ONLY a valid JSON array with no additional text or markdown:
[
  {
    "name": "OrderStatus",
    "source_table": "ORDER_STATUS_REF",
    "source_column": "STATUS_CODE",
    "values": ["PENDING", "ACTIVE", "COMPLETED", "CANCELLED"],
    "value_descriptions": {
      "PENDING": "Order is awaiting processing",
      "ACTIVE": "Order is being fulfilled",
      "COMPLETED": "Order has been delivered",
      "CANCELLED": "Order was cancelled"
    },
    "description": "Represents the lifecycle status of an order",
    "confidence": 0.95,
    "is_reference_table": true
  },
  {
    "name": "PaymentMethod",
    "source_table": "ORDERS",
    "source_column": "PAYMENT_TYPE",
    "values": ["CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", "CASH"],
    "value_descriptions": {},
    "description": "Method of payment for the order",
    "confidence": 0.8,
    "is_reference_table": false
  }
]

CONFIDENCE GUIDELINES:
- 0.9-1.0: Dedicated reference table or documented enum values
- 0.7-0.9: Clear naming pattern + limited known values
- 0.5-0.7: Column naming suggests enum but values unknown
- <0.5: Might be enum but uncertain

IMPORTANT:
- Only suggest enums where the value set is truly finite and stable
- Avoid suggesting enums for IDs, dates, free-text fields
- Consider if values represent a meaningful business concept
- Return empty array [] if no clear enumerations found
- Return ONLY the JSON, no explanations"""


ENUM_DETECTION_PROMPT = """Analyze the following database schema for enumeration opportunities.

--- DATABASE SCHEMA ---
{schema_info}
--- END SCHEMA ---

{reference_tables_info}

{doc_context}

Identify columns and tables that should be modeled as Pure enumerations.
Consider reference tables, column naming patterns, and documentation.
Return ONLY the JSON array of enumeration suggestions."""


ENUM_WITH_VALUES_PROMPT = """Analyze the following database schema and sample values for enumeration opportunities.

--- DATABASE SCHEMA ---
{schema_info}
--- END SCHEMA ---

--- SAMPLE VALUES ---
{sample_values}
--- END SAMPLE VALUES ---

{doc_context}

Based on the schema structure and sample values, identify columns that should be enumerations.
Return ONLY the JSON array of enumeration suggestions with the actual values."""


ENUM_DOCS_CONTEXT = """
--- DOCUMENTATION CONTEXT ---
{doc_content}
--- END DOCUMENTATION ---

Look for:
- Allowed values for fields
- Status/type definitions
- Code lists or reference data descriptions
"""


def format_schema_for_enum_analysis(database) -> str:
    """Format database schema for enumeration analysis.

    Args:
        database: Database object with schemas and tables

    Returns:
        Formatted string for enum detection
    """
    lines = []

    for schema in database.schemas:
        lines.append(f"\n## Schema: {schema.name}")

        for table in schema.tables:
            # Flag potential reference tables
            is_ref = _is_reference_table_name(table.name)
            ref_marker = " [LIKELY REFERENCE TABLE]" if is_ref else ""

            lines.append(f"\n### Table: {table.name}{ref_marker}")

            for col in table.columns:
                # Flag columns that might be enums
                is_enum_col = _is_enum_column_name(col.name)
                enum_marker = " [ENUM CANDIDATE]" if is_enum_col else ""

                prop_name = table.get_property_name(col.name)
                prop_type = col.to_pure_property_type()
                lines.append(f"  - {prop_name}: {prop_type}{enum_marker}")

    return "\n".join(lines)


def format_reference_tables(database) -> str:
    """Identify and format reference/lookup tables.

    Args:
        database: Database object

    Returns:
        Formatted string listing likely reference tables
    """
    lines = ["Potential Reference/Lookup Tables:"]

    for schema in database.schemas:
        for table in schema.tables:
            if _is_reference_table_name(table.name):
                col_count = len(table.columns)
                lines.append(f"  - {schema.name}.{table.name} ({col_count} columns)")

    if len(lines) == 1:
        lines.append("  None identified by naming pattern")

    return "\n".join(lines)


def format_sample_values(value_samples: dict) -> str:
    """Format sample values for enum detection.

    Args:
        value_samples: Dict of {table.column: [sample values]}

    Returns:
        Formatted sample values
    """
    lines = []

    for col_key, values in value_samples.items():
        unique_count = len(set(values))
        lines.append(f"\n{col_key}: {unique_count} unique values")
        # Show first 20 unique values
        unique_vals = list(set(values))[:20]
        lines.append(f"  Values: {', '.join(str(v) for v in unique_vals)}")
        if len(set(values)) > 20:
            lines.append(f"  ... and {len(set(values)) - 20} more")

    return "\n".join(lines)


def _is_reference_table_name(table_name: str) -> bool:
    """Check if table name suggests a reference/lookup table."""
    suffixes = (
        "_TYPE", "_STATUS", "_CODE", "_CATEGORY", "_LOOKUP",
        "_REF", "_REFERENCE", "_CODES", "_TYPES", "_ENUM",
        "_LIST", "_VALUES", "_OPTIONS", "_MASTER"
    )
    name_upper = table_name.upper()
    return any(name_upper.endswith(suffix) for suffix in suffixes)


def _is_enum_column_name(column_name: str) -> bool:
    """Check if column name suggests an enumeration."""
    suffixes = (
        "_TYPE", "_STATUS", "_CODE", "_CATEGORY", "_KIND",
        "_CLASS", "_MODE", "_STATE", "_LEVEL", "_PRIORITY",
        "_METHOD", "_REASON", "_SOURCE"
    )
    name_upper = column_name.upper()
    return any(name_upper.endswith(suffix) for suffix in suffixes)


def normalize_enum_value(value: str) -> str:
    """Convert a value to valid Pure enum format (UPPER_SNAKE_CASE).

    Args:
        value: Raw value from database

    Returns:
        Valid Pure enumeration value
    """
    if not value:
        return "UNKNOWN"

    # Convert to string and uppercase
    result = str(value).upper()

    # Replace spaces and hyphens with underscores
    result = result.replace(" ", "_").replace("-", "_")

    # Remove invalid characters (keep alphanumeric and underscore)
    result = "".join(c if c.isalnum() or c == "_" else "" for c in result)

    # Collapse multiple underscores
    while "__" in result:
        result = result.replace("__", "_")

    # Strip leading/trailing underscores
    result = result.strip("_")

    # Prefix with 'VALUE_' if starts with number
    if result and result[0].isdigit():
        result = f"VALUE_{result}"

    return result or "UNKNOWN"
