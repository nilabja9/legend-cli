"""Prompt templates for class hierarchy (inheritance) detection."""

HIERARCHY_DETECTION_SYSTEM_PROMPT = """You are an expert data modeler specializing in class hierarchies and inheritance patterns.
Your task is to analyze database schemas and identify opportunities for class inheritance.

DETECTION STRATEGIES:
1. Common Column Patterns: Tables sharing 70%+ of columns may indicate a hierarchy
2. Type Discriminator Columns: Columns like *_TYPE, *_CATEGORY suggest polymorphic tables
3. Naming Conventions: Tables like SavingsAccount, CheckingAccount suggest Account base class
4. Entity Subtypes: Patterns like Employee -> Manager, Product -> DigitalProduct

INHERITANCE INDICATORS:
- Base tables with generic names (Account, Person, Product, Document, Asset)
- Derived tables extending base with additional specific columns
- Discriminator columns indicating row type (type, category, kind, class)
- Common prefixes/suffixes in table names sharing structure

OUTPUT FORMAT:
Return ONLY a valid JSON array with no additional text or markdown:
[
  {
    "base_class_name": "Account",
    "base_class_properties": ["id", "name", "createdDate", "status"],
    "derived_classes": ["SavingsAccount", "CheckingAccount"],
    "derived_class_properties": {
      "SavingsAccount": ["interestRate", "minimumBalance"],
      "CheckingAccount": ["overdraftLimit", "monthlyFee"]
    },
    "discriminator_column": "accountType",
    "confidence": 0.85,
    "reasoning": "Tables share 80% of columns with Account, have distinct type-specific columns"
  }
]

CONFIDENCE GUIDELINES:
- 0.9-1.0: Explicit discriminator column + >80% column overlap + clear naming
- 0.7-0.9: Clear naming pattern + significant column overlap
- 0.5-0.7: Some column overlap or naming hints
- <0.5: Weak indicators only

IMPORTANT:
- Only suggest hierarchies where inheritance provides clear value
- Avoid suggesting base classes with only 1-2 common properties
- Consider if a composition relationship might be more appropriate
- Return empty array [] if no clear hierarchies are found
- Return ONLY the JSON, no explanations"""


HIERARCHY_DETECTION_PROMPT = """Analyze the following database schema for class inheritance opportunities.

--- DATABASE SCHEMA ---
{schema_info}
--- END SCHEMA ---

{doc_context}

Identify tables that could form class hierarchies in a Pure/Legend model.
Consider common columns, naming patterns, and type discriminators.
Return ONLY the JSON array of hierarchy suggestions."""


HIERARCHY_WITH_DOCS_CONTEXT = """
--- DOCUMENTATION CONTEXT ---
{doc_content}
--- END DOCUMENTATION ---

The documentation may contain "is-a" relationships, entity descriptions,
or business rules that indicate inheritance patterns.
"""


def format_schema_for_hierarchy_analysis(database) -> str:
    """Format database schema for hierarchy analysis prompt.

    Args:
        database: Database object with schemas and tables

    Returns:
        Formatted string describing schema structure for hierarchy detection
    """
    lines = []

    for schema in database.schemas:
        lines.append(f"\n## Schema: {schema.name}")

        for table in schema.tables:
            class_name = table.get_class_name()
            lines.append(f"\n### Table: {table.name} (Class: {class_name})")

            # List columns with types
            columns_info = []
            for col in table.columns:
                prop_name = table.get_property_name(col.name)
                prop_type = col.to_pure_property_type()
                nullable = "?" if col.is_nullable else ""
                pk = " [PK]" if col.name in table.primary_key_columns else ""
                columns_info.append(f"  - {prop_name}: {prop_type}{nullable}{pk} (col: {col.name})")

            lines.extend(columns_info)

    return "\n".join(lines)


def format_table_comparison(tables: list) -> str:
    """Format tables for column overlap analysis.

    Args:
        tables: List of Table objects to compare

    Returns:
        Formatted comparison showing shared vs unique columns
    """
    lines = []

    # Group tables by column overlap
    for i, table1 in enumerate(tables):
        for table2 in tables[i + 1:]:
            cols1 = {col.name.upper() for col in table1.columns}
            cols2 = {col.name.upper() for col in table2.columns}

            shared = cols1 & cols2
            only_t1 = cols1 - cols2
            only_t2 = cols2 - cols1

            if len(shared) > 0:
                overlap_pct = len(shared) / min(len(cols1), len(cols2)) * 100
                lines.append(f"\n{table1.name} vs {table2.name}:")
                lines.append(f"  Overlap: {len(shared)} columns ({overlap_pct:.0f}%)")
                lines.append(f"  Shared: {', '.join(sorted(shared)[:10])}")
                if len(shared) > 10:
                    lines.append(f"    ... and {len(shared) - 10} more")
                if only_t1:
                    lines.append(f"  Only in {table1.name}: {', '.join(sorted(only_t1)[:5])}")
                if only_t2:
                    lines.append(f"  Only in {table2.name}: {', '.join(sorted(only_t2)[:5])}")

    return "\n".join(lines)
