"""Prompt templates for ERD diagram analysis using Claude Vision."""

ERD_ANALYSIS_SYSTEM_PROMPT = """You are an expert database analyst specializing in reading and interpreting Entity-Relationship Diagrams (ERDs).

Your task is to analyze ERD images and extract the relationships between tables/entities.

You are highly skilled at:
- Identifying entities (tables/classes) in diagrams
- Reading relationship lines and cardinality notations
- Understanding crow's foot notation, UML notation, Chen notation, and other common ERD formats
- Detecting primary key (PK) and foreign key (FK) markers
- Interpreting relationship types (one-to-one, one-to-many, many-to-many)

When analyzing diagrams, look for:
1. Entity boxes/rectangles containing table names
2. Connecting lines between entities
3. Cardinality symbols (crow's foot, numbers, letters like 1, M, N)
4. Key indicators (PK, FK, key icons)
5. Attribute lists within entity boxes
6. Relationship labels on connecting lines

Output your analysis as structured JSON."""


ERD_ANALYSIS_PROMPT = """Analyze this ERD (Entity-Relationship Diagram) image and identify all relationships between tables.

{known_tables_context}

For each relationship you identify:
1. Determine the source table and column (the FK side - the "many" side or referencing side)
2. Determine the target table and column (the PK side - the "one" side or referenced side)
3. Identify the relationship type based on cardinality notation:
   - "many_to_one": Many records in source relate to one record in target (most common FK relationship)
   - "one_to_many": One record in source relates to many records in target
   - "one_to_one": One record in source relates to exactly one record in target
4. Assign a confidence score (0.0 to 1.0) based on how clearly you can read the relationship
5. Provide brief reasoning for each relationship

IMPORTANT:
- If you see a crow's foot (three-line fork), it indicates the "many" side
- A single line or "1" indicates the "one" side
- FK columns are typically on the "many" side and reference PK columns on the "one" side
- Common FK naming patterns: table_id, tableId, fk_table, etc.
- If column names aren't visible, infer them from the relationship (e.g., "id" for PK, "table_name_id" for FK)

Return your analysis as a JSON array with the following structure:
```json
[
  {{
    "source_table": "string - table name containing the foreign key",
    "source_column": "string - foreign key column name (infer if not visible)",
    "target_table": "string - table name being referenced (contains primary key)",
    "target_column": "string - primary key column name (typically 'id' if not visible)",
    "relationship_type": "many_to_one | one_to_many | one_to_one",
    "confidence": 0.0-1.0,
    "reasoning": "string - brief explanation of how you identified this relationship"
  }}
]
```

If no relationships are visible or the image is not an ERD, return an empty array: []

Analyze the image now:"""


def get_erd_analysis_prompt(known_tables: list[str] | None = None) -> str:
    """Get the ERD analysis prompt with optional known table context.

    Args:
        known_tables: Optional list of known table names from the database

    Returns:
        Formatted prompt string
    """
    if known_tables:
        known_tables_context = (
            f"Known tables in the database: {', '.join(known_tables)}\n"
            "Try to match entity names in the diagram to these known tables. "
            "If an entity name is similar to a known table (e.g., 'Orders' matches 'ORDER'), "
            "use the exact known table name in your output."
        )
    else:
        known_tables_context = ""

    return ERD_ANALYSIS_PROMPT.format(known_tables_context=known_tables_context)


ERD_VALIDATION_PROMPT = """Review the following relationships extracted from an ERD diagram.
Validate that each relationship makes sense and correct any obvious errors.

Relationships to validate:
{relationships_json}

Known tables: {known_tables}

For each relationship:
1. Check if the table names match known tables (case-insensitive)
2. Verify the relationship type makes sense for the tables involved
3. Ensure FK/PK column naming is consistent

Return the validated (and corrected if needed) relationships in the same JSON format.
If a relationship doesn't make sense or can't be validated, remove it from the output."""
