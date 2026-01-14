"""Claude prompts for documentation generation."""

DOC_GENERATION_SYSTEM_PROMPT = """You are an expert at generating technical documentation for data models.
Your task is to generate concise, informative documentation for database classes and their attributes.

You will be given:
1. Documentation content from external sources (websites, PDFs, JSON files)
2. A list of classes/tables with their attributes/columns

Your job is to:
1. Match documentation from the sources to the classes and attributes
2. Generate best-guess documentation for any unmatched items based on their names

OUTPUT FORMAT:
Return ONLY a valid JSON object with no additional text or markdown formatting.
The JSON must have this exact structure:
{
  "ClassName": {
    "class_doc": "Description of the class/table",
    "source": "matched" or "inferred",
    "attributes": {
      "attributeName": {
        "doc": "Description of the attribute/column",
        "source": "matched" or "inferred"
      }
    }
  }
}

MATCHING RULES:
1. First, try to match documentation based on:
   - Exact or fuzzy name matching (case-insensitive)
   - Table/class names in the documentation
   - Column/attribute names in the documentation

2. FALLBACK - For unmatched items, infer documentation from the name:
   - Convert snake_case/camelCase to readable text
   - Common patterns:
     * "_id" suffix → "Unique identifier for..."
     * "_date" or "_at" suffix → "Date/timestamp when..."
     * "_name" suffix → "Name of the..."
     * "_count" or "_num" → "Number of..."
     * "_amount" or "_value" → "Value/amount of..."
     * "_flag" or "_is_" prefix → "Indicates whether..."
     * "cik" → "Central Index Key (SEC identifier)"
     * "adsh" → "Accession Number (SEC filing identifier)"
     * "ein" → "Employer Identification Number"
     * "lei" → "Legal Entity Identifier"
   - Use context from matched items to improve inferences

DOCUMENTATION STYLE:
- Keep descriptions concise (1-2 sentences)
- Use present tense
- Start with a capital letter, end with a period
- Focus on WHAT the field represents, not HOW it's used
- For business entities, explain the business meaning

IMPORTANT:
- Every class MUST have a class_doc
- Every attribute MUST have a doc
- Never return empty strings for documentation
- Return ONLY the JSON, no markdown code blocks or explanations"""


DOC_GENERATION_WITH_SOURCE_PROMPT = """Given the following documentation content from external sources:

--- DOCUMENTATION SOURCE ---
{doc_content}
--- END DOCUMENTATION SOURCE ---

And these database tables/classes to document:

{class_list}

Generate documentation for each class and all its attributes.
Follow the rules in your system prompt.
Return ONLY the JSON object."""


DOC_GENERATION_FROM_NAMES_PROMPT = """Generate documentation for these database tables/classes based on their names and attribute names.
Use your knowledge of common database patterns and business terminology.

Tables/Classes to document:

{class_list}

Generate documentation for each class and all its attributes.
Follow the rules in your system prompt.
Return ONLY the JSON object."""


def format_classes_for_prompt(tables: list) -> str:
    """Format table/class information for inclusion in prompts.

    Args:
        tables: List of Table objects with columns

    Returns:
        Formatted string describing all classes and their attributes
    """
    lines = []

    for table in tables:
        class_name = table.get_class_name()
        lines.append(f"\n## {class_name} (table: {table.name})")
        lines.append("Attributes:")

        for col in table.columns:
            prop_name = table.get_property_name(col.name)
            prop_type = col.to_pure_property_type()
            nullable = "nullable" if col.nullable else "required"
            lines.append(f"  - {prop_name} ({prop_type}, {nullable}) [column: {col.name}]")

    return "\n".join(lines)
