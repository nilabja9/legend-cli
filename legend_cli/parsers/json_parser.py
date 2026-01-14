"""JSON document parser for extracting documentation content."""

import json
from pathlib import Path
from typing import Any

from .base import DocumentParser, DocumentationSource


class JsonParser(DocumentParser):
    """Parser for extracting content from JSON files.

    Supports common data dictionary formats:
    - Simple key-value pairs: {"table_name": "description"}
    - Nested structure: {"tables": [{"name": "...", "description": "...", "columns": [...]}]}
    - OpenAPI/Swagger schemas
    """

    def can_parse(self, source: str) -> bool:
        """Check if this parser can handle the given source."""
        path = Path(source)
        return path.suffix.lower() == ".json" and path.exists()

    async def parse(self, source: str) -> DocumentationSource:
        """Parse a JSON file and extract its content.

        Args:
            source: Path to JSON file

        Returns:
            DocumentationSource with extracted content
        """
        path = Path(source)

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Detect JSON format and extract accordingly
        content, detected_format = self._extract_content(data)

        metadata = {
            "file_path": source,
            "format": detected_format,
            "num_entries": self._count_entries(data),
        }

        return DocumentationSource(
            source_type="json",
            source_path=source,
            content=content,
            metadata=metadata,
        )

    def _extract_content(self, data: Any) -> tuple[str, str]:
        """Extract documentation content from JSON data.

        Returns:
            Tuple of (content_text, detected_format)
        """
        if isinstance(data, dict):
            # Check for common data dictionary formats
            if "tables" in data:
                return self._extract_tables_format(data), "tables_array"
            elif "schemas" in data or "definitions" in data:
                return self._extract_openapi_format(data), "openapi"
            elif "columns" in data or "fields" in data:
                return self._extract_single_table_format(data), "single_table"
            else:
                # Generic key-value format
                return self._extract_generic_format(data), "generic"
        elif isinstance(data, list):
            # Array of tables/entities
            return self._extract_array_format(data), "array"
        else:
            return str(data), "unknown"

    def _extract_tables_format(self, data: dict) -> str:
        """Extract from format: {"tables": [{"name": "...", "description": "...", "columns": [...]}]}"""
        lines = []
        tables = data.get("tables", [])

        for table in tables:
            if isinstance(table, dict):
                name = table.get("name", table.get("table_name", "Unknown"))
                desc = table.get("description", table.get("desc", ""))

                lines.append(f"\n## Table: {name}")
                if desc:
                    lines.append(f"Description: {desc}")

                # Extract columns
                columns = table.get("columns", table.get("fields", []))
                if columns:
                    lines.append("\nColumns:")
                    for col in columns:
                        if isinstance(col, dict):
                            col_name = col.get("name", col.get("column_name", ""))
                            col_desc = col.get("description", col.get("desc", ""))
                            col_type = col.get("type", col.get("data_type", ""))
                            lines.append(f"  - {col_name} ({col_type}): {col_desc}")
                        elif isinstance(col, str):
                            lines.append(f"  - {col}")

        return "\n".join(lines)

    def _extract_openapi_format(self, data: dict) -> str:
        """Extract from OpenAPI/Swagger schema format."""
        lines = []
        schemas = data.get("schemas", data.get("definitions", {}))

        for name, schema in schemas.items():
            if isinstance(schema, dict):
                desc = schema.get("description", "")
                lines.append(f"\n## Schema: {name}")
                if desc:
                    lines.append(f"Description: {desc}")

                # Extract properties
                properties = schema.get("properties", {})
                if properties:
                    lines.append("\nProperties:")
                    for prop_name, prop_def in properties.items():
                        if isinstance(prop_def, dict):
                            prop_desc = prop_def.get("description", "")
                            prop_type = prop_def.get("type", "")
                            lines.append(f"  - {prop_name} ({prop_type}): {prop_desc}")

        return "\n".join(lines)

    def _extract_single_table_format(self, data: dict) -> str:
        """Extract from single table format with columns/fields."""
        lines = []

        name = data.get("name", data.get("table_name", "Table"))
        desc = data.get("description", "")

        lines.append(f"## Table: {name}")
        if desc:
            lines.append(f"Description: {desc}")

        columns = data.get("columns", data.get("fields", []))
        if columns:
            lines.append("\nColumns:")
            for col in columns:
                if isinstance(col, dict):
                    col_name = col.get("name", col.get("column_name", ""))
                    col_desc = col.get("description", col.get("desc", ""))
                    lines.append(f"  - {col_name}: {col_desc}")

        return "\n".join(lines)

    def _extract_array_format(self, data: list) -> str:
        """Extract from array of tables/entities."""
        lines = []

        for item in data:
            if isinstance(item, dict):
                # Try to extract table/entity info
                name = item.get("name", item.get("table_name", item.get("entity", "")))
                desc = item.get("description", item.get("desc", ""))

                if name:
                    lines.append(f"\n## {name}")
                if desc:
                    lines.append(f"Description: {desc}")

                # Extract columns if present
                columns = item.get("columns", item.get("fields", item.get("attributes", [])))
                if columns:
                    lines.append("\nFields:")
                    for col in columns:
                        if isinstance(col, dict):
                            col_name = col.get("name", "")
                            col_desc = col.get("description", "")
                            lines.append(f"  - {col_name}: {col_desc}")

        return "\n".join(lines)

    def _extract_generic_format(self, data: dict, prefix: str = "") -> str:
        """Extract from generic key-value format."""
        lines = []

        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                # Check if it looks like a documentation entry
                if "description" in value or "desc" in value:
                    desc = value.get("description", value.get("desc", ""))
                    lines.append(f"{full_key}: {desc}")
                else:
                    # Recurse into nested dict
                    nested = self._extract_generic_format(value, full_key)
                    if nested:
                        lines.append(nested)
            elif isinstance(value, str):
                lines.append(f"{full_key}: {value}")
            elif isinstance(value, list):
                # Handle list values
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        nested = self._extract_generic_format(item, f"{full_key}[{i}]")
                        if nested:
                            lines.append(nested)

        return "\n".join(lines)

    def _count_entries(self, data: Any) -> int:
        """Count the number of documentation entries."""
        if isinstance(data, dict):
            if "tables" in data:
                return len(data.get("tables", []))
            elif "schemas" in data:
                return len(data.get("schemas", {}))
            elif "definitions" in data:
                return len(data.get("definitions", {}))
            else:
                return len(data)
        elif isinstance(data, list):
            return len(data)
        return 1
