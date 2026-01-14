"""Documentation generator using Claude API."""

import json
import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path

from .claude_client import ClaudeClient
from .parsers import DocumentParser, DocumentationSource, UrlParser, PdfParser, JsonParser
from .prompts.doc_templates import (
    DOC_GENERATION_SYSTEM_PROMPT,
    DOC_GENERATION_WITH_SOURCE_PROMPT,
    DOC_GENERATION_FROM_NAMES_PROMPT,
    format_classes_for_prompt,
)


@dataclass
class PropertyDocumentation:
    """Documentation for a single property/attribute."""
    doc: str
    source: str = "inferred"  # "matched" or "inferred"


@dataclass
class ClassDocumentation:
    """Documentation for a class and its attributes."""
    class_doc: str
    source: str = "inferred"  # "matched" or "inferred"
    attributes: Dict[str, PropertyDocumentation] = field(default_factory=dict)


class DocGenerator:
    """Generates documentation for Legend classes using Claude API."""

    def __init__(self, claude_client: Optional[ClaudeClient] = None):
        """Initialize the documentation generator.

        Args:
            claude_client: ClaudeClient instance. If None, creates a new one.
        """
        self.claude = claude_client or ClaudeClient()
        self._parsers: List[DocumentParser] = [
            UrlParser(),
            PdfParser(),
            JsonParser(),
        ]

    async def parse_source(self, source: str) -> DocumentationSource:
        """Parse a documentation source (URL, PDF, or JSON).

        Args:
            source: URL or file path to parse

        Returns:
            DocumentationSource with extracted content

        Raises:
            ValueError: If no parser can handle the source
        """
        for parser in self._parsers:
            if parser.can_parse(source):
                return await parser.parse(source)

        raise ValueError(
            f"Cannot parse source: {source}. "
            f"Supported formats: URLs (http/https), PDF files, JSON files."
        )

    async def parse_sources(self, sources: List[str]) -> List[DocumentationSource]:
        """Parse multiple documentation sources.

        Args:
            sources: List of URLs or file paths

        Returns:
            List of DocumentationSource objects
        """
        tasks = [self.parse_source(source) for source in sources]
        return await asyncio.gather(*tasks)

    def generate_class_docs(
        self,
        tables: List[Any],  # List of Table objects from snowflake_client
        doc_sources: Optional[List[DocumentationSource]] = None,
        generate_fallback: bool = True,
    ) -> Dict[str, ClassDocumentation]:
        """Generate documentation for classes and their attributes.

        Args:
            tables: List of Table objects with columns
            doc_sources: Parsed documentation sources (optional)
            generate_fallback: If True, generate docs from names for unmatched items

        Returns:
            Dict mapping class names to ClassDocumentation objects
        """
        # Format classes for the prompt
        class_list = format_classes_for_prompt(tables)

        # Build the prompt
        if doc_sources:
            # Combine all documentation content
            combined_docs = self._combine_doc_sources(doc_sources)
            user_prompt = DOC_GENERATION_WITH_SOURCE_PROMPT.format(
                doc_content=combined_docs,
                class_list=class_list,
            )
        else:
            # Generate from names only
            user_prompt = DOC_GENERATION_FROM_NAMES_PROMPT.format(
                class_list=class_list,
            )

        # Call Claude API
        response = self.claude.client.messages.create(
            model=self.claude.model,
            max_tokens=8192,
            system=DOC_GENERATION_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        response_text = response.content[0].text.strip()

        # Parse JSON response
        docs = self._parse_response(response_text, tables)

        return docs

    def generate_docs_from_names_only(
        self,
        tables: List[Any],
    ) -> Dict[str, ClassDocumentation]:
        """Generate documentation purely from class/attribute names.

        Args:
            tables: List of Table objects with columns

        Returns:
            Dict mapping class names to ClassDocumentation objects
        """
        return self.generate_class_docs(tables, doc_sources=None, generate_fallback=True)

    def _combine_doc_sources(self, doc_sources: List[DocumentationSource]) -> str:
        """Combine multiple documentation sources into a single string."""
        parts = []
        for i, source in enumerate(doc_sources, 1):
            title = source.metadata.get("title", f"Source {i}")
            parts.append(f"### {title} ({source.source_type})")
            parts.append(source.content)
            parts.append("")  # Empty line between sources

        return "\n".join(parts)

    def _parse_response(
        self,
        response_text: str,
        tables: List[Any],
    ) -> Dict[str, ClassDocumentation]:
        """Parse Claude's JSON response into ClassDocumentation objects.

        Args:
            response_text: Raw response from Claude
            tables: Original tables for fallback

        Returns:
            Dict mapping class names to ClassDocumentation objects
        """
        # Try to extract JSON from response (handle markdown code blocks)
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
            # If JSON parsing fails, return empty docs with fallback
            print(f"Warning: Failed to parse Claude response as JSON: {e}")
            return self._generate_fallback_docs(tables)

        # Convert to ClassDocumentation objects
        docs = {}
        for class_name, class_data in data.items():
            if isinstance(class_data, dict):
                # Parse attributes
                attributes = {}
                attrs_data = class_data.get("attributes", {})
                for attr_name, attr_data in attrs_data.items():
                    if isinstance(attr_data, dict):
                        attributes[attr_name] = PropertyDocumentation(
                            doc=attr_data.get("doc", ""),
                            source=attr_data.get("source", "inferred"),
                        )
                    elif isinstance(attr_data, str):
                        attributes[attr_name] = PropertyDocumentation(
                            doc=attr_data,
                            source="inferred",
                        )

                docs[class_name] = ClassDocumentation(
                    class_doc=class_data.get("class_doc", ""),
                    source=class_data.get("source", "inferred"),
                    attributes=attributes,
                )

        return docs

    def _generate_fallback_docs(self, tables: List[Any]) -> Dict[str, ClassDocumentation]:
        """Generate simple fallback documentation from names.

        Used when Claude's response can't be parsed.
        """
        docs = {}
        for table in tables:
            class_name = table.get_class_name()

            # Generate class doc from name
            readable_name = self._name_to_readable(class_name)
            class_doc = f"Represents {readable_name} data."

            # Generate attribute docs
            attributes = {}
            for col in table.columns:
                prop_name = table.get_property_name(col.name)
                prop_doc = self._infer_property_doc(prop_name, col.name)
                attributes[prop_name] = PropertyDocumentation(
                    doc=prop_doc,
                    source="inferred",
                )

            docs[class_name] = ClassDocumentation(
                class_doc=class_doc,
                source="inferred",
                attributes=attributes,
            )

        return docs

    def _name_to_readable(self, name: str) -> str:
        """Convert a CamelCase or snake_case name to readable text."""
        import re
        # Handle CamelCase
        name = re.sub(r'([A-Z])', r' \1', name).strip()
        # Handle snake_case
        name = name.replace('_', ' ')
        return name.lower()

    def _infer_property_doc(self, prop_name: str, column_name: str) -> str:
        """Infer documentation for a property based on its name."""
        name_lower = column_name.lower()

        # Common patterns
        if name_lower.endswith('_id') or name_lower == 'id':
            entity = name_lower.replace('_id', '').replace('_', ' ').strip()
            if entity:
                return f"Unique identifier for the {entity}."
            return "Unique identifier."

        if name_lower.endswith('_date') or name_lower.endswith('_at'):
            event = name_lower.replace('_date', '').replace('_at', '').replace('_', ' ')
            return f"Date when {event} occurred."

        if name_lower.startswith('is_') or name_lower.endswith('_flag'):
            condition = name_lower.replace('is_', '').replace('_flag', '').replace('_', ' ')
            return f"Indicates whether {condition}."

        if name_lower.endswith('_count') or name_lower.endswith('_num'):
            entity = name_lower.replace('_count', '').replace('_num', '').replace('_', ' ')
            return f"Number of {entity}."

        if name_lower.endswith('_amount') or name_lower.endswith('_value'):
            entity = name_lower.replace('_amount', '').replace('_value', '').replace('_', ' ')
            return f"Value of {entity}."

        if name_lower.endswith('_name') or name_lower == 'name':
            entity = name_lower.replace('_name', '').replace('_', ' ').strip()
            if entity:
                return f"Name of the {entity}."
            return "Name."

        # SEC-specific patterns
        if 'cik' in name_lower:
            return "Central Index Key (SEC company identifier)."
        if 'adsh' in name_lower:
            return "Accession Number (SEC filing identifier)."
        if name_lower == 'ein':
            return "Employer Identification Number."
        if name_lower == 'lei':
            return "Legal Entity Identifier."

        # Generic fallback
        readable = self._name_to_readable(prop_name)
        return f"The {readable}."
