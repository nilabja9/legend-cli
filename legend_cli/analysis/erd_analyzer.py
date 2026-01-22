"""ERD (Entity-Relationship Diagram) analyzer using Claude's vision capabilities.

This module provides functionality to analyze ERD images and extract
relationship information that can be used to generate database associations.
"""

import base64
import json
import logging
from dataclasses import dataclass
from typing import List, Literal, Optional, Set

from anthropic import Anthropic

from legend_cli.config import settings
from legend_cli.prompts.erd_templates import (
    ERD_ANALYSIS_SYSTEM_PROMPT,
    get_erd_analysis_prompt,
)

logger = logging.getLogger(__name__)


@dataclass
class ERDRelationship:
    """Represents a relationship extracted from an ERD diagram."""

    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: Literal["many_to_one", "one_to_many", "one_to_one"]
    confidence: float
    reasoning: str = ""
    source_page: Optional[int] = None

    def matches_known_tables(self, known_tables: Set[str]) -> bool:
        """Check if both tables in the relationship are known."""
        known_upper = {t.upper() for t in known_tables}
        return (
            self.source_table.upper() in known_upper
            and self.target_table.upper() in known_upper
        )

    def normalize_table_names(self, known_tables: Set[str]) -> "ERDRelationship":
        """Normalize table names to match known table names (case-sensitive match).

        Args:
            known_tables: Set of known table names

        Returns:
            New ERDRelationship with normalized table names
        """
        known_map = {t.upper(): t for t in known_tables}
        source = known_map.get(self.source_table.upper(), self.source_table)
        target = known_map.get(self.target_table.upper(), self.target_table)

        return ERDRelationship(
            source_table=source,
            source_column=self.source_column,
            target_table=target,
            target_column=self.target_column,
            relationship_type=self.relationship_type,
            confidence=self.confidence,
            reasoning=self.reasoning,
            source_page=self.source_page,
        )


class ERDAnalyzer:
    """Analyzes ERD diagrams using Claude's vision capabilities.

    This class takes images (extracted from PDFs or provided directly) and
    uses Claude's multimodal capabilities to identify table relationships
    depicted in the diagrams.

    Example usage:
        analyzer = ERDAnalyzer()

        # Analyze a single image
        relationships = await analyzer.analyze_image(
            image_data=png_bytes,
            image_format="png",
            known_tables=["ORDERS", "CUSTOMERS", "PRODUCTS"]
        )

        # Analyze multiple images
        all_relationships = await analyzer.analyze_images(
            images=[(1, png1, "png"), (2, png2, "png")],
            known_tables=known_tables
        )
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
    ):
        """Initialize the ERD analyzer.

        Args:
            api_key: Anthropic API key (defaults to settings)
            model: Claude model to use (defaults to settings, should support vision)
        """
        self.api_key = api_key or settings.anthropic_api_key
        self.model = model or settings.claude_model
        self._client: Optional[Anthropic] = None

    @property
    def client(self) -> Anthropic:
        """Get or create Anthropic client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError(
                    "Anthropic API key not configured. "
                    "Set ANTHROPIC_API_KEY environment variable."
                )
            self._client = Anthropic(api_key=self.api_key)
        return self._client

    async def analyze_image(
        self,
        image_data: bytes,
        image_format: str,
        known_tables: Optional[List[str]] = None,
        page_number: Optional[int] = None,
    ) -> List[ERDRelationship]:
        """Analyze a single image for ERD relationships.

        Args:
            image_data: Raw image bytes
            image_format: Image format (png, jpeg, etc.)
            known_tables: Optional list of known table names for matching
            page_number: Optional page number (for PDF sources)

        Returns:
            List of ERDRelationship objects extracted from the image
        """
        # Convert image to base64
        image_base64 = base64.standard_b64encode(image_data).decode("utf-8")

        # Determine media type
        media_type_map = {
            "png": "image/png",
            "jpeg": "image/jpeg",
            "jpg": "image/jpeg",
            "gif": "image/gif",
            "webp": "image/webp",
        }
        media_type = media_type_map.get(image_format.lower(), "image/png")

        # Build the prompt
        prompt = get_erd_analysis_prompt(known_tables)

        try:
            # Call Claude with vision
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=ERD_ANALYSIS_SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": image_base64,
                                },
                            },
                            {
                                "type": "text",
                                "text": prompt,
                            },
                        ],
                    }
                ],
            )

            # Parse the response
            response_text = response.content[0].text
            relationships = self._parse_response(response_text, page_number)

            # Normalize table names if known tables provided
            if known_tables:
                known_set = set(known_tables)
                relationships = [
                    rel.normalize_table_names(known_set) for rel in relationships
                ]

            logger.info(
                "Extracted %d relationships from image (page %s)",
                len(relationships),
                page_number or "unknown",
            )

            return relationships

        except Exception as e:
            logger.warning("Failed to analyze ERD image: %s", str(e))
            return []

    async def analyze_images(
        self,
        images: List[tuple],
        known_tables: Optional[List[str]] = None,
    ) -> List[ERDRelationship]:
        """Analyze multiple images for ERD relationships.

        Args:
            images: List of (page_number, image_data, image_format) tuples
            known_tables: Optional list of known table names

        Returns:
            Combined list of ERDRelationship objects (deduplicated)
        """
        all_relationships = []
        seen = set()

        for page_num, image_data, image_format in images:
            rels = await self.analyze_image(
                image_data=image_data,
                image_format=image_format,
                known_tables=known_tables,
                page_number=page_num,
            )

            for rel in rels:
                # Deduplicate by relationship signature
                sig = (
                    rel.source_table.upper(),
                    rel.source_column.upper(),
                    rel.target_table.upper(),
                    rel.target_column.upper(),
                )
                if sig not in seen:
                    seen.add(sig)
                    all_relationships.append(rel)

        logger.info(
            "Total unique relationships from %d images: %d",
            len(images),
            len(all_relationships),
        )

        return all_relationships

    def _parse_response(
        self,
        response_text: str,
        page_number: Optional[int] = None,
    ) -> List[ERDRelationship]:
        """Parse the LLM response into ERDRelationship objects.

        Args:
            response_text: Raw response text from Claude
            page_number: Optional page number for source tracking

        Returns:
            List of ERDRelationship objects
        """
        relationships = []

        # Try to extract JSON from the response
        try:
            # Look for JSON array in the response
            json_start = response_text.find("[")
            json_end = response_text.rfind("]") + 1

            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                data = json.loads(json_str)

                if isinstance(data, list):
                    for item in data:
                        try:
                            rel = ERDRelationship(
                                source_table=item.get("source_table", ""),
                                source_column=item.get("source_column", ""),
                                target_table=item.get("target_table", ""),
                                target_column=item.get("target_column", "id"),
                                relationship_type=item.get(
                                    "relationship_type", "many_to_one"
                                ),
                                confidence=float(item.get("confidence", 0.5)),
                                reasoning=item.get("reasoning", ""),
                                source_page=page_number,
                            )

                            # Basic validation
                            if rel.source_table and rel.target_table:
                                relationships.append(rel)

                        except (KeyError, ValueError) as e:
                            logger.debug("Skipping invalid relationship: %s", e)
                            continue

        except json.JSONDecodeError as e:
            logger.warning("Failed to parse ERD analysis response as JSON: %s", e)

        return relationships

    def filter_by_known_tables(
        self,
        relationships: List[ERDRelationship],
        known_tables: Set[str],
        require_both: bool = True,
    ) -> List[ERDRelationship]:
        """Filter relationships to only include known tables.

        Args:
            relationships: List of relationships to filter
            known_tables: Set of known table names
            require_both: If True, both tables must be known; if False, at least one

        Returns:
            Filtered list of relationships
        """
        filtered = []
        known_upper = {t.upper() for t in known_tables}

        for rel in relationships:
            source_known = rel.source_table.upper() in known_upper
            target_known = rel.target_table.upper() in known_upper

            if require_both:
                if source_known and target_known:
                    filtered.append(rel)
            else:
                if source_known or target_known:
                    filtered.append(rel)

        return filtered
