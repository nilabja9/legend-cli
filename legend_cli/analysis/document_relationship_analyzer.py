"""Document-based relationship analyzer for Legend CLI.

This module orchestrates the extraction of table relationships from
various document sources:
- ERD images (via Claude Vision)
- SQL JOIN patterns (via regex parsing)
- Documentation text (via LLM analysis)

The extracted relationships can be used to supplement or override
pattern-based and LLM-inferred relationships during model generation.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import List, Literal, Optional, Set

from legend_cli.analysis.erd_analyzer import ERDAnalyzer, ERDRelationship
from legend_cli.parsers.base import DocumentationSource
from legend_cli.parsers.sql_parser import JoinRelationship, SqlJoinExtractor

logger = logging.getLogger(__name__)


@dataclass
class DocumentRelationship:
    """Represents a relationship discovered from documentation.

    This is the unified relationship model that combines relationships
    from different sources (ERD images, SQL JOINs, text analysis).
    """

    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: Literal["many_to_one", "one_to_many", "one_to_one"]
    property_name: str  # Generated property name for association
    confidence: float
    source: Literal["erd_image", "sql_join", "text"]
    reasoning: str = ""
    source_document: str = ""
    page_number: Optional[int] = None

    def to_tuple(self) -> tuple:
        """Return a tuple for deduplication purposes."""
        return (
            self.source_table.upper(),
            self.source_column.upper(),
            self.target_table.upper(),
            self.target_column.upper(),
        )

    @classmethod
    def from_erd_relationship(
        cls,
        erd_rel: ERDRelationship,
        source_document: str,
    ) -> "DocumentRelationship":
        """Create a DocumentRelationship from an ERDRelationship.

        Args:
            erd_rel: ERDRelationship from ERD analysis
            source_document: Path or identifier of source document

        Returns:
            DocumentRelationship instance
        """
        property_name = cls._generate_property_name(
            erd_rel.source_table,
            erd_rel.target_table,
            erd_rel.relationship_type,
        )

        return cls(
            source_table=erd_rel.source_table,
            source_column=erd_rel.source_column,
            target_table=erd_rel.target_table,
            target_column=erd_rel.target_column,
            relationship_type=erd_rel.relationship_type,
            property_name=property_name,
            confidence=erd_rel.confidence,
            source="erd_image",
            reasoning=erd_rel.reasoning,
            source_document=source_document,
            page_number=erd_rel.source_page,
        )

    @classmethod
    def from_join_relationship(
        cls,
        join_rel: JoinRelationship,
        source_document: str,
    ) -> "DocumentRelationship":
        """Create a DocumentRelationship from a JoinRelationship.

        Args:
            join_rel: JoinRelationship from SQL JOIN extraction
            source_document: Path or identifier of source document

        Returns:
            DocumentRelationship instance
        """
        # For JOIN relationships, we infer FK direction from naming patterns
        # The table with _id suffix is typically the FK side
        if join_rel.left_column.lower().endswith("_id"):
            source_table = join_rel.left_table
            source_column = join_rel.left_column
            target_table = join_rel.right_table
            target_column = join_rel.right_column
        elif join_rel.right_column.lower().endswith("_id"):
            source_table = join_rel.right_table
            source_column = join_rel.right_column
            target_table = join_rel.left_table
            target_column = join_rel.left_column
        else:
            # Default: left is FK side
            source_table = join_rel.left_table
            source_column = join_rel.left_column
            target_table = join_rel.right_table
            target_column = join_rel.right_column

        property_name = cls._generate_property_name(
            source_table,
            target_table,
            "many_to_one",
        )

        return cls(
            source_table=source_table,
            source_column=source_column,
            target_table=target_table,
            target_column=target_column,
            relationship_type="many_to_one",  # JOINs typically indicate FK relationships
            property_name=property_name,
            confidence=0.85,  # High confidence for SQL-derived relationships
            source="sql_join",
            reasoning=f"Extracted from SQL JOIN: {join_rel.join_type} JOIN",
            source_document=source_document,
        )

    @staticmethod
    def _generate_property_name(
        source_table: str,
        target_table: str,
        relationship_type: str,
    ) -> str:
        """Generate a property name for the association.

        Args:
            source_table: Source table name
            target_table: Target table name
            relationship_type: Type of relationship

        Returns:
            Generated property name in camelCase
        """
        # Convert target table to camelCase
        parts = target_table.lower().split("_")
        name = parts[0] + "".join(word.capitalize() for word in parts[1:])

        # For one-to-many, make it plural
        if relationship_type == "one_to_many":
            if not name.endswith("s"):
                name += "s"

        return name


class DocumentRelationshipAnalyzer:
    """Discovers relationships from document sources.

    This class orchestrates the analysis of various document types
    to extract table relationships:
    - PDF documents with ERD images
    - SQL files with JOIN queries
    - Documentation text with embedded SQL

    Example usage:
        analyzer = DocumentRelationshipAnalyzer()
        doc_sources = [pdf_source, sql_source]
        known_tables = {"ORDERS", "CUSTOMERS", "PRODUCTS"}

        relationships = await analyzer.analyze_documents(
            doc_sources=doc_sources,
            known_tables=known_tables,
        )
    """

    def __init__(
        self,
        erd_analyzer: Optional[ERDAnalyzer] = None,
    ):
        """Initialize the document relationship analyzer.

        Args:
            erd_analyzer: Optional ERDAnalyzer instance (created if not provided)
        """
        self.erd_analyzer = erd_analyzer

    def _get_erd_analyzer(self) -> ERDAnalyzer:
        """Get or create ERD analyzer instance."""
        if self.erd_analyzer is None:
            self.erd_analyzer = ERDAnalyzer()
        return self.erd_analyzer

    async def analyze_documents(
        self,
        doc_sources: List[DocumentationSource],
        known_tables: Set[str],
    ) -> List[DocumentRelationship]:
        """Analyze all documents and extract relationships.

        This method processes multiple document sources in parallel,
        extracting relationships from ERD images and SQL JOINs.

        Args:
            doc_sources: List of DocumentationSource objects to analyze
            known_tables: Set of known table names for filtering/matching

        Returns:
            List of DocumentRelationship objects (deduplicated)
        """
        all_relationships: List[DocumentRelationship] = []
        seen: Set[tuple] = set()

        # Process each document source
        tasks = []
        for source in doc_sources:
            tasks.append(self._analyze_single_source(source, known_tables))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine and deduplicate results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(
                    "Failed to analyze document %s: %s",
                    doc_sources[i].source_path,
                    result,
                )
                continue

            for rel in result:
                key = rel.to_tuple()
                if key not in seen:
                    seen.add(key)
                    all_relationships.append(rel)

        logger.info(
            "Extracted %d unique relationships from %d documents",
            len(all_relationships),
            len(doc_sources),
        )

        return all_relationships

    async def _analyze_single_source(
        self,
        source: DocumentationSource,
        known_tables: Set[str],
    ) -> List[DocumentRelationship]:
        """Analyze a single document source.

        Args:
            source: DocumentationSource to analyze
            known_tables: Set of known table names

        Returns:
            List of DocumentRelationship objects
        """
        relationships: List[DocumentRelationship] = []

        # 1. Extract relationships from images (ERD diagrams)
        if source.has_images():
            erd_rels = await self._analyze_erd_images(source, known_tables)
            relationships.extend(erd_rels)

        # 2. Extract relationships from SQL JOINs in content
        sql_rels = self._analyze_sql_content(source, known_tables)
        relationships.extend(sql_rels)

        return relationships

    async def _analyze_erd_images(
        self,
        source: DocumentationSource,
        known_tables: Set[str],
    ) -> List[DocumentRelationship]:
        """Analyze ERD images in a document.

        Args:
            source: Document source containing images
            known_tables: Known table names for matching

        Returns:
            List of DocumentRelationship from ERD analysis
        """
        if not source.images:
            return []

        # Prepare images for analysis
        images = [
            (img.page_number, img.image_data, img.image_format)
            for img in source.images
        ]

        try:
            analyzer = self._get_erd_analyzer()
            erd_relationships = await analyzer.analyze_images(
                images=images,
                known_tables=list(known_tables),
            )

            # Filter to only include relationships between known tables
            erd_relationships = analyzer.filter_by_known_tables(
                erd_relationships,
                known_tables,
                require_both=True,
            )

            # Convert to DocumentRelationship
            return [
                DocumentRelationship.from_erd_relationship(rel, source.source_path)
                for rel in erd_relationships
            ]

        except Exception as e:
            logger.warning(
                "ERD analysis failed for %s: %s",
                source.source_path,
                e,
            )
            return []

    def _analyze_sql_content(
        self,
        source: DocumentationSource,
        known_tables: Set[str],
    ) -> List[DocumentRelationship]:
        """Extract JOIN relationships from SQL content.

        Args:
            source: Document source containing text content
            known_tables: Known table names for filtering

        Returns:
            List of DocumentRelationship from SQL JOIN extraction
        """
        if not source.content:
            return []

        try:
            # Extract JOINs from document content
            join_relationships = SqlJoinExtractor.extract_from_document(source.content)

            # Convert to DocumentRelationship and filter by known tables
            known_upper = {t.upper() for t in known_tables}
            known_map = {t.upper(): t for t in known_tables}

            doc_relationships = []
            for join_rel in join_relationships:
                # Check if both tables are known
                left_upper = join_rel.left_table.upper()
                right_upper = join_rel.right_table.upper()

                if left_upper in known_upper and right_upper in known_upper:
                    # Normalize table names
                    join_rel.left_table = known_map.get(left_upper, join_rel.left_table)
                    join_rel.right_table = known_map.get(
                        right_upper, join_rel.right_table
                    )

                    doc_rel = DocumentRelationship.from_join_relationship(
                        join_rel, source.source_path
                    )
                    doc_relationships.append(doc_rel)

            logger.debug(
                "Extracted %d SQL JOIN relationships from %s",
                len(doc_relationships),
                source.source_path,
            )

            return doc_relationships

        except Exception as e:
            logger.warning(
                "SQL JOIN extraction failed for %s: %s",
                source.source_path,
                e,
            )
            return []

    def analyze_documents_sync(
        self,
        doc_sources: List[DocumentationSource],
        known_tables: Set[str],
    ) -> List[DocumentRelationship]:
        """Synchronous wrapper for analyze_documents.

        Args:
            doc_sources: List of DocumentationSource objects
            known_tables: Set of known table names

        Returns:
            List of DocumentRelationship objects
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're already in an async context
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self.analyze_documents(doc_sources, known_tables),
                    )
                    return future.result()
            else:
                return loop.run_until_complete(
                    self.analyze_documents(doc_sources, known_tables)
                )
        except RuntimeError:
            return asyncio.run(self.analyze_documents(doc_sources, known_tables))


def extract_document_relationships(
    doc_sources: List[DocumentationSource],
    known_tables: Set[str],
) -> List[DocumentRelationship]:
    """Convenience function to extract relationships from documents.

    Args:
        doc_sources: List of DocumentationSource objects
        known_tables: Set of known table names

    Returns:
        List of DocumentRelationship objects
    """
    analyzer = DocumentRelationshipAnalyzer()
    return analyzer.analyze_documents_sync(doc_sources, known_tables)
