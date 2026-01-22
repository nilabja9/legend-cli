"""Relationship merger for combining relationships from multiple sources.

This module provides functionality to merge relationships discovered from
different sources (documents, patterns, LLM inference) with appropriate
priority handling. Document-derived relationships take precedence over
pattern-based and LLM-inferred ones.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from legend_cli.analysis.document_relationship_analyzer import DocumentRelationship
from legend_cli.database.models import Relationship

logger = logging.getLogger(__name__)


@dataclass
class MergeResult:
    """Result of merging relationships from multiple sources."""

    relationships: List[Relationship]
    statistics: Dict[str, int] = field(default_factory=dict)
    conflicts_resolved: List[Dict] = field(default_factory=list)

    def summary(self) -> str:
        """Generate a summary of the merge result."""
        lines = [
            f"Total relationships: {len(self.relationships)}",
        ]

        for source, count in self.statistics.items():
            lines.append(f"  From {source}: {count}")

        if self.conflicts_resolved:
            lines.append(f"  Conflicts resolved: {len(self.conflicts_resolved)}")

        return "\n".join(lines)


class RelationshipMerger:
    """Merges relationships from multiple sources with document priority.

    This class combines relationships from:
    1. Document sources (ERD diagrams, SQL JOINs) - highest priority
    2. Pattern-based detection (FK naming conventions) - medium priority
    3. LLM inference - lowest priority

    Document-derived relationships override conflicting relationships
    from other sources when they reference the same table/column pairs.

    Example usage:
        merger = RelationshipMerger()

        result = merger.merge(
            document_relationships=doc_rels,
            pattern_relationships=pattern_rels,
            llm_relationships=llm_rels,
        )

        # Use result.relationships for the final list
    """

    # Priority scores for different sources (higher = more trusted)
    SOURCE_PRIORITY = {
        "document:erd_image": 1.0,   # Highest - explicit design diagram
        "document:sql_join": 0.95,   # Very high - actual SQL queries
        "document:text": 0.85,       # High - documentation text
        "pattern": 0.7,              # Medium - naming conventions
        "llm": 0.6,                  # Lower - inference
    }

    def __init__(
        self,
        confidence_threshold: float = 0.5,
        prefer_document_relationship_type: bool = True,
    ):
        """Initialize the relationship merger.

        Args:
            confidence_threshold: Minimum confidence for including relationships
            prefer_document_relationship_type: If True, document-derived relationship
                types (many_to_one, etc.) override other sources even when merged
        """
        self.confidence_threshold = confidence_threshold
        self.prefer_document_relationship_type = prefer_document_relationship_type

    def merge(
        self,
        document_relationships: List[DocumentRelationship],
        pattern_relationships: Optional[List[Relationship]] = None,
        llm_relationships: Optional[List[Relationship]] = None,
    ) -> MergeResult:
        """Merge relationships from all sources.

        Document relationships take priority over pattern and LLM relationships.
        When the same table/column pair appears in multiple sources, the
        highest-priority source wins.

        Args:
            document_relationships: Relationships from document analysis
            pattern_relationships: Relationships from FK pattern detection
            llm_relationships: Relationships from LLM inference

        Returns:
            MergeResult containing merged relationships and statistics
        """
        pattern_relationships = pattern_relationships or []
        llm_relationships = llm_relationships or []

        # Track relationships by their signature (source_table, source_col, target_table, target_col)
        merged: Dict[Tuple[str, str, str, str], Tuple[Relationship, float, str]] = {}
        conflicts = []
        stats = {
            "document:erd_image": 0,
            "document:sql_join": 0,
            "document:text": 0,
            "pattern": 0,
            "llm": 0,
        }

        # 1. Process document relationships first (highest priority)
        for doc_rel in document_relationships:
            if doc_rel.confidence < self.confidence_threshold:
                continue

            sig = self._get_signature(doc_rel)
            priority = self._get_priority_for_doc_source(doc_rel.source)
            rel = self._doc_rel_to_relationship(doc_rel)
            source_key = f"document:{doc_rel.source}"

            if sig in merged:
                existing_rel, existing_priority, existing_source = merged[sig]
                if priority > existing_priority:
                    conflicts.append({
                        "signature": sig,
                        "winner": source_key,
                        "loser": existing_source,
                    })
                    merged[sig] = (rel, priority, source_key)
                    if source_key in stats:
                        stats[source_key] += 1
                    if existing_source in stats:
                        stats[existing_source] -= 1
            else:
                merged[sig] = (rel, priority, source_key)
                if source_key in stats:
                    stats[source_key] += 1

        # 2. Process pattern relationships (medium priority)
        for rel in pattern_relationships:
            sig = self._get_signature_from_rel(rel)
            priority = self.SOURCE_PRIORITY["pattern"]

            if sig in merged:
                existing_rel, existing_priority, existing_source = merged[sig]
                if priority > existing_priority:
                    conflicts.append({
                        "signature": sig,
                        "winner": "pattern",
                        "loser": existing_source,
                    })
                    merged[sig] = (rel, priority, "pattern")
                    stats["pattern"] += 1
                    stats[existing_source] -= 1
                # If document source has priority, we might still want to update
                # relationship type if pattern detection is more accurate
                elif (
                    not self.prefer_document_relationship_type
                    and existing_source.startswith("document:")
                ):
                    # Keep document source but potentially update metadata
                    pass
            else:
                merged[sig] = (rel, priority, "pattern")
                stats["pattern"] += 1

        # 3. Process LLM relationships (lowest priority)
        for rel in llm_relationships:
            sig = self._get_signature_from_rel(rel)
            priority = self.SOURCE_PRIORITY["llm"]

            if sig not in merged:
                merged[sig] = (rel, priority, "llm")
                stats["llm"] += 1

        # Build final list
        final_relationships = [rel for rel, _, _ in merged.values()]

        # Clean up stats (remove zeros)
        stats = {k: v for k, v in stats.items() if v > 0}

        logger.info(
            "Merged %d relationships: %s",
            len(final_relationships),
            ", ".join(f"{k}={v}" for k, v in stats.items()),
        )

        return MergeResult(
            relationships=final_relationships,
            statistics=stats,
            conflicts_resolved=conflicts,
        )

    def merge_into_database(
        self,
        document_relationships: List[DocumentRelationship],
        existing_relationships: List[Relationship],
    ) -> List[Relationship]:
        """Merge document relationships into existing database relationships.

        This is a simpler merge that just adds document relationships
        with priority over existing ones.

        Args:
            document_relationships: Relationships from document analysis
            existing_relationships: Existing relationships (from pattern/LLM)

        Returns:
            Merged list of Relationship objects
        """
        result = self.merge(
            document_relationships=document_relationships,
            pattern_relationships=existing_relationships,
            llm_relationships=[],
        )
        return result.relationships

    def _get_signature(self, doc_rel: DocumentRelationship) -> Tuple[str, str, str, str]:
        """Get a unique signature for a document relationship."""
        return (
            doc_rel.source_table.upper(),
            doc_rel.source_column.upper(),
            doc_rel.target_table.upper(),
            doc_rel.target_column.upper(),
        )

    def _get_signature_from_rel(self, rel: Relationship) -> Tuple[str, str, str, str]:
        """Get a unique signature for a Relationship."""
        return (
            rel.source_table.upper(),
            rel.source_column.upper(),
            rel.target_table.upper(),
            rel.target_column.upper(),
        )

    def _get_priority_for_doc_source(self, source: str) -> float:
        """Get priority score for a document source type."""
        key = f"document:{source}"
        return self.SOURCE_PRIORITY.get(key, self.SOURCE_PRIORITY["document:text"])

    def _doc_rel_to_relationship(self, doc_rel: DocumentRelationship) -> Relationship:
        """Convert a DocumentRelationship to a Relationship."""
        return Relationship(
            source_table=doc_rel.source_table,
            source_column=doc_rel.source_column,
            target_table=doc_rel.target_table,
            target_column=doc_rel.target_column,
            relationship_type=doc_rel.relationship_type,
            property_name=doc_rel.property_name,
        )


def merge_relationships(
    document_relationships: List[DocumentRelationship],
    pattern_relationships: Optional[List[Relationship]] = None,
    llm_relationships: Optional[List[Relationship]] = None,
    confidence_threshold: float = 0.5,
) -> List[Relationship]:
    """Convenience function to merge relationships from all sources.

    Args:
        document_relationships: Relationships from document analysis
        pattern_relationships: Relationships from FK pattern detection
        llm_relationships: Relationships from LLM inference
        confidence_threshold: Minimum confidence threshold

    Returns:
        Merged list of Relationship objects
    """
    merger = RelationshipMerger(confidence_threshold=confidence_threshold)
    result = merger.merge(
        document_relationships=document_relationships,
        pattern_relationships=pattern_relationships,
        llm_relationships=llm_relationships,
    )
    return result.relationships
