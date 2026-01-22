"""Enhanced schema analysis module for Legend model generation.

This module provides LLM-powered analysis of database schemas to detect:
- Class inheritance hierarchies
- Enumeration candidates
- Constraint opportunities
- Derived property patterns
- Document-based relationships (ERD diagrams, SQL JOINs)

The analysis combines multiple sources:
- Database schema structure
- Documentation (URLs, PDFs, JSON)
- SQL query patterns
- ERD images (via Claude Vision)
"""

from legend_cli.analysis.models import (
    AnalysisSource,
    ConstraintSuggestion,
    DerivedPropertySuggestion,
    EnhancedModelSpec,
    EnumerationCandidate,
    InheritanceOpportunity,
    TableAnalysis,
)
from legend_cli.analysis.hierarchy_detector import HierarchyDetector
from legend_cli.analysis.enum_detector import EnumDetector
from legend_cli.analysis.constraint_analyzer import ConstraintAnalyzer, DatabaseConstraint
from legend_cli.analysis.derived_analyzer import DerivedAnalyzer
from legend_cli.analysis.schema_analyzer import (
    SchemaAnalyzer,
    AnalysisContext,
    AnalysisOptions,
    analyze_schema,
)
from legend_cli.analysis.relationship_analyzer import (
    RelationshipAnalyzer,
    DiscoveredRelationship,
    discover_relationships,
)
from legend_cli.analysis.erd_analyzer import (
    ERDAnalyzer,
    ERDRelationship,
)
from legend_cli.analysis.document_relationship_analyzer import (
    DocumentRelationshipAnalyzer,
    DocumentRelationship,
    extract_document_relationships,
)
from legend_cli.analysis.relationship_merger import (
    RelationshipMerger,
    MergeResult,
    merge_relationships,
)

__all__ = [
    # Models
    "AnalysisSource",
    "ConstraintSuggestion",
    "DerivedPropertySuggestion",
    "EnhancedModelSpec",
    "EnumerationCandidate",
    "InheritanceOpportunity",
    "TableAnalysis",
    # Detectors/Analyzers
    "HierarchyDetector",
    "EnumDetector",
    "ConstraintAnalyzer",
    "DatabaseConstraint",
    "DerivedAnalyzer",
    # Orchestrator
    "SchemaAnalyzer",
    "AnalysisContext",
    "AnalysisOptions",
    "analyze_schema",
    # Relationship Discovery
    "RelationshipAnalyzer",
    "DiscoveredRelationship",
    "discover_relationships",
    # Document-Based Relationship Analysis
    "ERDAnalyzer",
    "ERDRelationship",
    "DocumentRelationshipAnalyzer",
    "DocumentRelationship",
    "extract_document_relationships",
    "RelationshipMerger",
    "MergeResult",
    "merge_relationships",
]
