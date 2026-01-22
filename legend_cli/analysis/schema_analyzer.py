"""Schema analyzer orchestrator for enhanced model generation.

Coordinates all analysis components (hierarchy detector, enum detector,
constraint analyzer, derived analyzer, document relationship analyzer)
to produce a unified EnhancedModelSpec.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from legend_cli.analysis.constraint_analyzer import ConstraintAnalyzer, DatabaseConstraint
from legend_cli.analysis.derived_analyzer import DerivedAnalyzer
from legend_cli.analysis.document_relationship_analyzer import (
    DocumentRelationshipAnalyzer,
    DocumentRelationship,
)
from legend_cli.analysis.enum_detector import EnumDetector
from legend_cli.analysis.hierarchy_detector import HierarchyDetector
from legend_cli.analysis.models import (
    ConstraintSuggestion,
    DerivedPropertySuggestion,
    EnhancedModelSpec,
    EnumerationCandidate,
    InheritanceOpportunity,
    TableAnalysis,
)
from legend_cli.analysis.relationship_merger import RelationshipMerger
from legend_cli.claude_client import ClaudeClient
from legend_cli.database.models import Database, Relationship

logger = logging.getLogger(__name__)


@dataclass
class AnalysisOptions:
    """Configuration options for schema analysis."""

    # Enable/disable individual analysis types
    detect_hierarchies: bool = False  # Disabled by default - creates unmappable phantom classes
    detect_enums: bool = True
    detect_constraints: bool = False
    detect_derived: bool = False

    # Document-based relationship analysis
    analyze_document_relationships: bool = True  # Analyze ERD images and SQL JOINs from documents

    # Use LLM for enhanced detection
    use_llm: bool = True

    # Confidence threshold for filtering results
    confidence_threshold: float = 0.7

    # Maximum items per category to include
    max_hierarchies: int = 20
    max_enums: int = 50
    max_constraints: int = 100
    max_derived: int = 50


@dataclass
class AnalysisContext:
    """Context data for analysis."""

    database: Database
    documentation: Optional[str] = None
    sql_queries: Optional[List[str]] = None
    db_constraints: Optional[List[DatabaseConstraint]] = None
    sample_values: Optional[Dict[str, List[Any]]] = None
    value_fetcher: Optional[Callable[[str, str], List[Any]]] = None
    # Document sources for relationship analysis (PDFs, SQL files, etc.)
    doc_sources: Optional[List[Any]] = None  # List[DocumentationSource]


class SchemaAnalyzer:
    """Orchestrates schema analysis for enhanced model generation.

    Coordinates multiple analyzers to produce a unified EnhancedModelSpec
    that can be used by EnhancedPureCodeGenerator. Also handles document-based
    relationship discovery from ERD images and SQL JOINs.
    """

    def __init__(
        self,
        claude_client: Optional[ClaudeClient] = None,
        options: Optional[AnalysisOptions] = None,
    ):
        """Initialize the schema analyzer.

        Args:
            claude_client: Shared Claude client for LLM calls
            options: Analysis configuration options
        """
        self.claude = claude_client or ClaudeClient()
        self.options = options or AnalysisOptions()

        # Initialize component analyzers
        self.hierarchy_detector = HierarchyDetector(claude_client=self.claude)
        self.enum_detector = EnumDetector(claude_client=self.claude)
        self.constraint_analyzer = ConstraintAnalyzer(claude_client=self.claude)
        self.derived_analyzer = DerivedAnalyzer(claude_client=self.claude)

        # Document relationship analyzer (for ERD and SQL JOIN extraction)
        self.doc_relationship_analyzer = DocumentRelationshipAnalyzer()
        self.relationship_merger = RelationshipMerger()

    def analyze(
        self,
        context: AnalysisContext,
    ) -> EnhancedModelSpec:
        """Perform complete schema analysis.

        Args:
            context: Analysis context with database and supplementary data

        Returns:
            EnhancedModelSpec with all analysis results
        """
        hierarchies = []
        enumerations = []
        constraints = []
        derived_properties = []

        # Analyze document relationships if doc sources are provided
        if self.options.analyze_document_relationships and context.doc_sources:
            self._analyze_document_relationships(context)

        # Run analyses based on options
        if self.options.detect_hierarchies:
            hierarchies = self._detect_hierarchies(context)

        if self.options.detect_enums:
            enumerations = self._detect_enums(context)

        if self.options.detect_constraints:
            constraints = self._detect_constraints(context)

        if self.options.detect_derived:
            derived_properties = self._detect_derived(context)

        # Build per-table analysis
        table_analyses = self._build_table_analyses(
            context.database, enumerations, constraints, derived_properties, hierarchies
        )

        # Create spec
        spec = EnhancedModelSpec(
            database_name=context.database.name,
            schema_names=[s.name for s in context.database.schemas],
            hierarchies=hierarchies,
            enumerations=enumerations,
            constraints=constraints,
            derived_properties=derived_properties,
            table_analyses=table_analyses,
            documentation={"content": context.documentation} if context.documentation else {},
            sql_queries=context.sql_queries or [],
            confidence_threshold=self.options.confidence_threshold,
        )

        # Filter by confidence threshold
        return spec.filter_by_confidence(self.options.confidence_threshold)

    async def analyze_async(
        self,
        context: AnalysisContext,
    ) -> EnhancedModelSpec:
        """Perform schema analysis with parallel LLM calls.

        Args:
            context: Analysis context

        Returns:
            EnhancedModelSpec with all analysis results
        """
        # Prepare tasks
        tasks = []

        if self.options.detect_hierarchies:
            tasks.append(self._detect_hierarchies_async(context))
        else:
            tasks.append(asyncio.coroutine(lambda: [])())

        if self.options.detect_enums:
            tasks.append(self._detect_enums_async(context))
        else:
            tasks.append(asyncio.coroutine(lambda: [])())

        if self.options.detect_constraints:
            tasks.append(self._detect_constraints_async(context))
        else:
            tasks.append(asyncio.coroutine(lambda: [])())

        if self.options.detect_derived:
            tasks.append(self._detect_derived_async(context))
        else:
            tasks.append(asyncio.coroutine(lambda: [])())

        # Run in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Extract results (handle exceptions)
        hierarchies = results[0] if not isinstance(results[0], Exception) else []
        enumerations = results[1] if not isinstance(results[1], Exception) else []
        constraints = results[2] if not isinstance(results[2], Exception) else []
        derived_properties = results[3] if not isinstance(results[3], Exception) else []

        # Log any exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                analysis_types = ["hierarchies", "enums", "constraints", "derived"]
                print(f"Warning: {analysis_types[i]} analysis failed: {result}")

        # Build per-table analysis
        table_analyses = self._build_table_analyses(
            context.database, enumerations, constraints, derived_properties, hierarchies
        )

        # Create spec
        spec = EnhancedModelSpec(
            database_name=context.database.name,
            schema_names=[s.name for s in context.database.schemas],
            hierarchies=hierarchies,
            enumerations=enumerations,
            constraints=constraints,
            derived_properties=derived_properties,
            table_analyses=table_analyses,
            documentation={"content": context.documentation} if context.documentation else {},
            sql_queries=context.sql_queries or [],
            confidence_threshold=self.options.confidence_threshold,
        )

        return spec.filter_by_confidence(self.options.confidence_threshold)

    def _analyze_document_relationships(
        self,
        context: AnalysisContext,
    ) -> None:
        """Analyze document sources for relationships and merge with existing.

        This method extracts relationships from:
        - ERD images in PDF documents (via Claude Vision)
        - SQL JOIN patterns in documents

        The extracted relationships are merged into the database's relationships
        list, with document-derived relationships taking priority over existing
        pattern-based relationships.

        Args:
            context: Analysis context with database and doc_sources
        """
        if not context.doc_sources:
            return

        try:
            # Get known table names
            known_tables = {t.name for t in context.database.get_all_tables()}

            # Analyze documents for relationships
            logger.info("Analyzing %d document sources for relationships...", len(context.doc_sources))
            doc_relationships = self.doc_relationship_analyzer.analyze_documents_sync(
                doc_sources=context.doc_sources,
                known_tables=known_tables,
            )

            if doc_relationships:
                logger.info("Found %d relationships in documents", len(doc_relationships))

                # Merge with existing relationships (document takes priority)
                merged = self.relationship_merger.merge_into_database(
                    document_relationships=doc_relationships,
                    existing_relationships=context.database.relationships,
                )

                # Update database relationships
                context.database.relationships = merged

                logger.info(
                    "Merged relationships: %d total (was %d)",
                    len(merged),
                    len(context.database.relationships),
                )
            else:
                logger.info("No relationships found in documents")

        except Exception as e:
            logger.warning("Document relationship analysis failed: %s", e)

    def _detect_hierarchies(
        self,
        context: AnalysisContext,
    ) -> List[InheritanceOpportunity]:
        """Run hierarchy detection."""
        try:
            results = self.hierarchy_detector.detect(
                database=context.database,
                documentation=context.documentation,
                use_llm=self.options.use_llm,
            )
            return results[:self.options.max_hierarchies]
        except Exception as e:
            print(f"Warning: Hierarchy detection failed: {e}")
            return []

    async def _detect_hierarchies_async(
        self,
        context: AnalysisContext,
    ) -> List[InheritanceOpportunity]:
        """Async wrapper for hierarchy detection."""
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: self._detect_hierarchies(context)
        )

    def _detect_enums(
        self,
        context: AnalysisContext,
    ) -> List[EnumerationCandidate]:
        """Run enumeration detection."""
        try:
            results = self.enum_detector.detect(
                database=context.database,
                documentation=context.documentation,
                sample_values=context.sample_values,
                value_fetcher=context.value_fetcher,
                use_llm=self.options.use_llm,
            )
            return results[:self.options.max_enums]
        except Exception as e:
            print(f"Warning: Enum detection failed: {e}")
            return []

    async def _detect_enums_async(
        self,
        context: AnalysisContext,
    ) -> List[EnumerationCandidate]:
        """Async wrapper for enum detection."""
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: self._detect_enums(context)
        )

    def _detect_constraints(
        self,
        context: AnalysisContext,
    ) -> List[ConstraintSuggestion]:
        """Run constraint analysis."""
        try:
            results = self.constraint_analyzer.analyze(
                database=context.database,
                documentation=context.documentation,
                sql_queries=context.sql_queries,
                db_constraints=context.db_constraints,
                use_llm=self.options.use_llm,
            )
            return results[:self.options.max_constraints]
        except Exception as e:
            print(f"Warning: Constraint analysis failed: {e}")
            return []

    async def _detect_constraints_async(
        self,
        context: AnalysisContext,
    ) -> List[ConstraintSuggestion]:
        """Async wrapper for constraint analysis."""
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: self._detect_constraints(context)
        )

    def _detect_derived(
        self,
        context: AnalysisContext,
    ) -> List[DerivedPropertySuggestion]:
        """Run derived property analysis."""
        try:
            results = self.derived_analyzer.analyze(
                database=context.database,
                sql_queries=context.sql_queries,
                documentation=context.documentation,
                use_llm=self.options.use_llm,
            )
            return results[:self.options.max_derived]
        except Exception as e:
            print(f"Warning: Derived property analysis failed: {e}")
            return []

    async def _detect_derived_async(
        self,
        context: AnalysisContext,
    ) -> List[DerivedPropertySuggestion]:
        """Async wrapper for derived property analysis."""
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: self._detect_derived(context)
        )

    def _build_table_analyses(
        self,
        database: Database,
        enumerations: List[EnumerationCandidate],
        constraints: List[ConstraintSuggestion],
        derived_properties: List[DerivedPropertySuggestion],
        hierarchies: List[InheritanceOpportunity],
    ) -> Dict[str, TableAnalysis]:
        """Build per-table analysis summaries."""
        analyses = {}

        # Build hierarchy role lookup
        hierarchy_roles: Dict[str, str] = {}
        for h in hierarchies:
            hierarchy_roles[h.base_class_name] = "base"
            for derived in h.derived_classes:
                hierarchy_roles[derived] = "derived"

        for schema in database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()

                # Find enums for this table
                table_enums = [
                    e for e in enumerations
                    if e.source_table == table.name
                ]

                # Find constraints for this class
                table_constraints = [
                    c for c in constraints
                    if c.class_name == class_name
                ]

                # Find derived properties for this class
                table_derived = [
                    d for d in derived_properties
                    if d.class_name == class_name
                ]

                # Check if reference table
                is_ref = self._is_reference_table(table)

                analyses[table.name] = TableAnalysis(
                    table_name=table.name,
                    schema_name=schema.name,
                    enum_candidates=table_enums,
                    constraints=table_constraints,
                    derived_properties=table_derived,
                    is_reference_table=is_ref,
                    hierarchy_role=hierarchy_roles.get(class_name),
                )

        return analyses

    def _is_reference_table(self, table) -> bool:
        """Check if a table is likely a reference/lookup table."""
        ref_suffixes = (
            "_TYPE", "_STATUS", "_CODE", "_CATEGORY", "_LOOKUP",
            "_REF", "_REFERENCE", "_MASTER",
        )
        name_upper = table.name.upper()
        return any(name_upper.endswith(suffix) for suffix in ref_suffixes)


def analyze_schema(
    database: Database,
    documentation: Optional[str] = None,
    sql_queries: Optional[List[str]] = None,
    confidence_threshold: float = 0.7,
    use_llm: bool = True,
    claude_client: Optional[ClaudeClient] = None,
) -> EnhancedModelSpec:
    """Convenience function for schema analysis.

    Args:
        database: Database to analyze
        documentation: Optional documentation content
        sql_queries: Optional SQL queries for pattern analysis
        confidence_threshold: Minimum confidence for suggestions
        use_llm: Whether to use LLM for enhanced analysis
        claude_client: Optional Claude client

    Returns:
        EnhancedModelSpec with analysis results
    """
    options = AnalysisOptions(
        use_llm=use_llm,
        confidence_threshold=confidence_threshold,
    )

    analyzer = SchemaAnalyzer(
        claude_client=claude_client,
        options=options,
    )

    context = AnalysisContext(
        database=database,
        documentation=documentation,
        sql_queries=sql_queries,
    )

    return analyzer.analyze(context)
