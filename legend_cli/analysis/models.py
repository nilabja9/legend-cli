"""Data models for enhanced schema analysis."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class AnalysisSource(str, Enum):
    """Source of an analysis suggestion."""

    SCHEMA_PATTERN = "schema_pattern"  # Detected from schema structure
    DOCUMENTATION = "documentation"  # Extracted from documentation
    SQL_PATTERN = "sql_pattern"  # Derived from SQL queries
    LLM_INFERENCE = "llm_inference"  # Inferred by LLM analysis
    DATABASE_CONSTRAINT = "database_constraint"  # From DB constraints


@dataclass
class InheritanceOpportunity:
    """Represents a detected opportunity for class inheritance.

    Identified through:
    - Common column patterns across tables (70%+ shared columns)
    - Type discriminator columns (*_TYPE, *_CATEGORY)
    - Naming conventions (e.g., SavingsAccount extends Account)
    - Documentation analysis for "is-a" relationships
    """

    base_class_name: str
    base_class_properties: List[str]
    derived_classes: List[str]
    discriminator_column: Optional[str] = None
    confidence: float = 0.0
    reasoning: str = ""
    source: AnalysisSource = AnalysisSource.LLM_INFERENCE

    # Mapping of derived class name to its additional properties
    derived_class_properties: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class EnumerationCandidate:
    """Represents a candidate for enumeration generation.

    Identified through:
    - Reference/lookup tables with < 50 rows
    - Columns with _TYPE, _STATUS, _CODE suffixes
    - Low cardinality columns (< 20 distinct values)
    - Documentation value lists
    """

    name: str
    source_table: str
    source_column: str
    values: List[str]
    confidence: float = 0.0
    description: Optional[str] = None
    source: AnalysisSource = AnalysisSource.LLM_INFERENCE

    # Mapping of enum value to description/display name
    value_descriptions: Dict[str, str] = field(default_factory=dict)


@dataclass
class ConstraintSuggestion:
    """Represents a suggested constraint for a class.

    Identified through:
    - Database CHECK constraints and UNIQUE constraints
    - Business rules extracted from documentation
    - SQL WHERE clause patterns
    - Common validation patterns (date ranges, positive values)
    """

    class_name: str
    constraint_name: str
    expression: str  # Pure expression, e.g., "$this.amount > 0"
    description: str
    confidence: float = 0.0
    source: AnalysisSource = AnalysisSource.LLM_INFERENCE

    # Original SQL expression if derived from SQL
    source_sql: Optional[str] = None


@dataclass
class DerivedPropertySuggestion:
    """Represents a suggested derived (computed) property.

    Identified through:
    - SQL aggregations (COUNT, SUM, AVG)
    - Calculated fields (price * quantity)
    - Date calculations (DATEDIFF)
    - String operations (CONCAT)
    """

    class_name: str
    property_name: str
    expression: str  # Pure expression
    return_type: str  # Pure type (String, Integer, Float, etc.)
    multiplicity: str = "[1]"  # Pure multiplicity
    description: Optional[str] = None
    confidence: float = 0.0
    source: AnalysisSource = AnalysisSource.LLM_INFERENCE

    # Original SQL expression if derived from SQL
    source_sql: Optional[str] = None


@dataclass
class TableAnalysis:
    """Analysis results for a single table."""

    table_name: str
    schema_name: str

    # Suggested enums from this table's columns
    enum_candidates: List[EnumerationCandidate] = field(default_factory=list)

    # Suggested constraints for the class generated from this table
    constraints: List[ConstraintSuggestion] = field(default_factory=list)

    # Suggested derived properties
    derived_properties: List[DerivedPropertySuggestion] = field(default_factory=list)

    # Whether this table is likely a reference/lookup table
    is_reference_table: bool = False

    # Whether this table participates in a hierarchy
    hierarchy_role: Optional[str] = None  # "base", "derived", or None


@dataclass
class EnhancedModelSpec:
    """Complete specification for enhanced model generation.

    This is the unified output of all analyzers, used by the
    EnhancedPureCodeGenerator to produce Pure code with:
    - Enumerations
    - Class hierarchies
    - Constraints
    - Derived properties
    """

    # Reference to the analyzed database schema
    database_name: str
    schema_names: List[str]

    # Detected inheritance hierarchies
    hierarchies: List[InheritanceOpportunity] = field(default_factory=list)

    # Enumeration candidates
    enumerations: List[EnumerationCandidate] = field(default_factory=list)

    # Constraint suggestions across all classes
    constraints: List[ConstraintSuggestion] = field(default_factory=list)

    # Derived property suggestions across all classes
    derived_properties: List[DerivedPropertySuggestion] = field(default_factory=list)

    # Per-table analysis details
    table_analyses: Dict[str, TableAnalysis] = field(default_factory=dict)

    # Documentation context used for analysis
    documentation: Dict[str, Any] = field(default_factory=dict)

    # SQL queries analyzed
    sql_queries: List[str] = field(default_factory=list)

    # Confidence threshold used for filtering
    confidence_threshold: float = 0.7

    def filter_by_confidence(self, threshold: Optional[float] = None) -> "EnhancedModelSpec":
        """Return a new spec with only items above the confidence threshold."""
        thresh = threshold if threshold is not None else self.confidence_threshold

        return EnhancedModelSpec(
            database_name=self.database_name,
            schema_names=self.schema_names,
            hierarchies=[h for h in self.hierarchies if h.confidence >= thresh],
            enumerations=[e for e in self.enumerations if e.confidence >= thresh],
            constraints=[c for c in self.constraints if c.confidence >= thresh],
            derived_properties=[d for d in self.derived_properties if d.confidence >= thresh],
            table_analyses=self.table_analyses,
            documentation=self.documentation,
            sql_queries=self.sql_queries,
            confidence_threshold=thresh,
        )

    def get_constraints_for_class(self, class_name: str) -> List[ConstraintSuggestion]:
        """Get all constraints for a specific class."""
        return [c for c in self.constraints if c.class_name == class_name]

    def get_derived_properties_for_class(self, class_name: str) -> List[DerivedPropertySuggestion]:
        """Get all derived properties for a specific class."""
        return [d for d in self.derived_properties if d.class_name == class_name]

    def get_base_class(self, class_name: str) -> Optional[str]:
        """Get the base class for a derived class, if any."""
        for hierarchy in self.hierarchies:
            if class_name in hierarchy.derived_classes:
                return hierarchy.base_class_name
        return None

    def is_base_class(self, class_name: str) -> bool:
        """Check if a class is a base class in any hierarchy."""
        return any(h.base_class_name == class_name for h in self.hierarchies)

    def get_enum_for_column(self, table_name: str, column_name: str) -> Optional[EnumerationCandidate]:
        """Get the enumeration candidate for a specific column."""
        for enum in self.enumerations:
            if enum.source_table == table_name and enum.source_column == column_name:
                return enum
        return None

    def summary(self) -> str:
        """Generate a human-readable summary of the analysis."""
        lines = [
            f"Enhanced Model Analysis for {self.database_name}",
            f"Schemas analyzed: {', '.join(self.schema_names)}",
            f"Confidence threshold: {self.confidence_threshold}",
            "",
            f"Hierarchies detected: {len(self.hierarchies)}",
        ]

        for h in self.hierarchies:
            lines.append(f"  - {h.base_class_name} <- {', '.join(h.derived_classes)} "
                        f"(confidence: {h.confidence:.2f})")

        lines.append(f"\nEnumerations detected: {len(self.enumerations)}")
        for e in self.enumerations:
            lines.append(f"  - {e.name}: {len(e.values)} values "
                        f"(from {e.source_table}.{e.source_column}, confidence: {e.confidence:.2f})")

        lines.append(f"\nConstraints suggested: {len(self.constraints)}")
        for c in self.constraints:
            lines.append(f"  - {c.class_name}.{c.constraint_name} (confidence: {c.confidence:.2f})")

        lines.append(f"\nDerived properties suggested: {len(self.derived_properties)}")
        for d in self.derived_properties:
            lines.append(f"  - {d.class_name}.{d.property_name}: {d.return_type} "
                        f"(confidence: {d.confidence:.2f})")

        return "\n".join(lines)
