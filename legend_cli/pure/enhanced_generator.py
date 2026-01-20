"""Enhanced Pure code generator with support for advanced modeling features.

Extends the base PureCodeGenerator to support:
- Class inheritance (extends)
- Constraints
- Enumerations
- Derived properties
"""

from typing import Any, Dict, List, Optional, Set

from legend_cli.analysis.models import (
    ConstraintSuggestion,
    DerivedPropertySuggestion,
    EnhancedModelSpec,
    EnumerationCandidate,
    InheritanceOpportunity,
)
from legend_cli.database.models import Database, Table
from legend_cli.pure.generator import PureCodeGenerator


class EnhancedPureCodeGenerator(PureCodeGenerator):
    """Extended generator that produces Pure code with advanced features.

    Generates:
    - Enumerations from detected candidates
    - Classes with inheritance (extends) where detected
    - Constraints on classes
    - Derived (computed) properties
    """

    def __init__(
        self,
        database: Database,
        enhanced_spec: Optional[EnhancedModelSpec] = None,
        package_prefix: str = "model",
    ):
        """Initialize the enhanced generator.

        Args:
            database: Database schema
            enhanced_spec: Enhanced model specification with analysis results
            package_prefix: Package prefix for generated code
        """
        super().__init__(database, package_prefix)
        self.spec = enhanced_spec

        # Build lookup maps from spec
        self._base_class_map: Dict[str, str] = {}  # derived -> base
        self._enum_map: Dict[str, EnumerationCandidate] = {}  # table.col -> enum
        self._constraints_map: Dict[str, List[ConstraintSuggestion]] = {}  # class -> constraints
        self._derived_map: Dict[str, List[DerivedPropertySuggestion]] = {}  # class -> derived

        if enhanced_spec:
            self._build_lookup_maps()

    def _build_lookup_maps(self) -> None:
        """Build lookup maps from enhanced spec for efficient access."""
        if not self.spec:
            return

        # Build inheritance map
        for hierarchy in self.spec.hierarchies:
            for derived in hierarchy.derived_classes:
                self._base_class_map[derived] = hierarchy.base_class_name

        # Build enum map - map source columns directly
        enum_source_tables: Dict[str, Any] = {}  # table_name -> enum
        for enum in self.spec.enumerations:
            key = f"{enum.source_table}.{enum.source_column}"
            self._enum_map[key] = enum
            # Track which tables are enum source tables
            enum_source_tables[enum.source_table] = enum

        # Also map FK columns that reference enum source tables
        # Only map if the target table IS the enum source table (e.g., CLIENT_TYPE)
        # and the relationship is a lookup relationship (source column name contains target table name)
        if self.database.relationships:
            for rel in self.database.relationships:
                # If the target table is an enum source table, check if this is a type/status lookup
                if rel.target_table in enum_source_tables:
                    # Only map if the FK column name suggests it references the enum type
                    # e.g., CLIENT_TYPE_ID should map to ClientType, but CLIENT_ID should not
                    target_table_upper = rel.target_table.upper()
                    source_col_upper = rel.source_column.upper()

                    # Check if the FK column name contains the target table name pattern
                    # CLIENT_TYPE_ID contains CLIENT_TYPE -> yes
                    # CLIENT_ID does not contain CLIENT_TYPE -> no
                    is_type_lookup = (
                        target_table_upper.replace("_", "") in source_col_upper.replace("_", "") or
                        source_col_upper.startswith(target_table_upper.split("_")[0] + "_TYPE") or
                        source_col_upper.endswith("_TYPE_ID") or
                        source_col_upper.endswith("_STATUS_ID")
                    )

                    if is_type_lookup:
                        enum = enum_source_tables[rel.target_table]
                        fk_key = f"{rel.source_table}.{rel.source_column}"
                        if fk_key not in self._enum_map:
                            self._enum_map[fk_key] = enum

        # Build constraints map
        for constraint in self.spec.constraints:
            if constraint.class_name not in self._constraints_map:
                self._constraints_map[constraint.class_name] = []
            self._constraints_map[constraint.class_name].append(constraint)

        # Build derived properties map
        for derived in self.spec.derived_properties:
            if derived.class_name not in self._derived_map:
                self._derived_map[derived.class_name] = []
            self._derived_map[derived.class_name].append(derived)

    def generate_enumerations(self) -> str:
        """Generate Pure enumeration definitions.

        Returns:
            Pure code for all enumerations
        """
        if not self.spec or not self.spec.enumerations:
            return ""

        enum_defs = ["###Pure"]

        for enum in self.spec.enumerations:
            if not enum.values:
                continue

            lines = [f"Enum {self.package_prefix}::domain::{enum.name}"]
            lines.append("{")

            # Add enum values
            value_lines = []
            for value in enum.values:
                # Clean value for Pure syntax
                clean_value = self._sanitize_enum_value(value)
                value_lines.append(f"  {clean_value}")

            lines.append(",\n".join(value_lines))
            lines.append("}")

            enum_defs.append("\n".join(lines))

        return "\n\n".join(enum_defs)

    def generate_classes_enhanced(self, docs: Optional[Dict[str, Any]] = None) -> str:
        """Generate Pure classes with inheritance, constraints, and derived properties.

        Args:
            docs: Optional documentation dictionary

        Returns:
            Pure code for all classes with enhanced features
        """
        class_defs = ["###Pure"]
        generated_base_classes: Set[str] = set()

        # First, generate base classes for hierarchies (if not actual tables)
        for hierarchy in (self.spec.hierarchies if self.spec else []):
            if not self._is_table_class(hierarchy.base_class_name):
                base_class_code = self._generate_base_class(hierarchy, docs)
                if base_class_code:
                    class_defs.append(base_class_code)
                    generated_base_classes.add(hierarchy.base_class_name)

        # Generate classes for each table
        for schema in self.database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()

                # Skip if already generated as base class
                if class_name in generated_base_classes:
                    continue

                class_code = self._generate_enhanced_class(
                    table, schema.name, class_name, docs
                )
                class_defs.append(class_code)

        return "\n\n".join(class_defs)

    def _generate_enhanced_class(
        self,
        table: Table,
        schema_name: str,
        class_name: str,
        docs: Optional[Dict[str, Any]],
    ) -> str:
        """Generate a single enhanced class definition.

        Args:
            table: Table to generate class for
            schema_name: Schema name
            class_name: Target class name
            docs: Optional documentation

        Returns:
            Pure class definition string
        """
        lines = []

        # Get documentation
        class_doc = ""
        attr_docs = {}
        if docs and class_name in docs:
            class_doc_obj = docs[class_name]
            class_doc = getattr(class_doc_obj, 'class_doc', '') or ''
            attr_docs = {
                k: getattr(v, 'doc', '')
                for k, v in getattr(class_doc_obj, 'attributes', {}).items()
            }

        # Check for inheritance
        base_class = self._base_class_map.get(class_name)

        # Class declaration
        class_decl = self._build_class_declaration(
            class_name, class_doc, base_class
        )

        # Add constraints BEFORE the class body (Legend Pure syntax)
        constraints = self._constraints_map.get(class_name, [])
        if constraints:
            lines.append(class_decl)
            lines.append("[")
            constraint_lines = []
            for constraint in constraints:
                # Sanitize the constraint expression
                sanitized_expr = self._sanitize_pure_expression(constraint.expression)
                constraint_lines.append(
                    f"  {constraint.constraint_name}: {sanitized_expr}"
                )
            lines.append(",\n".join(constraint_lines))
            lines.append("]")
            lines.append("{")
        else:
            lines.append(class_decl)
            lines.append("{")

        # Get base class properties to exclude (if inheriting)
        base_properties = self._get_base_class_properties(base_class)

        # Regular properties
        for col in table.columns:
            prop_name = table.get_property_name(col.name)

            # Skip if inherited from base class
            if prop_name in base_properties:
                continue

            # Check if column maps to an enum
            enum_type = self._get_enum_type_for_column(table.name, col.name)

            if enum_type:
                prop_type = f"{self.package_prefix}::domain::{enum_type}"
            else:
                prop_type = col.to_pure_property_type()

            multiplicity = "[0..1]" if col.is_nullable else "[1]"

            # Property with optional doc
            prop_doc = attr_docs.get(prop_name, '')
            if prop_doc:
                escaped_doc = self._escape_doc_string(prop_doc)
                lines.append(
                    f"  {{meta::pure::profiles::doc.doc = '{escaped_doc}'}} "
                    f"{prop_name}: {prop_type}{multiplicity};"
                )
            else:
                lines.append(f"  {prop_name}: {prop_type}{multiplicity};")

        # Add derived properties
        derived_props = self._derived_map.get(class_name, [])
        for derived in derived_props:
            lines.append("")
            if derived.description:
                lines.append(f"  // {derived.description}")
            # Sanitize the expression to fix common LLM-generated syntax issues
            sanitized_expr = self._sanitize_pure_expression(derived.expression)
            lines.append(
                f"  {derived.property_name}: {derived.return_type}{derived.multiplicity} "
                f"= {sanitized_expr};"
            )

        lines.append("}")

        return "\n".join(lines)

    def _generate_base_class(
        self,
        hierarchy: InheritanceOpportunity,
        docs: Optional[Dict[str, Any]],
    ) -> str:
        """Generate an abstract base class for a hierarchy.

        Args:
            hierarchy: Hierarchy definition
            docs: Optional documentation

        Returns:
            Pure class definition for base class
        """
        lines = []
        class_name = hierarchy.base_class_name

        # Class declaration (no extends for base)
        class_doc = ""
        if docs and class_name in docs:
            class_doc_obj = docs[class_name]
            class_doc = getattr(class_doc_obj, 'class_doc', '') or ''

        if class_doc:
            escaped_doc = self._escape_doc_string(class_doc)
            lines.append(
                f"Class {{meta::pure::profiles::doc.doc = '{escaped_doc}'}} "
                f"{self.package_prefix}::domain::{class_name}"
            )
        else:
            lines.append(f"Class {self.package_prefix}::domain::{class_name}")

        lines.append("{")

        # Add base class properties
        for prop_name in hierarchy.base_class_properties:
            # Convert column name to property name format
            prop_name_camel = self._to_camel_case(prop_name)
            # Default to String[0..1] for abstract properties
            lines.append(f"  {prop_name_camel}: String[0..1];")

        lines.append("}")

        return "\n".join(lines)

    def _build_class_declaration(
        self,
        class_name: str,
        class_doc: str,
        base_class: Optional[str],
    ) -> str:
        """Build the class declaration line with optional doc and extends."""
        parts = ["Class"]

        # Doc annotation comes after Class keyword
        if class_doc:
            escaped_doc = self._escape_doc_string(class_doc)
            parts.append(f"{{meta::pure::profiles::doc.doc = '{escaped_doc}'}}")

        # Full class path
        class_path = f"{self.package_prefix}::domain::{class_name}"
        parts.append(class_path)

        # Extends clause
        if base_class:
            base_path = f"{self.package_prefix}::domain::{base_class}"
            parts.append(f"extends {base_path}")

        return " ".join(parts)

    def _get_base_class_properties(self, base_class: Optional[str]) -> Set[str]:
        """Get property names from base class to exclude from derived class.

        Args:
            base_class: Base class name or None

        Returns:
            Set of property names in base class
        """
        if not base_class or not self.spec:
            return set()

        for hierarchy in self.spec.hierarchies:
            if hierarchy.base_class_name == base_class:
                return {
                    self._to_camel_case(p)
                    for p in hierarchy.base_class_properties
                }

        return set()

    def _get_enum_type_for_column(
        self,
        table_name: str,
        column_name: str,
    ) -> Optional[str]:
        """Get the enum type for a column if one is defined.

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            Enum type name or None
        """
        key = f"{table_name}.{column_name}"
        enum = self._enum_map.get(key)
        return enum.name if enum else None

    def _is_table_class(self, class_name: str) -> bool:
        """Check if a class name corresponds to an actual table."""
        return class_name in self.table_to_class.values()

    def _sanitize_enum_value(self, value: str) -> str:
        """Sanitize a value for use as Pure enum value.

        Pure enum values must be valid identifiers (alphanumeric + underscore).
        """
        # Already should be in UPPER_SNAKE_CASE from enum_detector
        result = value.upper()

        # Replace invalid characters
        result = "".join(c if c.isalnum() or c == "_" else "_" for c in result)

        # Collapse multiple underscores
        while "__" in result:
            result = result.replace("__", "_")

        # Strip leading/trailing underscores
        result = result.strip("_")

        # Prefix if starts with digit
        if result and result[0].isdigit():
            result = f"VALUE_{result}"

        return result or "UNKNOWN"

    def _to_camel_case(self, name: str) -> str:
        """Convert UPPER_SNAKE_CASE to camelCase."""
        parts = name.lower().split("_")
        return parts[0] + "".join(p.capitalize() for p in parts[1:])

    def _sanitize_pure_expression(self, expression: str) -> str:
        """Sanitize a Pure expression to fix common LLM-generated syntax issues.

        Args:
            expression: Raw expression string

        Returns:
            Sanitized expression with valid Pure syntax
        """
        import re

        result = expression

        # Fix %now() -> now() (% is for date literals, not function calls)
        result = re.sub(r'%now\(\)', 'now()', result)
        result = re.sub(r'%today\(\)', 'today()', result)

        # Fix if() syntax - add lambda markers (|) to then/else branches
        # Pattern: if(condition, thenExpr, elseExpr)
        # Should be: if(condition, |thenExpr, |elseExpr)
        def fix_if_syntax(match):
            full_match = match.group(0)
            # Simple approach: add | before 2nd and 3rd arguments if not present
            # This is a simplified fix that handles common cases
            parts = full_match[3:-1]  # Remove 'if(' and ')'

            # Find the comma positions (accounting for nested parens)
            depth = 0
            comma_positions = []
            for i, c in enumerate(parts):
                if c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                elif c == ',' and depth == 0:
                    comma_positions.append(i)

            if len(comma_positions) >= 2:
                condition = parts[:comma_positions[0]].strip()
                then_expr = parts[comma_positions[0]+1:comma_positions[1]].strip()
                else_expr = parts[comma_positions[1]+1:].strip()

                # Add | prefix if not present
                if not then_expr.startswith('|'):
                    then_expr = '|' + then_expr
                if not else_expr.startswith('|'):
                    else_expr = '|' + else_expr

                return f"if({condition}, {then_expr}, {else_expr})"
            return full_match

        # Fix if() calls - find balanced parentheses
        result = re.sub(r'if\([^)]+(?:\([^)]*\)[^)]*)*\)', fix_if_syntax, result)

        # Remove any trailing semicolons (should not be in the expression itself)
        result = result.rstrip(';').strip()

        return result

    def generate_enhanced_mapping(self) -> str:
        """Generate mapping with enum type mappings.

        Returns:
            Pure mapping code
        """
        # For now, use base mapping
        # Enum mappings typically handled in store definition
        return self.generate_mapping()

    def generate_all_enhanced(
        self,
        connection_code: str,
        docs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """Generate all Pure code artifacts with enhanced features.

        Args:
            connection_code: Pre-generated connection code
            docs: Optional documentation dictionary

        Returns:
            Dictionary with all artifact types including enumerations
        """
        artifacts = {}

        # Enumerations first (other artifacts may reference them)
        enums = self.generate_enumerations()
        if enums:
            artifacts["enumerations"] = enums

        # Store with joins
        artifacts["store"] = self.generate_store_with_joins()

        # Enhanced classes with inheritance, constraints, derived properties
        artifacts["classes"] = self.generate_classes_enhanced(docs=docs)

        # Associations
        associations = self.generate_associations()
        if associations:
            artifacts["associations"] = associations

        # Connection
        artifacts["connection"] = connection_code

        # Mapping
        artifacts["mapping"] = self.generate_enhanced_mapping()

        # Runtime
        artifacts["runtime"] = self.generate_runtime()

        return artifacts

    def get_enhanced_summary(self) -> Dict[str, Any]:
        """Get a summary of enhanced features used in generation.

        Returns:
            Summary dictionary with counts and details
        """
        summary = {
            "hierarchies": [],
            "enumerations": [],
            "constraints": [],
            "derived_properties": [],
        }

        if not self.spec:
            return summary

        # Hierarchies
        for h in self.spec.hierarchies:
            summary["hierarchies"].append({
                "base_class": h.base_class_name,
                "derived_classes": h.derived_classes,
                "confidence": h.confidence,
            })

        # Enumerations
        for e in self.spec.enumerations:
            summary["enumerations"].append({
                "name": e.name,
                "source": f"{e.source_table}.{e.source_column}",
                "value_count": len(e.values),
                "confidence": e.confidence,
            })

        # Constraints
        for c in self.spec.constraints:
            summary["constraints"].append({
                "class": c.class_name,
                "name": c.constraint_name,
                "confidence": c.confidence,
            })

        # Derived properties
        for d in self.spec.derived_properties:
            summary["derived_properties"].append({
                "class": d.class_name,
                "property": d.property_name,
                "type": d.return_type,
                "confidence": d.confidence,
            })

        return summary
