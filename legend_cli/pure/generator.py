"""Pure code generator for Legend models."""

from typing import Optional, List, Dict, Any

from ..database.models import Database


class PureCodeGenerator:
    """Generates Pure code from introspected database schema."""

    def __init__(self, database: Database, package_prefix: str = "model"):
        self.database = database
        self.package_prefix = package_prefix
        # Build lookup for table -> class name
        self.table_to_class = {}
        for table in database.get_all_tables():
            self.table_to_class[table.name] = table.get_class_name()

    def generate_store(self) -> str:
        """Generate Pure store definition."""
        lines = ["###Relational"]
        lines.append(f"Database {self.package_prefix}::store::{self.database.name}")
        lines.append("(")

        for schema in self.database.schemas:
            lines.append(f"  Schema {schema.name}")
            lines.append("  (")

            for table in schema.tables:
                lines.append(f"    Table {table.name}")
                lines.append("    (")

                col_lines = []
                for col in table.columns:
                    col_lines.append(f"      {col.name} {col.to_pure_type()}")

                lines.append(",\n".join(col_lines))
                lines.append("    )")

            lines.append("  )")

        lines.append(")")
        return "\n".join(lines)

    def generate_classes(self, docs: Optional[Dict[str, Any]] = None) -> str:
        """Generate Pure class definitions (without association properties).

        Args:
            docs: Optional dictionary mapping class names to ClassDocumentation objects.
                  If provided, doc.doc tagged values will be added to classes and properties.
        """
        class_defs = []

        for schema in self.database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()

                # Get class documentation if available
                class_doc = ""
                attr_docs = {}
                if docs and class_name in docs:
                    class_doc_obj = docs[class_name]
                    class_doc = getattr(class_doc_obj, 'class_doc', '') or ''
                    attr_docs = {k: getattr(v, 'doc', '') for k, v in getattr(class_doc_obj, 'attributes', {}).items()}

                # Class declaration with optional doc.doc
                if class_doc:
                    escaped_doc = self._escape_doc_string(class_doc)
                    lines = [f"Class {{meta::pure::profiles::doc.doc = '{escaped_doc}'}} {self.package_prefix}::domain::{class_name}"]
                else:
                    lines = [f"Class {self.package_prefix}::domain::{class_name}"]

                lines.append("{")

                # Regular properties only (no association properties)
                for col in table.columns:
                    prop_name = table.get_property_name(col.name)
                    prop_type = col.to_pure_property_type()
                    multiplicity = "[0..1]" if col.is_nullable else "[1]"

                    # Property with optional doc.doc
                    prop_doc = attr_docs.get(prop_name, '')
                    if prop_doc:
                        escaped_prop_doc = self._escape_doc_string(prop_doc)
                        lines.append(f"  {{meta::pure::profiles::doc.doc = '{escaped_prop_doc}'}} {prop_name}: {prop_type}{multiplicity};")
                    else:
                        lines.append(f"  {prop_name}: {prop_type}{multiplicity};")

                lines.append("}")
                class_defs.append("\n".join(lines))

        return "\n\n".join(class_defs)

    def _escape_doc_string(self, doc: str) -> str:
        """Escape a documentation string for use in Pure code.

        Escapes single quotes and removes newlines to ensure valid Pure syntax.
        """
        if not doc:
            return ""
        # Escape single quotes
        escaped = doc.replace("'", "\\'")
        # Remove or replace newlines
        escaped = escaped.replace("\n", " ").replace("\r", "")
        # Trim excess whitespace
        escaped = " ".join(escaped.split())
        return escaped

    def generate_associations(self) -> str:
        """Generate Pure Association definitions."""
        if not self.database.relationships:
            return ""

        association_defs = []
        seen_associations = set()

        for rel in self.database.relationships:
            source_class = self.table_to_class.get(rel.source_table)
            target_class = self.table_to_class.get(rel.target_table)

            if not source_class or not target_class:
                continue

            # Create a unique key for this association to avoid duplicates
            assoc_key = (source_class, target_class, rel.property_name)
            if assoc_key in seen_associations:
                continue
            seen_associations.add(assoc_key)

            # Association name combines both classes
            assoc_name = f"{source_class}_{target_class}_{rel.property_name}"

            # Generate reverse property name (plural of source class)
            reverse_prop = rel.get_reverse_property_name(rel.source_table)

            lines = [f"Association {self.package_prefix}::domain::{assoc_name}"]
            lines.append("{")
            # Source side (many) - the table that has the FK
            lines.append(f"  {reverse_prop}: {self.package_prefix}::domain::{source_class}[*];")
            # Target side (one) - the referenced table
            lines.append(f"  {rel.property_name}: {self.package_prefix}::domain::{target_class}[0..1];")
            lines.append("}")

            association_defs.append("\n".join(lines))

        return "\n\n".join(association_defs)

    def generate_mapping(self) -> str:
        """Generate Pure mapping definition with association mappings."""
        lines = ["###Mapping"]
        lines.append(f"Mapping {self.package_prefix}::mapping::{self.database.name}Mapping")
        lines.append("(")

        mapping_blocks = []
        for schema in self.database.schemas:
            for table in schema.tables:
                class_name = table.get_class_name()
                class_path = f"{self.package_prefix}::domain::{class_name}"
                store_path = f"{self.package_prefix}::store::{self.database.name}"

                block_lines = [f"  {class_path}: Relational"]
                block_lines.append("  {")

                # Primary key
                if table.columns:
                    pk_col = table.primary_key_columns[0] if table.primary_key_columns else table.columns[0].name
                    block_lines.append("    ~primaryKey")
                    block_lines.append("    (")
                    block_lines.append(f"      [{store_path}]{schema.name}.{table.name}.{pk_col}")
                    block_lines.append("    )")

                block_lines.append(f"    ~mainTable [{store_path}]{schema.name}.{table.name}")

                # Property mappings only (no association mappings here)
                prop_mappings = []
                for col in table.columns:
                    prop_name = table.get_property_name(col.name)
                    prop_mappings.append(
                        f"    {prop_name}: [{store_path}]{schema.name}.{table.name}.{col.name}"
                    )

                block_lines.append(",\n".join(prop_mappings))
                block_lines.append("  }")

                mapping_blocks.append("\n".join(block_lines))

        lines.append("\n".join(mapping_blocks))

        # Add association mappings
        if self.database.relationships:
            lines.append("")
            seen_associations = set()
            for rel in self.database.relationships:
                source_class = self.table_to_class.get(rel.source_table)
                target_class = self.table_to_class.get(rel.target_table)

                if not source_class or not target_class:
                    continue

                # Create a unique key for this association
                assoc_key = (source_class, target_class, rel.property_name)
                if assoc_key in seen_associations:
                    continue
                seen_associations.add(assoc_key)

                assoc_name = f"{source_class}_{target_class}_{rel.property_name}"
                assoc_path = f"{self.package_prefix}::domain::{assoc_name}"
                store_path = f"{self.package_prefix}::store::{self.database.name}"
                join_name = f"{rel.source_table}_{rel.target_table}"

                lines.append(f"  {assoc_path}: Relational")
                lines.append("  {")
                lines.append(f"    AssociationMapping")
                lines.append("    (")
                lines.append(f"      {rel.property_name}: [{store_path}]@{join_name}")
                lines.append("    )")
                lines.append("  }")

        lines.append(")")
        return "\n".join(lines)

    def generate_store_with_joins(self) -> str:
        """Generate Pure store definition including join definitions."""
        lines = ["###Relational"]
        lines.append(f"Database {self.package_prefix}::store::{self.database.name}")
        lines.append("(")

        for schema in self.database.schemas:
            lines.append(f"  Schema {schema.name}")
            lines.append("  (")

            for table in schema.tables:
                lines.append(f"    Table {table.name}")
                lines.append("    (")

                col_lines = []
                for col in table.columns:
                    col_lines.append(f"      {col.name} {col.to_pure_type()}")

                lines.append(",\n".join(col_lines))
                lines.append("    )")

            lines.append("  )")

        # Add Join definitions
        if self.database.relationships:
            lines.append("")
            for rel in self.database.relationships:
                source_table = self.database.get_table_by_name(rel.source_table)
                target_table = self.database.get_table_by_name(rel.target_table)
                if source_table and target_table:
                    join_name = f"{rel.source_table}_{rel.target_table}"
                    lines.append(f"  Join {join_name}({source_table.schema}.{rel.source_table}.{rel.source_column} = {target_table.schema}.{rel.target_table}.{rel.target_column})")

        lines.append(")")
        return "\n".join(lines)

    def generate_runtime(self) -> str:
        """Generate Pure runtime definition."""
        lines = ["###Runtime"]
        lines.append(f"Runtime {self.package_prefix}::runtime::{self.database.name}Runtime")
        lines.append("{")
        lines.append(f"  mappings:")
        lines.append("  [")
        lines.append(f"    {self.package_prefix}::mapping::{self.database.name}Mapping")
        lines.append("  ];")
        lines.append("  connections:")
        lines.append("  [")
        lines.append(f"    {self.package_prefix}::store::{self.database.name}:")
        lines.append("    [")
        lines.append(f"      connection: {self.package_prefix}::connection::{self.database.name}Connection")
        lines.append("    ]")
        lines.append("  ];")
        lines.append("}")
        return "\n".join(lines)

    def generate_all(
        self,
        connection_code: str,
        docs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """Generate all Pure code artifacts.

        Args:
            connection_code: Pre-generated connection code from ConnectionGenerator
            docs: Optional dict mapping class names to ClassDocumentation for doc.doc generation

        Returns:
            Dictionary with keys: store, classes, connection, mapping, runtime, (optional) associations
        """
        artifacts = {
            "store": self.generate_store_with_joins(),
            "classes": self.generate_classes(docs=docs),
            "connection": connection_code,
            "mapping": self.generate_mapping(),
            "runtime": self.generate_runtime(),
        }

        # Add associations if there are relationships
        associations = self.generate_associations()
        if associations:
            artifacts["associations"] = associations

        return artifacts

    def get_relationship_summary(self) -> List[Dict[str, str]]:
        """Get a summary of detected relationships."""
        return [
            {
                "source": f"{rel.source_table}.{rel.source_column}",
                "target": f"{rel.target_table}.{rel.target_column}",
                "type": rel.relationship_type,
                "property": rel.property_name,
            }
            for rel in self.database.relationships
        ]
