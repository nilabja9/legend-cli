"""SQL query parser for extracting queries from files and text."""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Literal, Optional

from legend_cli.parsers.base import DocumentParser, DocumentationSource


@dataclass
class JoinRelationship:
    """Represents a JOIN relationship extracted from SQL.

    This captures the relationship between two tables as expressed
    in a SQL JOIN clause, which can be used to infer foreign key
    relationships for database introspection.
    """

    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str  # INNER, LEFT, RIGHT, FULL
    source_query: str = ""

    def to_relationship_tuple(self) -> tuple:
        """Return a tuple for deduplication purposes."""
        # Normalize to always have the same order
        tables = sorted([
            (self.left_table, self.left_column),
            (self.right_table, self.right_column)
        ])
        return (tables[0][0], tables[0][1], tables[1][0], tables[1][1])


@dataclass
class SqlQuery:
    """Represents a parsed SQL query."""

    query: str
    query_type: Literal["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "OTHER"]
    tables: List[str]  # Tables referenced in the query
    source_file: Optional[str] = None
    line_number: Optional[int] = None

    @classmethod
    def from_text(cls, text: str, source_file: Optional[str] = None, line_number: Optional[int] = None) -> "SqlQuery":
        """Parse a SQL query from text."""
        query = text.strip()
        query_type = cls._detect_query_type(query)
        tables = cls._extract_tables(query)

        return cls(
            query=query,
            query_type=query_type,
            tables=tables,
            source_file=source_file,
            line_number=line_number,
        )

    @staticmethod
    def _detect_query_type(query: str) -> Literal["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "OTHER"]:
        """Detect the type of SQL query."""
        query_upper = query.upper().strip()

        if query_upper.startswith("SELECT"):
            return "SELECT"
        elif query_upper.startswith("INSERT"):
            return "INSERT"
        elif query_upper.startswith("UPDATE"):
            return "UPDATE"
        elif query_upper.startswith("DELETE"):
            return "DELETE"
        elif query_upper.startswith("CREATE"):
            return "CREATE"
        elif query_upper.startswith("ALTER"):
            return "ALTER"
        else:
            return "OTHER"

    @staticmethod
    def _extract_tables(query: str) -> List[str]:
        """Extract table names from a SQL query."""
        tables = []

        # Pattern for FROM clause
        from_pattern = r"FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)"
        for match in re.finditer(from_pattern, query, re.IGNORECASE):
            table = match.group(1).split(".")[-1]  # Remove schema prefix
            if table.upper() not in ("SELECT", "WHERE", "AND", "OR"):
                tables.append(table)

        # Pattern for JOIN clause
        join_pattern = r"JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)"
        for match in re.finditer(join_pattern, query, re.IGNORECASE):
            table = match.group(1).split(".")[-1]
            if table not in tables:
                tables.append(table)

        # Pattern for INSERT INTO
        insert_pattern = r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)"
        for match in re.finditer(insert_pattern, query, re.IGNORECASE):
            table = match.group(1).split(".")[-1]
            if table not in tables:
                tables.append(table)

        # Pattern for UPDATE
        update_pattern = r"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)"
        for match in re.finditer(update_pattern, query, re.IGNORECASE):
            table = match.group(1).split(".")[-1]
            if table not in tables:
                tables.append(table)

        return tables

    def extract_joins(self) -> List[JoinRelationship]:
        """Extract JOIN relationships from this query.

        Returns:
            List of JoinRelationship objects representing table joins
        """
        return SqlJoinExtractor.extract_joins(self.query)


@dataclass
class SqlSource:
    """Represents a collection of SQL queries from a source."""

    source_path: str
    source_type: Literal["file", "directory", "text"]
    queries: List[SqlQuery] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def get_select_queries(self) -> List[SqlQuery]:
        """Get only SELECT queries."""
        return [q for q in self.queries if q.query_type == "SELECT"]

    def get_queries_for_table(self, table_name: str) -> List[SqlQuery]:
        """Get queries that reference a specific table."""
        table_upper = table_name.upper()
        return [q for q in self.queries if any(t.upper() == table_upper for t in q.tables)]


class SqlParser(DocumentParser):
    """Parser for SQL files and directories.

    Supports:
    - .sql files
    - Markdown files with SQL code blocks
    - Directories containing SQL files
    """

    # File extensions to parse
    SQL_EXTENSIONS = {".sql"}
    MARKDOWN_EXTENSIONS = {".md", ".markdown"}

    async def parse(self, source: str) -> DocumentationSource:
        """Parse SQL source and return as DocumentationSource.

        Args:
            source: File path or directory path

        Returns:
            DocumentationSource with SQL content
        """
        sql_source = self.parse_source(source)

        # Convert to DocumentationSource format
        content_lines = []
        for query in sql_source.queries:
            content_lines.append(f"-- Query ({query.query_type})")
            content_lines.append(query.query)
            content_lines.append("")

        return DocumentationSource(
            source_type="json",  # Using json as generic file type
            source_path=source,
            content="\n".join(content_lines),
            metadata={
                "query_count": len(sql_source.queries),
                "source_type": sql_source.source_type,
            },
        )

    def can_parse(self, source: str) -> bool:
        """Check if this parser can handle the source.

        Args:
            source: File or directory path

        Returns:
            True if this is a SQL file, markdown file, or directory
        """
        path = Path(source)

        if path.is_dir():
            return True

        suffix = path.suffix.lower()
        return suffix in self.SQL_EXTENSIONS or suffix in self.MARKDOWN_EXTENSIONS

    def parse_source(self, source: str) -> SqlSource:
        """Parse SQL from a file or directory.

        Args:
            source: File or directory path

        Returns:
            SqlSource with parsed queries
        """
        path = Path(source)

        if path.is_dir():
            return self._parse_directory(path)
        elif path.is_file():
            return self._parse_file(path)
        else:
            raise ValueError(f"Source does not exist: {source}")

    def parse_text(self, text: str, source_name: str = "<text>") -> SqlSource:
        """Parse SQL queries from raw text.

        Args:
            text: SQL text content
            source_name: Name for the source

        Returns:
            SqlSource with parsed queries
        """
        queries = self._extract_queries_from_sql(text, source_name)

        return SqlSource(
            source_path=source_name,
            source_type="text",
            queries=queries,
        )

    def _parse_directory(self, directory: Path) -> SqlSource:
        """Parse all SQL files in a directory."""
        queries = []

        for sql_file in directory.rglob("*.sql"):
            file_queries = self._extract_queries_from_file(sql_file)
            queries.extend(file_queries)

        for md_file in directory.rglob("*.md"):
            md_queries = self._extract_queries_from_markdown(md_file)
            queries.extend(md_queries)

        return SqlSource(
            source_path=str(directory),
            source_type="directory",
            queries=queries,
            metadata={"file_count": len(list(directory.rglob("*.sql")))},
        )

    def _parse_file(self, file_path: Path) -> SqlSource:
        """Parse a single SQL or markdown file."""
        suffix = file_path.suffix.lower()

        if suffix in self.SQL_EXTENSIONS:
            queries = self._extract_queries_from_file(file_path)
        elif suffix in self.MARKDOWN_EXTENSIONS:
            queries = self._extract_queries_from_markdown(file_path)
        else:
            queries = []

        return SqlSource(
            source_path=str(file_path),
            source_type="file",
            queries=queries,
        )

    def _extract_queries_from_file(self, file_path: Path) -> List[SqlQuery]:
        """Extract SQL queries from a .sql file."""
        try:
            content = file_path.read_text(encoding="utf-8")
        except (IOError, UnicodeDecodeError) as e:
            print(f"Warning: Could not read file {file_path}: {e}")
            return []

        return self._extract_queries_from_sql(content, str(file_path))

    def _extract_queries_from_sql(self, content: str, source_file: str) -> List[SqlQuery]:
        """Extract individual queries from SQL content."""
        queries = []

        # Remove comments
        content = self._remove_sql_comments(content)

        # Split by semicolons (simple approach)
        raw_queries = content.split(";")

        line_number = 1
        for raw in raw_queries:
            query_text = raw.strip()
            if query_text and len(query_text) > 5:  # Skip empty or trivial
                query = SqlQuery.from_text(query_text, source_file, line_number)
                queries.append(query)

            # Track line numbers approximately
            line_number += raw.count("\n")

        return queries

    def _extract_queries_from_markdown(self, file_path: Path) -> List[SqlQuery]:
        """Extract SQL queries from markdown code blocks."""
        try:
            content = file_path.read_text(encoding="utf-8")
        except (IOError, UnicodeDecodeError) as e:
            print(f"Warning: Could not read file {file_path}: {e}")
            return []

        queries = []

        # Pattern for SQL code blocks
        sql_block_pattern = r"```(?:sql|SQL)\s*\n(.*?)```"

        for match in re.finditer(sql_block_pattern, content, re.DOTALL):
            block_content = match.group(1)
            block_queries = self._extract_queries_from_sql(block_content, str(file_path))
            queries.extend(block_queries)

        return queries

    def _remove_sql_comments(self, content: str) -> str:
        """Remove SQL comments from content."""
        # Remove single-line comments (-- ...)
        content = re.sub(r"--[^\n]*", "", content)

        # Remove multi-line comments (/* ... */)
        content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)

        return content


def parse_sql_files(paths: List[str]) -> List[str]:
    """Parse SQL files and return list of query strings.

    Convenience function for simple use cases.

    Args:
        paths: List of file or directory paths

    Returns:
        List of SQL query strings
    """
    parser = SqlParser()
    all_queries = []

    for path in paths:
        try:
            source = parser.parse_source(path)
            all_queries.extend(q.query for q in source.queries)
        except Exception as e:
            print(f"Warning: Failed to parse {path}: {e}")

    return all_queries


def extract_select_queries(paths: List[str]) -> List[str]:
    """Parse SQL files and return only SELECT queries.

    Args:
        paths: List of file or directory paths

    Returns:
        List of SELECT query strings
    """
    parser = SqlParser()
    select_queries = []

    for path in paths:
        try:
            source = parser.parse_source(path)
            select_queries.extend(q.query for q in source.get_select_queries())
        except Exception as e:
            print(f"Warning: Failed to parse {path}: {e}")

    return select_queries


class SqlJoinExtractor:
    """Extracts JOIN relationships from SQL queries.

    This class parses SQL queries to extract JOIN conditions,
    which represent relationships between tables. These relationships
    can be used to infer foreign key constraints for databases
    that don't have explicit FK metadata.

    Example:
        SELECT * FROM orders o
        JOIN customers c ON o.customer_id = c.id

        Extracts: orders.customer_id -> customers.id (INNER JOIN)
    """

    # Regex pattern for extracting JOIN clauses with ON conditions
    # Supports: INNER JOIN, LEFT [OUTER] JOIN, RIGHT [OUTER] JOIN, FULL [OUTER] JOIN
    JOIN_PATTERN = re.compile(
        r"""
        (?P<join_type>INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?
        JOIN\s+
        (?P<table>[\w.]+)
        (?:\s+(?:AS\s+)?(?P<alias>\w+))?
        \s+ON\s+
        (?P<condition>[^)]+?)
        (?=\s+(?:INNER|LEFT|RIGHT|FULL|CROSS|JOIN|WHERE|GROUP|ORDER|HAVING|LIMIT|UNION|;|$))
        """,
        re.IGNORECASE | re.VERBOSE | re.DOTALL,
    )

    # Pattern for parsing equality conditions in ON clause
    # Supports: t1.col1 = t2.col2 or col1 = col2
    CONDITION_PATTERN = re.compile(
        r"""
        (?P<left>[\w.]+)
        \s*=\s*
        (?P<right>[\w.]+)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    @classmethod
    def extract_joins(cls, sql: str) -> List[JoinRelationship]:
        """Extract JOIN relationships from a single SQL query.

        Args:
            sql: SQL query string

        Returns:
            List of JoinRelationship objects
        """
        relationships = []

        # Build alias map from the query
        alias_map = cls._build_alias_map(sql)

        # Find all JOIN clauses
        for match in cls.JOIN_PATTERN.finditer(sql):
            join_type = (match.group("join_type") or "INNER").strip().upper()
            join_type = join_type.replace("OUTER", "").strip()
            if not join_type:
                join_type = "INNER"

            joined_table = match.group("table").split(".")[-1]  # Remove schema
            alias = match.group("alias")
            condition = match.group("condition")

            # Register alias if present
            if alias:
                alias_map[alias.upper()] = joined_table

            # Parse the ON condition for equality joins
            for cond_match in cls.CONDITION_PATTERN.finditer(condition):
                left = cond_match.group("left")
                right = cond_match.group("right")

                # Resolve table.column references
                left_table, left_col = cls._resolve_column_ref(left, alias_map)
                right_table, right_col = cls._resolve_column_ref(right, alias_map)

                if left_table and right_table and left_col and right_col:
                    relationships.append(
                        JoinRelationship(
                            left_table=left_table,
                            left_column=left_col,
                            right_table=right_table,
                            right_column=right_col,
                            join_type=join_type,
                            source_query=sql[:200] + "..." if len(sql) > 200 else sql,
                        )
                    )

        return relationships

    @classmethod
    def extract_from_document(cls, content: str) -> List[JoinRelationship]:
        """Extract JOIN relationships from document content.

        This method looks for SQL code blocks or inline SQL queries
        in documentation text and extracts JOIN relationships from them.

        Args:
            content: Document content (may contain SQL queries)

        Returns:
            List of JoinRelationship objects (deduplicated)
        """
        relationships = []
        seen = set()

        # Find SQL code blocks (```sql ... ```)
        sql_block_pattern = r"```(?:sql|SQL)?\s*\n(.*?)```"
        for match in re.finditer(sql_block_pattern, content, re.DOTALL):
            block = match.group(1)
            for rel in cls.extract_joins(block):
                key = rel.to_relationship_tuple()
                if key not in seen:
                    seen.add(key)
                    relationships.append(rel)

        # Also look for inline SELECT statements
        select_pattern = r"(SELECT\s+.+?(?:;|$))"
        for match in re.finditer(select_pattern, content, re.IGNORECASE | re.DOTALL):
            query = match.group(1)
            # Only process if it contains JOIN
            if re.search(r"\bJOIN\b", query, re.IGNORECASE):
                for rel in cls.extract_joins(query):
                    key = rel.to_relationship_tuple()
                    if key not in seen:
                        seen.add(key)
                        relationships.append(rel)

        return relationships

    @classmethod
    def _build_alias_map(cls, sql: str) -> dict:
        """Build a mapping of table aliases to table names.

        Args:
            sql: SQL query string

        Returns:
            Dictionary mapping alias (uppercase) to table name
        """
        alias_map = {}

        # Pattern for FROM clause with alias: FROM table [AS] alias
        from_pattern = r"FROM\s+([\w.]+)(?:\s+(?:AS\s+)?(\w+))?"
        for match in re.finditer(from_pattern, sql, re.IGNORECASE):
            table = match.group(1).split(".")[-1]
            alias = match.group(2)
            if alias:
                alias_map[alias.upper()] = table
            # Also map table name to itself
            alias_map[table.upper()] = table

        # Pattern for JOIN clause with alias
        join_alias_pattern = r"JOIN\s+([\w.]+)(?:\s+(?:AS\s+)?(\w+))?"
        for match in re.finditer(join_alias_pattern, sql, re.IGNORECASE):
            table = match.group(1).split(".")[-1]
            alias = match.group(2)
            if alias:
                alias_map[alias.upper()] = table
            alias_map[table.upper()] = table

        return alias_map

    @classmethod
    def _resolve_column_ref(
        cls, ref: str, alias_map: dict
    ) -> tuple[Optional[str], Optional[str]]:
        """Resolve a column reference to (table_name, column_name).

        Args:
            ref: Column reference (e.g., "t.col" or "col")
            alias_map: Mapping of aliases to table names

        Returns:
            Tuple of (table_name, column_name), or (None, None) if unresolvable
        """
        parts = ref.split(".")
        if len(parts) == 2:
            # Has table qualifier
            table_or_alias = parts[0].upper()
            column = parts[1]
            table = alias_map.get(table_or_alias, parts[0])
            return (table, column)
        elif len(parts) == 1:
            # No qualifier - can't determine table
            return (None, parts[0])
        elif len(parts) == 3:
            # Schema.table.column
            table = parts[1]
            column = parts[2]
            return (table, column)
        else:
            return (None, None)
