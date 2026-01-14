"""PDF document parser for extracting documentation content."""

from pathlib import Path
from typing import Optional

from .base import DocumentParser, DocumentationSource


class PdfParser(DocumentParser):
    """Parser for extracting content from PDF files."""

    def can_parse(self, source: str) -> bool:
        """Check if this parser can handle the given source."""
        path = Path(source)
        return path.suffix.lower() == ".pdf" and path.exists()

    async def parse(self, source: str) -> DocumentationSource:
        """Parse a PDF file and extract its content.

        Args:
            source: Path to PDF file

        Returns:
            DocumentationSource with extracted content
        """
        try:
            # Try pypdf first (preferred)
            content, metadata = self._parse_with_pypdf(source)
        except ImportError:
            try:
                # Fallback to pdfplumber
                content, metadata = self._parse_with_pdfplumber(source)
            except ImportError:
                raise ImportError(
                    "No PDF parser available. Install 'pypdf' or 'pdfplumber': "
                    "pip install pypdf"
                )

        return DocumentationSource(
            source_type="pdf",
            source_path=source,
            content=content,
            metadata=metadata,
        )

    def _parse_with_pypdf(self, source: str) -> tuple[str, dict]:
        """Parse PDF using pypdf library."""
        from pypdf import PdfReader

        reader = PdfReader(source)

        # Extract metadata
        pdf_metadata = reader.metadata or {}
        metadata = {
            "title": pdf_metadata.get("/Title", ""),
            "author": pdf_metadata.get("/Author", ""),
            "subject": pdf_metadata.get("/Subject", ""),
            "num_pages": len(reader.pages),
            "file_path": source,
        }

        # Extract text from all pages
        pages_text = []
        for i, page in enumerate(reader.pages):
            text = page.extract_text()
            if text:
                pages_text.append(f"--- Page {i + 1} ---\n{text}")

        content = "\n\n".join(pages_text)

        return content, metadata

    def _parse_with_pdfplumber(self, source: str) -> tuple[str, dict]:
        """Parse PDF using pdfplumber library (fallback)."""
        import pdfplumber

        with pdfplumber.open(source) as pdf:
            metadata = {
                "title": pdf.metadata.get("Title", ""),
                "author": pdf.metadata.get("Author", ""),
                "num_pages": len(pdf.pages),
                "file_path": source,
            }

            pages_text = []
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    pages_text.append(f"--- Page {i + 1} ---\n{text}")

                # Also extract tables if present
                tables = page.extract_tables()
                for table in tables:
                    if table:
                        table_text = self._format_table(table)
                        pages_text.append(table_text)

            content = "\n\n".join(pages_text)

        return content, metadata

    def _format_table(self, table: list) -> str:
        """Format a table as text."""
        lines = []
        for row in table:
            if row:
                cells = [str(cell) if cell else "" for cell in row]
                lines.append(" | ".join(cells))
        return "\n".join(lines)
