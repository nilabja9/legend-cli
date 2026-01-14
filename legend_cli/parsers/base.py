"""Base classes for document parsing."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal, Optional
from pathlib import Path


@dataclass
class DocumentationSource:
    """Represents a parsed documentation source."""

    source_type: Literal["url", "pdf", "json"]
    source_path: str  # Original URL or file path
    content: str  # Extracted text content
    metadata: dict = field(default_factory=dict)  # Additional metadata (title, sections, etc.)

    def __post_init__(self):
        if not self.content:
            self.content = ""


class DocumentParser(ABC):
    """Abstract base class for document parsers."""

    @abstractmethod
    async def parse(self, source: str) -> DocumentationSource:
        """Parse the source and return a DocumentationSource.

        Args:
            source: URL or file path to parse

        Returns:
            DocumentationSource with extracted content
        """
        pass

    @abstractmethod
    def can_parse(self, source: str) -> bool:
        """Check if this parser can handle the given source.

        Args:
            source: URL or file path

        Returns:
            True if this parser can handle the source
        """
        pass

    @staticmethod
    def detect_source_type(source: str) -> Optional[Literal["url", "pdf", "json"]]:
        """Detect the type of documentation source.

        Args:
            source: URL or file path

        Returns:
            Source type or None if unknown
        """
        source_lower = source.lower()

        # Check if it's a URL
        if source_lower.startswith(("http://", "https://")):
            return "url"

        # Check file extension
        path = Path(source)
        suffix = path.suffix.lower()

        if suffix == ".pdf":
            return "pdf"
        elif suffix == ".json":
            return "json"

        return None
