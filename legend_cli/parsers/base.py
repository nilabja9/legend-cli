"""Base classes for document parsing."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Literal, Optional, Tuple
from pathlib import Path


@dataclass
class ExtractedImage:
    """Represents an image extracted from a document."""

    page_number: int
    image_data: bytes
    image_format: str  # e.g., "png", "jpeg", "jpg"
    width: Optional[int] = None
    height: Optional[int] = None

    @property
    def media_type(self) -> str:
        """Get the MIME type for this image format."""
        format_map = {
            "png": "image/png",
            "jpeg": "image/jpeg",
            "jpg": "image/jpeg",
            "gif": "image/gif",
            "webp": "image/webp",
            "bmp": "image/bmp",
        }
        return format_map.get(self.image_format.lower(), "image/png")


@dataclass
class DocumentationSource:
    """Represents a parsed documentation source."""

    source_type: Literal["url", "pdf", "json"]
    source_path: str  # Original URL or file path
    content: str  # Extracted text content
    metadata: dict = field(default_factory=dict)  # Additional metadata (title, sections, etc.)
    images: List[ExtractedImage] = field(default_factory=list)  # Extracted images from document

    def __post_init__(self):
        if not self.content:
            self.content = ""

    def has_images(self) -> bool:
        """Check if this source contains any images."""
        return len(self.images) > 0

    def get_images_from_page(self, page_number: int) -> List[ExtractedImage]:
        """Get all images from a specific page."""
        return [img for img in self.images if img.page_number == page_number]


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
