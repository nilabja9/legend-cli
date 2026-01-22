"""PDF document parser for extracting documentation content."""

import io
from pathlib import Path
from typing import List, Optional, Tuple

from .base import DocumentParser, DocumentationSource, ExtractedImage


class PdfParser(DocumentParser):
    """Parser for extracting content from PDF files.

    Supports extracting both text content and embedded images,
    which is useful for analyzing ERD diagrams and other visual
    documentation.
    """

    def can_parse(self, source: str) -> bool:
        """Check if this parser can handle the given source."""
        path = Path(source)
        return path.suffix.lower() == ".pdf" and path.exists()

    async def parse(
        self,
        source: str,
        extract_images: bool = True,
    ) -> DocumentationSource:
        """Parse a PDF file and extract its content.

        Args:
            source: Path to PDF file
            extract_images: Whether to extract embedded images (default: True)

        Returns:
            DocumentationSource with extracted content and images
        """
        try:
            # Try pypdf first (preferred)
            content, metadata = self._parse_with_pypdf(source)
            images = []
            if extract_images:
                images = self._extract_images_pypdf(source)
        except ImportError:
            try:
                # Fallback to pdfplumber
                content, metadata = self._parse_with_pdfplumber(source)
                images = []
                if extract_images:
                    images = self._extract_images_pdfplumber(source)
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
            images=images,
        )

    def extract_images(self, source: str) -> List[ExtractedImage]:
        """Extract images from a PDF file.

        This method extracts all embedded images from the PDF,
        which can then be analyzed using vision models for
        ERD diagram interpretation.

        Args:
            source: Path to PDF file

        Returns:
            List of ExtractedImage objects containing image data
        """
        try:
            return self._extract_images_pypdf(source)
        except ImportError:
            try:
                return self._extract_images_pdfplumber(source)
            except ImportError:
                raise ImportError(
                    "No PDF parser with image support available. "
                    "Install 'pypdf' with: pip install pypdf"
                )

    def _extract_images_pypdf(self, source: str) -> List[ExtractedImage]:
        """Extract images using pypdf library."""
        from pypdf import PdfReader

        images = []
        reader = PdfReader(source)

        for page_num, page in enumerate(reader.pages, start=1):
            try:
                # pypdf stores images in the /Resources/XObject dictionary
                if "/XObject" not in page.get("/Resources", {}):
                    continue

                x_objects = page["/Resources"]["/XObject"].get_object()

                for obj_name in x_objects:
                    x_obj = x_objects[obj_name]

                    if x_obj["/Subtype"] == "/Image":
                        try:
                            # Get image data
                            image_data = self._extract_image_data_pypdf(x_obj)
                            if image_data:
                                img_bytes, img_format = image_data

                                # Get dimensions
                                width = x_obj.get("/Width")
                                height = x_obj.get("/Height")

                                images.append(
                                    ExtractedImage(
                                        page_number=page_num,
                                        image_data=img_bytes,
                                        image_format=img_format,
                                        width=width,
                                        height=height,
                                    )
                                )
                        except Exception as e:
                            # Log but continue with other images
                            print(f"Warning: Failed to extract image from page {page_num}: {e}")
                            continue

            except Exception as e:
                # Page might not have XObjects
                continue

        return images

    def _extract_image_data_pypdf(
        self, x_obj
    ) -> Optional[Tuple[bytes, str]]:
        """Extract raw image bytes from a pypdf XObject.

        Returns:
            Tuple of (image_bytes, format) or None if extraction fails
        """
        try:
            # Check the filter to determine image format
            filters = x_obj.get("/Filter", [])
            if not isinstance(filters, list):
                filters = [filters]

            # Get the raw data
            data = x_obj.get_data()

            # Determine format based on filter
            if "/DCTDecode" in filters:
                return (data, "jpeg")
            elif "/JPXDecode" in filters:
                return (data, "jp2")
            elif "/FlateDecode" in filters or not filters:
                # PNG or raw bitmap - try to convert
                try:
                    # Try using PIL to convert
                    from PIL import Image

                    width = x_obj.get("/Width")
                    height = x_obj.get("/Height")
                    color_space = x_obj.get("/ColorSpace")
                    bits_per_component = x_obj.get("/BitsPerComponent", 8)

                    # Determine mode
                    if color_space == "/DeviceRGB":
                        mode = "RGB"
                    elif color_space == "/DeviceGray":
                        mode = "L"
                    elif color_space == "/DeviceCMYK":
                        mode = "CMYK"
                    else:
                        mode = "RGB"  # Default

                    # Create image from raw data
                    if width and height:
                        img = Image.frombytes(mode, (width, height), data)
                        # Convert to PNG
                        output = io.BytesIO()
                        img.save(output, format="PNG")
                        return (output.getvalue(), "png")
                except Exception:
                    # Return raw data as-is
                    return (data, "png")

            return None

        except Exception:
            return None

    def _extract_images_pdfplumber(self, source: str) -> List[ExtractedImage]:
        """Extract images using pdfplumber library (fallback).

        Note: pdfplumber has limited image extraction support.
        """
        import pdfplumber

        images = []

        with pdfplumber.open(source) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                try:
                    # pdfplumber can extract images if they exist
                    page_images = page.images

                    for img_info in page_images:
                        # pdfplumber provides image coordinates but not raw data
                        # We'd need to use the underlying PDF objects
                        # This is a simplified fallback
                        pass

                except Exception:
                    continue

        return images

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
