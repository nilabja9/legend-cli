"""URL/Web page parser for extracting documentation content."""

import httpx
from bs4 import BeautifulSoup
from typing import Optional

from .base import DocumentParser, DocumentationSource


class UrlParser(DocumentParser):
    """Parser for extracting content from web pages."""

    def __init__(self, timeout: float = 30.0):
        """Initialize the URL parser.

        Args:
            timeout: HTTP request timeout in seconds
        """
        self.timeout = timeout

    def can_parse(self, source: str) -> bool:
        """Check if this parser can handle the given source."""
        return source.lower().startswith(("http://", "https://"))

    async def parse(self, source: str) -> DocumentationSource:
        """Parse a web page and extract its content.

        Args:
            source: URL to parse

        Returns:
            DocumentationSource with extracted content
        """
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            response = await client.get(source)
            response.raise_for_status()

            html_content = response.text
            soup = BeautifulSoup(html_content, "lxml")

            # Extract metadata
            title = self._extract_title(soup)
            metadata = {
                "title": title,
                "url": source,
                "status_code": response.status_code,
            }

            # Extract main content
            content = self._extract_content(soup)

            return DocumentationSource(
                source_type="url",
                source_path=source,
                content=content,
                metadata=metadata,
            )

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract the page title."""
        title_tag = soup.find("title")
        if title_tag:
            return title_tag.get_text(strip=True)

        h1_tag = soup.find("h1")
        if h1_tag:
            return h1_tag.get_text(strip=True)

        return ""

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract main text content from HTML.

        Preserves structure by keeping headings, paragraphs, and tables.
        Removes scripts, styles, and navigation elements.
        """
        # Remove unwanted elements
        for element in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
            element.decompose()

        # Try to find main content area
        main_content = (
            soup.find("main")
            or soup.find("article")
            or soup.find(class_=["content", "main-content", "documentation", "docs"])
            or soup.find(id=["content", "main-content", "documentation", "docs"])
            or soup.body
        )

        if not main_content:
            main_content = soup

        # Extract text with structure
        lines = []

        for element in main_content.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "td", "th", "pre", "code"]):
            text = element.get_text(strip=True)
            if text:
                # Add heading markers
                if element.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                    level = int(element.name[1])
                    prefix = "#" * level
                    lines.append(f"\n{prefix} {text}\n")
                elif element.name == "li":
                    lines.append(f"- {text}")
                elif element.name in ["td", "th"]:
                    lines.append(f"| {text} |")
                else:
                    lines.append(text)

        return "\n".join(lines)
