"""Legend Engine API client for grammar parsing."""

import httpx
from typing import Optional, Dict, Any, List
from .config import settings


class EngineClient:
    """Client for Legend Engine API."""

    def __init__(self, base_url: Optional[str] = None):
        # Derive engine URL from SDLC URL
        sdlc_url = base_url or settings.legend_sdlc_url
        # Convert http://localhost:6900/sdlc/api to http://localhost:6900/engine/api
        self.base_url = sdlc_url.replace("/sdlc/api", "/engine/api")
        self._client: Optional[httpx.Client] = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.base_url,
                headers={"Content-Type": "application/json"},
                timeout=60.0,
            )
        return self._client

    def close(self):
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def grammar_to_json(self, pure_code: str) -> Dict[str, Any]:
        """Convert Pure grammar to JSON protocol format."""
        response = self.client.post(
            "/pure/v1/grammar/transformGrammarToJson",
            json={"code": pure_code},
        )
        response.raise_for_status()
        return response.json()

    def json_to_grammar(self, model_data: Dict[str, Any]) -> str:
        """Convert JSON protocol to Pure grammar."""
        response = self.client.post(
            "/pure/v1/grammar/transformJsonToGrammar",
            json=model_data,
        )
        response.raise_for_status()
        result = response.json()
        return result.get("code", "")

    def extract_entities(self, grammar_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract entity definitions from grammar transformation result."""
        entities = []
        model_data = grammar_result.get("modelDataContext", {})
        elements = model_data.get("elements", [])

        for element in elements:
            element_type = element.get("_type")
            # Skip internal elements like sectionIndex
            if element_type == "sectionIndex":
                continue

            package = element.get("package", "")
            name = element.get("name", "")
            path = f"{package}::{name}" if package else name

            # Map element type to classifier path
            classifier_map = {
                "class": "meta::pure::metamodel::type::Class",
                "relationalDatabase": "meta::relational::metamodel::Database",
                "relationalDatabaseConnection": "meta::pure::runtime::PackageableConnection",
                "mapping": "meta::pure::mapping::Mapping",
                "function": "meta::pure::metamodel::function::ConcreteFunctionDefinition",
                "packageableRuntime": "meta::pure::runtime::PackageableRuntime",
                "association": "meta::pure::metamodel::relationship::Association",
                "profile": "meta::pure::metamodel::extension::Profile",
                "enumeration": "meta::pure::metamodel::type::Enumeration",
            }

            classifier = classifier_map.get(element_type, f"meta::pure::metamodel::{element_type}")

            entities.append({
                "path": path,
                "classifierPath": classifier,
                "content": element,
            })

        return entities

    def parse_pure_code(self, pure_code: str) -> List[Dict[str, Any]]:
        """Parse Pure code and return entities ready for SDLC."""
        result = self.grammar_to_json(pure_code)
        return self.extract_entities(result)

    def parse_multiple_pure_codes(self, pure_codes: Dict[str, str]) -> List[Dict[str, Any]]:
        """Parse multiple Pure code blocks and return combined entities.

        This is useful for batching all artifacts into a single SDLC push,
        ensuring proper ordering and atomic transaction.

        Args:
            pure_codes: Dict mapping artifact name to Pure code string

        Returns:
            Combined list of all entities from all artifacts
        """
        all_entities = []
        for artifact_name, code in pure_codes.items():
            if not code or not code.strip():
                continue
            entities = self.parse_pure_code(code)
            all_entities.extend(entities)
        return all_entities

    def health_check(self) -> bool:
        """Check if Engine is healthy."""
        try:
            response = self.client.get("/server/v1/info")
            return response.status_code == 200
        except Exception:
            return False
