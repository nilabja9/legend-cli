"""Legend Engine API client for grammar parsing."""

import logging
import httpx
from typing import Optional, Dict, Any, List
from .config import settings

logger = logging.getLogger(__name__)


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
        result = response.json()

        # Debug logging for response structure
        logger.debug("grammar_to_json response keys: %s", list(result.keys()))
        if "modelDataContext" in result:
            mdc = result["modelDataContext"]
            logger.debug("modelDataContext keys: %s", list(mdc.keys()) if isinstance(mdc, dict) else type(mdc))
            if isinstance(mdc, dict):
                elements = mdc.get("elements", [])
                stores = mdc.get("stores", [])
                logger.debug("modelDataContext.elements count: %d", len(elements) if isinstance(elements, list) else 0)
                logger.debug("modelDataContext.stores count: %d", len(stores) if isinstance(stores, list) else 0)
                if elements and isinstance(elements, list) and len(elements) > 0:
                    logger.debug("First element _type: %s", elements[0].get("_type") if isinstance(elements[0], dict) else "N/A")
        if "pureModelContextData" in result:
            pmcd = result["pureModelContextData"]
            logger.debug("pureModelContextData keys: %s", list(pmcd.keys()) if isinstance(pmcd, dict) else type(pmcd))

        return result

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
        """Extract entity definitions from grammar transformation result.

        Handles multiple response structures from Engine API:
        - modelDataContext.elements (standard)
        - modelDataContext.stores (alternative for Database definitions)
        - pureModelContextData.elements (alternative structure)
        """
        entities = []

        # Log input structure for debugging
        logger.debug("extract_entities input keys: %s", list(grammar_result.keys()))

        # Collect elements from all possible locations
        all_elements = []

        # Check modelDataContext.elements (primary location)
        model_data = grammar_result.get("modelDataContext", {})
        if isinstance(model_data, dict):
            elements = model_data.get("elements", [])
            if isinstance(elements, list) and elements:
                logger.debug("Found %d elements in modelDataContext.elements", len(elements))
                all_elements.extend(elements)

            # Check modelDataContext.stores (alternative for Database definitions)
            stores = model_data.get("stores", [])
            if isinstance(stores, list) and stores:
                logger.debug("Found %d elements in modelDataContext.stores", len(stores))
                all_elements.extend(stores)

        # Check pureModelContextData.elements (alternative structure)
        pure_model_data = grammar_result.get("pureModelContextData", {})
        if isinstance(pure_model_data, dict):
            pmcd_elements = pure_model_data.get("elements", [])
            if isinstance(pmcd_elements, list) and pmcd_elements:
                logger.debug("Found %d elements in pureModelContextData.elements", len(pmcd_elements))
                all_elements.extend(pmcd_elements)

            # Check pureModelContextData.stores
            pmcd_stores = pure_model_data.get("stores", [])
            if isinstance(pmcd_stores, list) and pmcd_stores:
                logger.debug("Found %d elements in pureModelContextData.stores", len(pmcd_stores))
                all_elements.extend(pmcd_stores)

        # Log warning if no elements found
        if not all_elements:
            logger.warning("No elements found in grammar result. Available keys: %s", list(grammar_result.keys()))
            if model_data:
                logger.warning("modelDataContext keys: %s", list(model_data.keys()) if isinstance(model_data, dict) else type(model_data))

        # Map element type to classifier path (including case variants)
        classifier_map = {
            # Class types
            "class": "meta::pure::metamodel::type::Class",
            "Class": "meta::pure::metamodel::type::Class",
            # Database/Store types
            "relational": "meta::relational::metamodel::Database",
            "Relational": "meta::relational::metamodel::Database",
            "database": "meta::relational::metamodel::Database",
            "Database": "meta::relational::metamodel::Database",
            "relationalDatabase": "meta::relational::metamodel::Database",
            "RelationalDatabase": "meta::relational::metamodel::Database",
            # Connection types
            "relationalDatabaseConnection": "meta::pure::runtime::PackageableConnection",
            "RelationalDatabaseConnection": "meta::pure::runtime::PackageableConnection",
            "connection": "meta::pure::runtime::PackageableConnection",
            "Connection": "meta::pure::runtime::PackageableConnection",
            "PackageableConnection": "meta::pure::runtime::PackageableConnection",
            # Mapping types
            "mapping": "meta::pure::mapping::Mapping",
            "Mapping": "meta::pure::mapping::Mapping",
            # Function types
            "function": "meta::pure::metamodel::function::ConcreteFunctionDefinition",
            "Function": "meta::pure::metamodel::function::ConcreteFunctionDefinition",
            # Runtime types
            "packageableRuntime": "meta::pure::runtime::PackageableRuntime",
            "PackageableRuntime": "meta::pure::runtime::PackageableRuntime",
            "runtime": "meta::pure::runtime::PackageableRuntime",
            "Runtime": "meta::pure::runtime::PackageableRuntime",
            # Association types
            "association": "meta::pure::metamodel::relationship::Association",
            "Association": "meta::pure::metamodel::relationship::Association",
            # Other types
            "profile": "meta::pure::metamodel::extension::Profile",
            "Profile": "meta::pure::metamodel::extension::Profile",
            "enumeration": "meta::pure::metamodel::type::Enumeration",
            "Enumeration": "meta::pure::metamodel::type::Enumeration",
        }

        for element in all_elements:
            if not isinstance(element, dict):
                logger.warning("Skipping non-dict element: %s", type(element))
                continue

            element_type = element.get("_type")
            # Skip internal elements like sectionIndex
            if element_type == "sectionIndex":
                continue

            package = element.get("package", "")
            name = element.get("name", "")
            path = f"{package}::{name}" if package else name

            if element_type in classifier_map:
                classifier = classifier_map[element_type]
            else:
                # Log warning for unknown element types
                logger.warning("Unknown element type '%s' for entity '%s', using fallback classifier", element_type, path)
                classifier = f"meta::pure::metamodel::{element_type}"

            logger.debug("Extracted entity: path=%s, type=%s, classifier=%s", path, element_type, classifier)

            entities.append({
                "path": path,
                "classifierPath": classifier,
                "content": element,
            })

        logger.debug("Total entities extracted: %d", len(entities))
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

    def debug_parse_response(self, pure_code: str) -> Dict[str, Any]:
        """Parse Pure code and return detailed diagnostic information.

        This method is useful for debugging parsing failures. It returns
        the raw response, diagnostic info about the response structure,
        and the extracted entities.

        Args:
            pure_code: Pure code to parse

        Returns:
            Dict with:
                - raw_response: The raw JSON response from Engine
                - diagnostic: Human-readable diagnostic info
                - entities: Extracted entities (if any)
                - error: Error message (if parsing failed)
        """
        result = {
            "raw_response": None,
            "diagnostic": "",
            "entities": [],
            "error": None,
        }

        try:
            raw_response = self.grammar_to_json(pure_code)
            result["raw_response"] = raw_response

            # Build diagnostic info
            diag_lines = []
            diag_lines.append(f"Response keys: {list(raw_response.keys())}")

            # Analyze modelDataContext
            if "modelDataContext" in raw_response:
                mdc = raw_response["modelDataContext"]
                if isinstance(mdc, dict):
                    diag_lines.append(f"modelDataContext keys: {list(mdc.keys())}")
                    elements = mdc.get("elements", [])
                    stores = mdc.get("stores", [])
                    diag_lines.append(f"modelDataContext.elements count: {len(elements) if isinstance(elements, list) else 'N/A'}")
                    diag_lines.append(f"modelDataContext.stores count: {len(stores) if isinstance(stores, list) else 'N/A'}")
                    if elements and isinstance(elements, list):
                        types = [e.get("_type", "unknown") for e in elements if isinstance(e, dict)]
                        diag_lines.append(f"Element _types: {types}")
                else:
                    diag_lines.append(f"modelDataContext is not a dict: {type(mdc)}")
            else:
                diag_lines.append("modelDataContext: NOT PRESENT")

            # Analyze pureModelContextData
            if "pureModelContextData" in raw_response:
                pmcd = raw_response["pureModelContextData"]
                if isinstance(pmcd, dict):
                    diag_lines.append(f"pureModelContextData keys: {list(pmcd.keys())}")
                    pmcd_elements = pmcd.get("elements", [])
                    pmcd_stores = pmcd.get("stores", [])
                    diag_lines.append(f"pureModelContextData.elements count: {len(pmcd_elements) if isinstance(pmcd_elements, list) else 'N/A'}")
                    diag_lines.append(f"pureModelContextData.stores count: {len(pmcd_stores) if isinstance(pmcd_stores, list) else 'N/A'}")
            else:
                diag_lines.append("pureModelContextData: NOT PRESENT")

            result["diagnostic"] = "\n".join(diag_lines)

            # Extract entities
            entities = self.extract_entities(raw_response)
            result["entities"] = entities

            if not entities:
                result["error"] = "No entities extracted from response"

        except Exception as e:
            result["error"] = str(e)
            result["diagnostic"] = f"Parse failed with error: {e}"

        return result
