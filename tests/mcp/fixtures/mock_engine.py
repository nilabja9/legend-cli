"""Mock Engine client for testing."""

from typing import Dict, Any, List, Optional, Callable
import re


class MockEngineClient:
    """Mock EngineClient for testing without a real Engine server.

    Supports pattern-based response matching and uses real extract_entities logic
    for accurate testing.
    """

    def __init__(self):
        self._responses: Dict[str, Dict[str, Any]] = {}
        self._default_response: Optional[Dict[str, Any]] = None
        self._call_history: List[Dict[str, Any]] = []
        self._pattern_responses: List[tuple] = []  # List of (pattern, response) tuples

    def add_response(self, code_pattern: str, response: Dict[str, Any]):
        """Add a response for a specific code pattern (regex).

        Args:
            code_pattern: Regex pattern to match against Pure code
            response: Response to return when pattern matches
        """
        self._pattern_responses.append((re.compile(code_pattern, re.DOTALL), response))

    def set_default_response(self, response: Dict[str, Any]):
        """Set the default response when no pattern matches."""
        self._default_response = response

    def grammar_to_json(self, pure_code: str) -> Dict[str, Any]:
        """Mock grammar_to_json that returns configured responses."""
        self._call_history.append({"method": "grammar_to_json", "code": pure_code})

        # Try pattern matching first
        for pattern, response in self._pattern_responses:
            if pattern.search(pure_code):
                return response

        # Fall back to default response
        if self._default_response:
            return self._default_response

        # Return empty response if nothing configured
        return {"modelDataContext": {"elements": []}}

    def extract_entities(self, grammar_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract entities using the same logic as the real EngineClient.

        This ensures tests accurately reflect production behavior.
        """
        entities = []

        # Collect elements from all possible locations
        all_elements = []

        # Check modelDataContext.elements (primary location)
        model_data = grammar_result.get("modelDataContext", {})
        if isinstance(model_data, dict):
            elements = model_data.get("elements", [])
            if isinstance(elements, list):
                all_elements.extend(elements)

            # Check modelDataContext.stores (alternative for Database definitions)
            stores = model_data.get("stores", [])
            if isinstance(stores, list):
                all_elements.extend(stores)

        # Check pureModelContextData.elements (alternative structure)
        pure_model_data = grammar_result.get("pureModelContextData", {})
        if isinstance(pure_model_data, dict):
            pmcd_elements = pure_model_data.get("elements", [])
            if isinstance(pmcd_elements, list):
                all_elements.extend(pmcd_elements)

            # Check pureModelContextData.stores
            pmcd_stores = pure_model_data.get("stores", [])
            if isinstance(pmcd_stores, list):
                all_elements.extend(pmcd_stores)

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
                continue

            element_type = element.get("_type")
            # Skip internal elements like sectionIndex
            if element_type == "sectionIndex":
                continue

            package = element.get("package", "")
            name = element.get("name", "")
            path = f"{package}::{name}" if package else name

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

    def debug_parse_response(self, pure_code: str) -> Dict[str, Any]:
        """Debug version of parse that returns diagnostic info."""
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

            if "modelDataContext" in raw_response:
                mdc = raw_response["modelDataContext"]
                if isinstance(mdc, dict):
                    diag_lines.append(f"modelDataContext keys: {list(mdc.keys())}")
                    elements = mdc.get("elements", [])
                    stores = mdc.get("stores", [])
                    diag_lines.append(f"modelDataContext.elements count: {len(elements) if isinstance(elements, list) else 'N/A'}")
                    diag_lines.append(f"modelDataContext.stores count: {len(stores) if isinstance(stores, list) else 'N/A'}")

            if "pureModelContextData" in raw_response:
                pmcd = raw_response["pureModelContextData"]
                if isinstance(pmcd, dict):
                    diag_lines.append(f"pureModelContextData keys: {list(pmcd.keys())}")

            result["diagnostic"] = "\n".join(diag_lines)
            result["entities"] = self.extract_entities(raw_response)

            if not result["entities"]:
                result["error"] = "No entities extracted from response"

        except Exception as e:
            result["error"] = str(e)
            result["diagnostic"] = f"Parse failed with error: {e}"

        return result

    def health_check(self) -> bool:
        """Mock health check always returns True."""
        return True

    def get_call_history(self) -> List[Dict[str, Any]]:
        """Get the history of method calls for assertions."""
        return self._call_history

    def clear_call_history(self):
        """Clear the call history."""
        self._call_history.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def create_mock_engine_with_store_response() -> MockEngineClient:
    """Create a MockEngineClient configured to return store responses.

    Returns a mock that returns appropriate responses for:
    - ###Relational blocks -> relational database entity
    - ###Pure blocks -> class entities
    - ###Connection blocks -> connection entity
    - ###Mapping blocks -> mapping entity
    """
    mock = MockEngineClient()

    # Configure responses for different Pure code patterns
    mock.add_response(
        r"###Relational\s+Database",
        {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "relational",
                        "package": "model::store",
                        "name": "TestDB"
                    }
                ]
            }
        }
    )

    mock.add_response(
        r"###Pure\s+Class",
        {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "class",
                        "package": "model::domain",
                        "name": "TestClass"
                    }
                ]
            }
        }
    )

    mock.add_response(
        r"###Connection",
        {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "relationalDatabaseConnection",
                        "package": "model::connection",
                        "name": "TestConnection"
                    }
                ]
            }
        }
    )

    mock.add_response(
        r"###Mapping",
        {
            "modelDataContext": {
                "elements": [
                    {
                        "_type": "mapping",
                        "package": "model::mapping",
                        "name": "TestMapping"
                    }
                ]
            }
        }
    )

    return mock


def create_mock_engine_with_bug() -> MockEngineClient:
    """Create a MockEngineClient that reproduces the '0 entities' bug.

    This simulates the case where the Engine returns elements in
    an unexpected location (e.g., 'stores' instead of 'elements').
    """
    mock = MockEngineClient()

    # This simulates the bug: elements returned in 'stores' key
    mock.set_default_response({
        "modelDataContext": {
            "elements": [],
            "stores": [
                {
                    "_type": "relational",
                    "package": "model::store",
                    "name": "TestDB"
                }
            ]
        }
    })

    return mock
