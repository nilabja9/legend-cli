"""Claude API client for Pure code generation."""

import re
from typing import Optional
from anthropic import Anthropic
from .config import settings
from .models import EntityType, PureCode, GenerationRequest
from .prompts import get_prompt_for_entity_type


class ClaudeClient:
    """Client for generating Pure code using Claude API."""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or settings.anthropic_api_key
        self.model = model or settings.claude_model
        self._client: Optional[Anthropic] = None

    @property
    def client(self) -> Anthropic:
        """Get or create Anthropic client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError(
                    "Anthropic API key not configured. "
                    "Set ANTHROPIC_API_KEY environment variable."
                )
            self._client = Anthropic(api_key=self.api_key)
        return self._client

    def generate_pure_code(
        self,
        request: GenerationRequest,
    ) -> PureCode:
        """Generate Pure code from natural language description."""
        system_prompt = get_prompt_for_entity_type(request.entity_type.value)

        user_message = f"Generate a Pure {request.entity_type.value} for: {request.description}"
        if request.package:
            user_message += f"\nUse package: {request.package}"
        if request.additional_context:
            user_message += f"\nAdditional context: {request.additional_context}"

        response = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )

        generated_code = response.content[0].text.strip()

        # Extract path from generated code
        path = self._extract_path(generated_code, request.entity_type)

        return PureCode(
            entity_type=request.entity_type,
            path=path,
            code=generated_code,
            description=request.description,
        )

    def _extract_path(self, code: str, entity_type: EntityType) -> str:
        """Extract the entity path from generated Pure code."""
        patterns = {
            EntityType.CLASS: r"Class\s+([\w:]+)",
            EntityType.STORE: r"Database\s+([\w:]+)",
            EntityType.CONNECTION: r"RelationalDatabaseConnection\s+([\w:]+)",
            EntityType.MAPPING: r"Mapping\s+([\w:]+)",
        }

        pattern = patterns.get(entity_type)
        if pattern:
            match = re.search(pattern, code)
            if match:
                return match.group(1)

        return f"model::generated::{entity_type.value}"

    def generate_class(
        self,
        description: str,
        package: str = "model::domain",
    ) -> PureCode:
        """Generate a Pure class from description."""
        return self.generate_pure_code(
            GenerationRequest(
                entity_type=EntityType.CLASS,
                description=description,
                package=package,
            )
        )

    def generate_store(
        self,
        description: str,
        package: str = "model::store",
    ) -> PureCode:
        """Generate a Pure database store from description."""
        return self.generate_pure_code(
            GenerationRequest(
                entity_type=EntityType.STORE,
                description=description,
                package=package,
            )
        )

    def generate_connection(
        self,
        description: str,
        package: str = "model::connection",
        store_path: Optional[str] = None,
    ) -> PureCode:
        """Generate a Pure connection from description."""
        additional_context = None
        if store_path:
            additional_context = f"Reference store: {store_path}"

        return self.generate_pure_code(
            GenerationRequest(
                entity_type=EntityType.CONNECTION,
                description=description,
                package=package,
                additional_context=additional_context,
            )
        )

    def generate_mapping(
        self,
        description: str,
        package: str = "model::mapping",
        store_path: Optional[str] = None,
        class_paths: Optional[list] = None,
    ) -> PureCode:
        """Generate a Pure mapping from description."""
        additional_context_parts = []
        if store_path:
            additional_context_parts.append(f"Store: {store_path}")
        if class_paths:
            additional_context_parts.append(f"Classes: {', '.join(class_paths)}")

        return self.generate_pure_code(
            GenerationRequest(
                entity_type=EntityType.MAPPING,
                description=description,
                package=package,
                additional_context="\n".join(additional_context_parts) if additional_context_parts else None,
            )
        )
