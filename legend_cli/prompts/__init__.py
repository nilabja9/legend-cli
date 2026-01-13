"""Prompt templates for Pure code generation."""

from .templates import (
    CLASS_SYSTEM_PROMPT,
    STORE_SYSTEM_PROMPT,
    CONNECTION_SYSTEM_PROMPT,
    MAPPING_SYSTEM_PROMPT,
    get_prompt_for_entity_type,
)
from .examples import (
    CLASS_EXAMPLES,
    STORE_EXAMPLES,
    CONNECTION_EXAMPLES,
    MAPPING_EXAMPLES,
)

__all__ = [
    "CLASS_SYSTEM_PROMPT",
    "STORE_SYSTEM_PROMPT",
    "CONNECTION_SYSTEM_PROMPT",
    "MAPPING_SYSTEM_PROMPT",
    "get_prompt_for_entity_type",
    "CLASS_EXAMPLES",
    "STORE_EXAMPLES",
    "CONNECTION_EXAMPLES",
    "MAPPING_EXAMPLES",
]
