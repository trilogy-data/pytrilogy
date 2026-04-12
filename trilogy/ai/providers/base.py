import json
from abc import ABC, abstractmethod
from typing import Any, List, Optional

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMRequestOptions, LLMResponse

RETRYABLE_CODES = [429, 500, 502, 503, 504]


class LLMProvider(ABC):
    def __init__(self, name: str, api_key: str, model: str, provider: Provider):
        self.api_key = api_key
        self.models: List[str] = []
        self.name = name
        self.model = model
        self.type = provider
        self.error: Optional[str] = None

    # Abstract method to be implemented by specific providers
    @abstractmethod
    def generate_completion(
        self, options: LLMRequestOptions, history: List[LLMMessage]
    ) -> LLMResponse:
        pass


def parse_tool_arguments(arguments: str | dict[str, Any] | None) -> dict[str, Any]:
    if arguments is None:
        return {}
    if isinstance(arguments, dict):
        return arguments
    if not arguments.strip():
        return {}
    parsed = json.loads(arguments)
    if not isinstance(parsed, dict):
        raise ValueError(f"Tool arguments must decode to an object, got {type(parsed)}")
    return parsed
