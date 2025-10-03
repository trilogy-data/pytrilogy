from abc import ABC, abstractmethod
from typing import List, Optional

from trilogy.ai.enums import Provider
from trilogy.ai.models import LLMMessage, LLMRequestOptions, LLMResponse


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
