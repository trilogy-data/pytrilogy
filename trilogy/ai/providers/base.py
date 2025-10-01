from abc import ABC, abstractmethod
from typing import List, Optional

from trilogy.ai.models import LLMMessage, LLMRequestOptions, LLMResponse


class LLMProvider(ABC):
    def __init__(
        self, name: str, api_key: str, model: str, save_credential: bool = False
    ):
        self.api_key = api_key
        self.models: List[str] = []
        self.name = name
        self.model = model
        # revisit if we load storage from any other location
        self.storage = "local"
        self.type = "generic"
        self.connected = False
        self.error: Optional[str] = None
        self.save_credential = save_credential
        self.is_default = False
        self.changed = False
        self.deleted = False

    def set_api_key(self, api_key: str) -> None:
        if self.api_key == api_key:
            return  # No change, do nothing
        self.changed = True
        self.api_key = api_key

    def set_model(self, model: str) -> None:
        if self.model == model:
            return  # No change, do nothing
        self.changed = True
        self.model = model

    def get_api_key(self) -> str:
        return self.api_key

    def delete(self) -> None:
        self.deleted = True
        self.changed = True

    @abstractmethod
    def reset(self) -> None:
        pass

    # Abstract method to be implemented by specific providers
    @abstractmethod
    def generate_completion(
        self, options: LLMRequestOptions, history: List[LLMMessage]
    ) -> LLMResponse:
        pass

    # Convert instance to JSON
    def to_json(self) -> dict:
        return {
            "name": self.name,
            "model": self.model,
            "type": self.type,
            # redacted will trigger a fetch from the secure store
            "apiKey": "saved" if self.save_credential else None,
            "saveCredential": self.save_credential,
            "isDefault": self.is_default,
        }

    def get_credential_name(self) -> str:
        return f"trilogy-llm-{self.type}"

    # Create instance from JSON
    @classmethod
    def from_json(cls, json_data):
        import json as json_module

        if isinstance(json_data, str):
            restored = json_module.loads(json_data)
        else:
            restored = json_data

        instance = cls(
            name=restored["name"],
            api_key=restored["apiKey"],
            model=restored["model"],
            save_credential=restored.get("saveCredential", False),
        )
        instance.is_default = restored.get("isDefault", False)
        return instance
