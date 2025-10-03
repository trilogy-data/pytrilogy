from dataclasses import dataclass
from typing import Literal, Optional


@dataclass
class UsageDict:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


@dataclass
class LLMResponse:
    text: str
    usage: UsageDict


@dataclass
class LLMRequestOptions:
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None


@dataclass
class LLMMessage:
    role: Literal["user", "assistant", "system"]
    content: str
    model_info: Optional[dict] = None
    hidden: bool = False  # Used to hide messages in the UI

    def __post_init__(self):
        if self.model_info is None:
            self.model_info = {}
