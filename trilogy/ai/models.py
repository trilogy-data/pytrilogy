from dataclasses import dataclass, field
from typing import Any, Literal, Optional


@dataclass
class UsageDict:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


@dataclass
class LLMToolDefinition:
    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass
class LLMToolCall:
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMResponse:
    text: str
    usage: UsageDict
    tool_calls: list[LLMToolCall] = field(default_factory=list)


@dataclass
class LLMRequestOptions:
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    tools: list[LLMToolDefinition] = field(default_factory=list)
    require_tool: bool = False
    tool_choice: Optional[str] = None


@dataclass
class LLMMessage:
    role: Literal["user", "assistant", "system"]
    content: str
    model_info: Optional[dict] = None
    hidden: bool = False  # Used to hide messages in the UI

    def __post_init__(self):
        if self.model_info is None:
            self.model_info = {}
