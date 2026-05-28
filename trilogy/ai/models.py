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
    # Set when the model emitted a tool call whose arguments could not be
    # parsed; the agent surfaces this back to the model instead of crashing.
    parse_error: str | None = None


@dataclass
class LLMResponse:
    text: str
    usage: UsageDict
    tool_calls: list[LLMToolCall] = field(default_factory=list)
    # Raw finish_reason / stop_reason from the provider. "length" means the
    # output was truncated by max_tokens — tool_call arguments may be partial
    # (or auto-closed JSON), which silently corrupts write_file payloads.
    finish_reason: Optional[str] = None


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
