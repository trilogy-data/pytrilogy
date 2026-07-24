from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class UsageDict:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    # Reasoning/thinking tokens, when the provider reports them separately
    # (e.g. Gemini's thoughtsTokenCount). Not included in completion_tokens.
    reasoning_tokens: int = 0


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
    finish_reason: str | None = None
    # Thought-summary text, when the provider returns reasoning (e.g. Gemini
    # parts flagged `thought: true`). Surfaced into the conversation trace.
    reasoning: str | None = None


@dataclass
class LLMRequestOptions:
    max_tokens: int | None = None
    temperature: float | None = None
    top_p: float | None = None
    tools: list[LLMToolDefinition] = field(default_factory=list)
    require_tool: bool = False
    tool_choice: str | None = None


@dataclass
class LLMMessage:
    role: Literal["user", "assistant", "system"]
    content: str
    model_info: dict | None = None
    hidden: bool = False  # Used to hide messages in the UI

    def __post_init__(self):
        if self.model_info is None:
            self.model_info = {}
