import pytest

from trilogy.ai.enums import Provider
from trilogy.ai.models import (
    LLMMessage,
    LLMRequestOptions,
    LLMResponse,
    LLMToolCall,
    UsageDict,
)
from trilogy.ai.providers.anthropic import AnthropicProvider
from trilogy.ai.providers.base import LLMProvider
from trilogy.ai.providers.openai import OpenAIProvider
from trilogy.execution.config import AgentConfig
from trilogy.scripts import agent as agent_mod
from trilogy.scripts import display_core
from trilogy.scripts.agent import (
    ALL_TOOLS,
    AgentState,
    TodoItem,
    _build_provider,
    _dispatch,
    _run_turn,
    handle_return_control,
    handle_show_message,
    handle_todo,
    handle_trilogy,
    truncate_middle,
)

try:
    import rich  # noqa: F401

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class ScriptedProvider(LLMProvider):
    def __init__(self, responses: list[LLMResponse]):
        super().__init__("scripted", "test-key", "test-model", Provider.OPENAI)
        self.responses = responses
        self.call_count = 0

    def generate_completion(
        self, options: LLMRequestOptions, history: list[LLMMessage]
    ) -> LLMResponse:
        self.call_count += 1
        return self.responses.pop(0)


def make_response(*tool_calls: LLMToolCall, text: str = "") -> LLMResponse:
    return LLMResponse(
        text=text,
        usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
        tool_calls=list(tool_calls),
    )


def call(name: str, **args) -> LLMToolCall:
    return LLMToolCall(name=name, arguments=args)


def make_conv(provider: ScriptedProvider):
    from trilogy.ai.conversation import Conversation

    conv = Conversation.create(provider, model_prompt="test-system")
    conv.add_message("do the thing", role="user")
    return conv


# --- truncate_middle ---


def test_truncate_middle_short_passthrough():
    assert truncate_middle("abc", 100) == "abc"


def test_truncate_middle_preserves_head_and_tail():
    text = "A" * 100 + "B" * 100
    out = truncate_middle(text, 80)
    assert out.startswith("A")
    assert out.endswith("B")
    assert "truncated" in out
    assert len(out) <= 80 + len("\n...[truncated 120 bytes]...\n")


def test_truncate_middle_tiny_limit_still_returns_something():
    out = truncate_middle("0123456789", 3)
    assert "truncated" in out
    assert out


# --- todo handler ---


def test_todo_add_assigns_id_and_lists():
    state = AgentState()
    result = handle_todo(state, {"action": "add", "description": "step one"})
    assert "todo added" in result
    assert len(state.todos) == 1
    assert state.todos[0].description == "step one"


def test_todo_complete_marks_item():
    state = AgentState(todos=[TodoItem(id="aaa", description="x")])
    handle_todo(state, {"action": "complete", "id": "aaa"})
    assert state.todos[0].completed is True


def test_todo_remove_drops_item():
    state = AgentState(todos=[TodoItem(id="aaa", description="x")])
    handle_todo(state, {"action": "remove", "id": "aaa"})
    assert state.todos == []


def test_todo_list_empty():
    assert "empty" in handle_todo(AgentState(), {"action": "list"}).lower()


def test_todo_unknown_action():
    assert "unknown action" in handle_todo(AgentState(), {"action": "nope"})


def test_todo_add_requires_description():
    assert "required" in handle_todo(AgentState(), {"action": "add"})


def test_todo_complete_requires_valid_id():
    assert "no item with id" in handle_todo(
        AgentState(todos=[TodoItem(id="a", description="x")]),
        {"action": "complete", "id": "missing"},
    )


# --- return_control_to_user gating ---


def test_return_control_blocked_by_open_todos():
    state = AgentState(todos=[TodoItem(id="a", description="x")])
    result = handle_return_control(state, {"message": "done"})
    assert "refused" in result
    assert state.done is False


def test_return_control_succeeds_when_todos_complete():
    state = AgentState(todos=[TodoItem(id="a", description="x", completed=True)])
    result = handle_return_control(state, {"message": "all good"})
    assert result == "return_control_to_user: ok"
    assert state.done is True
    assert state.farewell == "all good"


def test_return_control_succeeds_with_no_todos():
    state = AgentState()
    handle_return_control(state, {"message": "ok"})
    assert state.done is True


# --- show_message uses rich helpers with fallback ---


@pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich not installed")
def test_show_message_uses_rich_console(capsys):
    with display_core.set_rich_mode(True):
        handle_show_message(AgentState(), {"message": "hello from agent"})
    captured = capsys.readouterr()
    assert "hello from agent" in captured.out


def test_show_message_falls_back_without_rich(capsys):
    with display_core.set_rich_mode(False):
        handle_show_message(AgentState(), {"message": "hello fallback"})
    captured = capsys.readouterr()
    assert "hello fallback" in captured.out


def test_show_message_validates_input():
    assert "non-empty string" in handle_show_message(AgentState(), {})


# --- trilogy subprocess tool ---


def test_trilogy_tool_runs_subprocess():
    result = handle_trilogy(AgentState(), {"args": ["--version"]})
    assert "exit_code: 0" in result
    assert "v" in result


def test_trilogy_tool_truncates_middle():
    state = AgentState(tool_output_limit=80)
    result = handle_trilogy(state, {"args": ["--help"]})
    assert "exit_code: 0" in result
    assert "truncated" in result


def test_trilogy_tool_rejects_bad_args():
    assert "list of strings" in handle_trilogy(AgentState(), {"args": "run"})


def test_trilogy_tool_rejects_bad_stdin():
    assert "string or null" in handle_trilogy(
        AgentState(), {"args": ["--version"], "stdin": 42}
    )


# --- _dispatch routing ---


def test_dispatch_unknown_tool():
    result = _dispatch(AgentState(), LLMToolCall(name="bogus", arguments={}))
    assert "Unknown tool" in result


def test_dispatch_catches_handler_exceptions(monkeypatch):
    def boom(state, args):
        raise RuntimeError("kaboom")

    monkeypatch.setitem(agent_mod.TOOL_HANDLERS, "show_message", boom)
    result = _dispatch(AgentState(), LLMToolCall(name="show_message"))
    assert "RuntimeError" in result
    assert "kaboom" in result


# --- _run_turn loop ---


def test_run_turn_happy_path_returns_control():
    provider = ScriptedProvider(
        responses=[make_response(call("return_control_to_user", message="all done"))]
    )
    conv = make_conv(provider)
    state = AgentState()
    _run_turn(conv, state, max_iterations=5)
    assert state.done is True
    assert state.farewell == "all done"
    assert provider.call_count == 1


def test_run_turn_reprompts_when_no_tool_calls():
    provider = ScriptedProvider(
        responses=[
            make_response(text="plain text, no tool"),
            make_response(call("return_control_to_user", message="ok")),
        ]
    )
    conv = make_conv(provider)
    state = AgentState()
    _run_turn(conv, state, max_iterations=5)
    assert state.done is True
    assert provider.call_count == 2
    assert any(
        "must call a tool" in m.content for m in conv.messages if m.role == "user"
    )


def test_run_turn_blocks_return_until_todos_completed(monkeypatch):
    class _FixedUUID:
        hex = "abcd1234ffff"

    monkeypatch.setattr(agent_mod.uuid, "uuid4", lambda: _FixedUUID())
    provider = ScriptedProvider(
        responses=[
            make_response(call("todo", action="add", description="step one")),
            make_response(call("return_control_to_user", message="premature")),
            make_response(call("todo", action="complete", id="abcd1234")),
            make_response(call("return_control_to_user", message="done now")),
        ]
    )
    conv = make_conv(provider)
    state = AgentState()
    _run_turn(conv, state, max_iterations=10)
    assert state.done is True
    assert state.farewell == "done now"
    refusal_msgs = [
        m.content for m in conv.messages if m.role == "user" and "refused" in m.content
    ]
    assert len(refusal_msgs) == 1


def test_run_turn_raises_after_max_iterations():
    import click

    provider = ScriptedProvider(
        responses=[make_response(text="no tool") for _ in range(3)]
    )
    conv = make_conv(provider)
    with pytest.raises(click.ClickException, match="exhausted"):
        _run_turn(conv, AgentState(), max_iterations=3)


def test_run_turn_handles_unknown_tool_and_continues():
    provider = ScriptedProvider(
        responses=[
            make_response(call("not_a_tool", x=1)),
            make_response(call("return_control_to_user", message="ok")),
        ]
    )
    conv = make_conv(provider)
    state = AgentState()
    _run_turn(conv, state, max_iterations=5)
    assert state.done is True
    assert provider.call_count == 2


# --- _build_provider ---


def test_build_provider_missing_api_key(monkeypatch):
    import click

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(click.ClickException, match="ANTHROPIC_API_KEY"):
        _build_provider(AgentConfig(), None)


def test_build_provider_uses_default_anthropic(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    provider = _build_provider(AgentConfig(), None)
    assert isinstance(provider, AnthropicProvider)
    assert provider.model == agent_mod.DEFAULT_MODEL


def test_build_provider_cli_model_override(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.delenv("TRILOGY_AGENT_MODEL", raising=False)
    provider = _build_provider(AgentConfig(), "claude-override")
    assert provider.model == "claude-override"


def test_build_provider_env_model_override(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("TRILOGY_AGENT_MODEL", "env-model")
    provider = _build_provider(AgentConfig(model="toml-model"), None)
    assert provider.model == "env-model"


def test_build_provider_uses_toml_provider_and_model(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("TRILOGY_AGENT_MODEL", raising=False)
    cfg = AgentConfig(provider=Provider.OPENAI, model="gpt-x")
    provider = _build_provider(cfg, None)
    assert isinstance(provider, OpenAIProvider)
    assert provider.model == "gpt-x"


def test_build_provider_custom_api_key_env(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("MY_CUSTOM_KEY", "sk-test")
    cfg = AgentConfig(api_key_env="MY_CUSTOM_KEY")
    provider = _build_provider(cfg, None)
    assert provider.api_key == "sk-test"


def test_build_provider_reports_missing_custom_env(monkeypatch):
    import click

    monkeypatch.delenv("MY_CUSTOM_KEY", raising=False)
    cfg = AgentConfig(api_key_env="MY_CUSTOM_KEY")
    with pytest.raises(click.ClickException, match="MY_CUSTOM_KEY"):
        _build_provider(cfg, None)


# --- tool definitions surface check ---


def test_all_tools_registered():
    names = {t.name for t in ALL_TOOLS}
    assert names == {"show_message", "trilogy", "todo", "return_control_to_user"}
    assert set(agent_mod.TOOL_HANDLERS) == names


def test_tool_schemas_are_valid_json_schema_objects():
    for tool in ALL_TOOLS:
        assert tool.input_schema["type"] == "object"
        assert "properties" in tool.input_schema
