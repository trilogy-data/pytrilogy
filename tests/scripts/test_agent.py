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
from trilogy.scripts import agent_tools as agent_tools_mod
from trilogy.scripts import display_core
from trilogy.scripts.agent import (
    MAX_SUBMIT_KICKBACKS,
    _build_provider,
    _dispatch,
    _format_call,
    _maybe_flag_loop,
    _render_reviewer_transcript,
    _run_turn,
    _status_message,
)
from trilogy.scripts.agent_tools import (
    ALL_TOOLS,
    AgentState,
    TodoItem,
    handle_list_files,
    handle_read_file,
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


def test_todo_add_accepts_list_of_descriptions():
    state = AgentState()
    result = handle_todo(
        state, {"action": "add", "description": ["step one", "step two", "step three"]}
    )
    assert "todo added" in result
    assert len(state.todos) == 3
    assert [t.description for t in state.todos] == [
        "step one",
        "step two",
        "step three",
    ]


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


def test_todo_complete_without_id_lists_existing_ids():
    state = AgentState(todos=[TodoItem(id="abc123", description="x")])
    result = handle_todo(state, {"action": "complete"})
    assert "'id' is required" in result
    assert "abc123" in result


def test_todo_complete_accepts_list_of_ids():
    state = AgentState(
        todos=[TodoItem(id="aa", description="x"), TodoItem(id="bb", description="y")]
    )
    result = handle_todo(state, {"action": "complete", "id": ["aa", "bb"]})
    assert "todo completed: aa, bb" in result
    assert all(t.completed for t in state.todos)


def test_todo_complete_list_reports_missing_ids():
    state = AgentState(todos=[TodoItem(id="aa", description="x")])
    result = handle_todo(state, {"action": "complete", "id": ["aa", "zz"]})
    assert "todo completed: aa" in result
    assert "no item with id: zz" in result


# --- light hardening: loop flagging + raw/ write guidance ---


def test_maybe_flag_loop_escalates_after_repeats():
    state = AgentState()
    call = LLMToolCall(name="show_message", arguments={"message": "x"})
    assert "[guidance]" not in _maybe_flag_loop(state, call, "ok")
    assert "[guidance]" not in _maybe_flag_loop(state, call, "ok")
    flagged = _maybe_flag_loop(state, call, "ok")
    assert "[guidance]" in flagged and "3 times" in flagged


def test_maybe_flag_loop_resets_on_different_call():
    state = AgentState()
    a = LLMToolCall(name="todo", arguments={"action": "list"})
    b = LLMToolCall(name="todo", arguments={"action": "add", "description": "x"})
    for _ in range(3):
        _maybe_flag_loop(state, a, "ok")
    assert "[guidance]" not in _maybe_flag_loop(state, b, "ok")


def test_read_file_returns_content_and_reports_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "q.preql").write_text("select 1 -> x;", encoding="utf-8")
    assert handle_read_file(AgentState(), {"path": "q.preql"}) == "select 1 -> x;"
    assert "no such file" in handle_read_file(AgentState(), {"path": "missing.preql"})


def test_list_files_recursive_shows_nested_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "store_sales.preql").write_text("x", encoding="utf-8")
    (tmp_path / "trilogy.toml").write_text("y", encoding="utf-8")
    out = handle_list_files(AgentState(), {"path": "."})
    assert "trilogy.toml" in out
    assert "raw/store_sales.preql" in out


def test_list_files_skips_workspace_noise(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "tpcds.duckdb").write_text("bin", encoding="utf-8")
    (tmp_path / "_worker_0").mkdir()
    (tmp_path / "_worker_0" / "junk.preql").write_text("z", encoding="utf-8")
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "x.pyc").write_text("z", encoding="utf-8")
    (tmp_path / "query01.preql").write_text("q", encoding="utf-8")
    out = handle_list_files(AgentState(), {"path": "."})
    assert "query01.preql" in out
    assert "tpcds.duckdb" not in out
    assert "_worker_0" not in out
    assert "__pycache__" not in out


def test_list_files_reports_missing_and_non_directory(tmp_path):
    assert "no such path" in handle_list_files(
        AgentState(), {"path": str(tmp_path / "missing")}
    )
    f = tmp_path / "f.txt"
    f.write_text("x", encoding="utf-8")
    assert "not a directory" in handle_list_files(AgentState(), {"path": str(f)})


def test_list_files_validates_args():
    assert "non-empty string" in handle_list_files(AgentState(), {"path": ""})
    assert "must be a boolean" in handle_list_files(
        AgentState(), {"path": ".", "recursive": "yes"}
    )


def test_read_file_validates_path():
    assert "non-empty string" in handle_read_file(AgentState(), {})
    assert "non-empty string" in handle_read_file(AgentState(), {"path": ""})


def test_return_control_rejects_non_string_message():
    state = AgentState()
    result = handle_return_control(state, {"message": 123})
    assert "must be a string" in result
    assert state.done is False


def test_handle_trilogy_file_write_hints_on_split_content(tmp_path):
    # The CLI `file write` is allowed (it's a real, useful command). What we
    # intercept is the common misuse where the agent split the file body
    # across multiple positional args instead of one --content string.
    target = tmp_path / "q.preql"
    bad = handle_trilogy(
        AgentState(),
        {
            "args": [
                "file",
                "write",
                str(target),
                "--content",
                "import",
                "raw.store_sales",
                "as",
                "store_sales;",
            ]
        },
    )
    assert "SINGLE string" in bad
    # And valid usage isn't intercepted — falls through to the subprocess.
    ok = handle_trilogy(
        AgentState(),
        {
            "args": [
                "file",
                "write",
                str(target),
                "--content",
                "select 1+1 -> answer;",
            ]
        },
    )
    assert "exit_code: 0" in ok


# --- return_control_to_user gating ---


def test_return_control_auto_discards_open_todos():
    # Earlier behavior refused exit with uncompleted TODOs; the eval showed
    # agents burning iterations on complete/remove just to satisfy the gate.
    # Open items are now auto-discarded so exit is always cheap.
    state = AgentState(todos=[TodoItem(id="a", description="x")])
    result = handle_return_control(state, {"message": "done"})
    assert result == "return_control_to_user: ok"
    assert state.done is True
    assert state.todos == []
    assert state.farewell == "done"


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


def test_trilogy_tool_uses_utf8_subprocess_pipe(monkeypatch):
    import subprocess

    captured: dict = {}

    class _Fake:
        returncode = 0
        stdout = "ok"
        stderr = ""

    def fake_run(cmd, **kwargs):
        captured.update(kwargs)
        captured["cmd"] = cmd
        return _Fake()

    monkeypatch.setattr(subprocess, "run", fake_run)
    handle_trilogy(AgentState(), {"args": ["--version"]})
    assert captured["encoding"] == "utf-8"
    assert captured["errors"] == "replace"
    assert captured["env"]["PYTHONIOENCODING"] == "utf-8"


def test_trilogy_tool_survives_non_ascii_output(monkeypatch, tmp_path):
    import subprocess
    import sys

    real_run = subprocess.run
    script = tmp_path / "emit.py"
    script.write_text(
        'import sys; sys.stdout.write("hello \\u2014 caf\\xe9 \\u2713\\n")',
        encoding="utf-8",
    )

    def fake_run(cmd, **kwargs):
        return real_run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            encoding=kwargs.get("encoding"),
            errors=kwargs.get("errors"),
            env=kwargs.get("env"),
            timeout=30,
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = handle_trilogy(AgentState(), {"args": ["ignored"]})
    assert "exit_code: 0" in result
    assert "café" in result and "✓" in result


# --- _dispatch routing ---


def test_dispatch_unknown_tool():
    result = _dispatch(AgentState(), LLMToolCall(name="bogus", arguments={}))
    assert "Unknown tool" in result


def test_dispatch_catches_handler_exceptions(monkeypatch):
    def boom(state, args):
        raise RuntimeError("kaboom")

    monkeypatch.setitem(agent_tools_mod.TOOL_HANDLERS, "show_message", boom)
    result = _dispatch(AgentState(), LLMToolCall(name="show_message"))
    assert "RuntimeError" in result


# --- Additional coverage for handlers and helpers ---


def test_list_files_truncates_at_max_entries(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(agent_tools_mod, "_LIST_FILES_MAX_ENTRIES", 3)
    for i in range(10):
        (tmp_path / f"f{i:02d}.preql").write_text("x", encoding="utf-8")
    out = handle_list_files(AgentState(), {"path": "."})
    # Header carries the `+` truncation marker.
    assert "+" in out.splitlines()[0]


def test_list_files_non_recursive_marks_directories(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "raw").mkdir()
    (tmp_path / "__pycache__").mkdir()  # exercises the skip-continue branch
    (tmp_path / "q.preql").write_text("x", encoding="utf-8")
    out = handle_list_files(AgentState(), {"path": ".", "recursive": False})
    assert "raw/" in out
    assert "q.preql" in out
    assert "__pycache__" not in out


def test_list_files_reports_empty_directory(tmp_path):
    out = handle_list_files(AgentState(), {"path": str(tmp_path)})
    assert "no files under" in out


def test_read_file_reports_oserror(monkeypatch, tmp_path):
    target = tmp_path / "q.preql"
    target.write_text("x", encoding="utf-8")
    from pathlib import Path

    def boom(self, *a, **kw):
        raise OSError("permission denied")

    monkeypatch.setattr(Path, "read_text", boom)
    result = handle_read_file(AgentState(), {"path": str(target)})
    assert "read_file error" in result
    assert "permission denied" in result


def test_first_non_flag_arg_skips_value_flag_and_options():
    fn = agent_tools_mod._first_non_flag_arg
    assert fn(["--debug-file", "log.txt", "agent-info"]) == "agent-info"
    assert fn(["--debug", "agent-info"]) == "agent-info"
    assert fn(["--debug-file"]) is None
    assert fn([]) is None


def test_trilogy_file_write_hint_returns_none_without_content_flag():
    assert (
        agent_tools_mod._trilogy_file_write_hint(["file", "write", "x.preql"]) is None
    )


def test_handle_trilogy_reports_subprocess_timeout(monkeypatch):
    import subprocess

    def boom(*a, **kw):
        raise subprocess.TimeoutExpired(cmd="trilogy", timeout=600)

    monkeypatch.setattr(subprocess, "run", boom)
    result = handle_trilogy(AgentState(), {"args": ["--version"]})
    assert "timed out" in result


def test_handle_trilogy_preserves_full_output_for_agent_info(monkeypatch):
    """agent-info is the language reference — middle-truncating it eats the
    syntax rules. The handler skips truncate_middle on the agent-info path."""
    import subprocess

    big = "x" * 5000

    class _Fake:
        returncode = 0
        stdout = big
        stderr = ""

    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: _Fake())
    state = AgentState(tool_output_limit=80)
    result = handle_trilogy(state, {"args": ["agent-info"]})
    assert big in result
    assert "truncated" not in result


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


def test_run_turn_return_control_drops_open_todos(monkeypatch):
    class _FixedUUID:
        hex = "abcd1234ffff"

    monkeypatch.setattr(agent_tools_mod.uuid, "uuid4", lambda: _FixedUUID())
    # Open TODOs no longer gate exit. The agent adds one, then returns
    # control immediately; the open item is discarded and the run ends.
    provider = ScriptedProvider(
        responses=[
            make_response(call("todo", action="add", description="step one")),
            make_response(call("return_control_to_user", message="done")),
        ]
    )
    conv = make_conv(provider)
    state = AgentState()
    _run_turn(conv, state, max_iterations=10)
    assert state.done is True
    assert state.farewell == "done"
    assert state.todos == []
    # No refusal message — the gate is gone.
    assert not any("refused" in m.content for m in conv.messages if m.role == "user")


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


def test_run_turn_surfaces_malformed_tool_call_to_model():
    bad = LLMToolCall(name="todo", parse_error="invalid tool arguments: boom")
    provider = ScriptedProvider(
        responses=[
            make_response(bad),
            make_response(call("return_control_to_user", message="ok")),
        ]
    )
    conv = make_conv(provider)
    state = AgentState()
    _run_turn(conv, state, max_iterations=5)
    assert state.done is True
    assert any(
        "Re-issue the call with valid JSON" in m.content
        for m in conv.messages
        if m.role == "user"
    )


# --- reviewer validation on submit ---


def test_run_turn_reviewer_kicks_back_then_accepts():
    """Two `return_control_to_user` calls: reviewer says NOT_DONE on the
    first, DONE on the second. The agent should loop once then exit."""
    agent_provider = ScriptedProvider(
        responses=[
            make_response(call("return_control_to_user", message="first")),
            make_response(call("return_control_to_user", message="second")),
        ]
    )
    reviewer_responses = iter(
        [
            LLMResponse(
                text="NOT_DONE - you skipped the min cost filter",
                usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
            ),
            LLMResponse(
                text="DONE - looks right",
                usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
            ),
        ]
    )

    class ReviewerProvider(LLMProvider):
        def __init__(self):
            super().__init__("rev", "k", "m", Provider.OPENAI)

        def generate_completion(self, options, history):
            return next(reviewer_responses)

    conv = make_conv(agent_provider)
    state = AgentState()
    _run_turn(
        conv,
        state,
        max_iterations=5,
        provider=ReviewerProvider(),
        original_task="do the thing",
    )
    assert state.done is True
    assert state.submit_kickbacks == 1
    assert state.farewell == "second"
    assert any(
        "reviewer determined" in m.content for m in conv.messages if m.role == "user"
    )


def test_run_turn_reviewer_kickback_capped(monkeypatch):
    """If the reviewer keeps saying NOT_DONE, the agent exits anyway after
    MAX_SUBMIT_KICKBACKS kickbacks rather than looping forever."""
    submit_responses = [
        make_response(call("return_control_to_user", message=f"try {i}"))
        for i in range(MAX_SUBMIT_KICKBACKS + 2)
    ]
    agent_provider = ScriptedProvider(responses=submit_responses)

    class AlwaysNotDoneReviewer(LLMProvider):
        def __init__(self):
            super().__init__("rev", "k", "m", Provider.OPENAI)

        def generate_completion(self, options, history):
            return LLMResponse(
                text="NOT_DONE - never satisfied",
                usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
            )

    conv = make_conv(agent_provider)
    state = AgentState()
    _run_turn(
        conv,
        state,
        max_iterations=10,
        provider=AlwaysNotDoneReviewer(),
        original_task="do the thing",
    )
    assert state.done is True
    assert state.submit_kickbacks == MAX_SUBMIT_KICKBACKS


def test_run_turn_reviewer_skipped_when_no_provider():
    """Backwards compat: no provider passed → no reviewer call, immediate exit."""
    provider = ScriptedProvider(
        responses=[make_response(call("return_control_to_user", message="ok"))]
    )
    conv = make_conv(provider)
    state = AgentState()
    _run_turn(conv, state, max_iterations=5)
    assert state.done is True
    assert state.submit_kickbacks == 0


def test_reviewer_transcript_includes_tool_calls_and_results():
    msgs = [
        LLMMessage(role="system", content="ignored"),
        LLMMessage(role="user", content="do the thing"),
        LLMMessage(role="assistant", content=""),
        LLMMessage(role="user", content='{"tool":"trilogy","result":"ok"}'),
    ]
    msgs[2].model_info = {
        "tool_calls": [{"name": "trilogy", "arguments": {"args": ["run"]}}]
    }
    rendered = _render_reviewer_transcript(msgs)
    assert "do the thing" in rendered
    assert "AGENT CALL trilogy" in rendered
    assert "ignored" not in rendered  # system messages dropped


def test_reviewer_transcript_truncates_long_message_from_middle():
    """Long tool-result content keeps head + tail; the middle is collapsed."""
    head = "HEAD_MARKER_"
    tail = "_TAIL_MARKER"
    body = head + "x" * (agent_mod.REVIEWER_TRANSCRIPT_MSG_LIMIT * 3) + tail
    msgs = [LLMMessage(role="user", content=body)]
    rendered = _render_reviewer_transcript(msgs)
    assert head in rendered
    assert tail in rendered
    assert "truncated" in rendered
    assert len(rendered) < len(body)


def test_reviewer_transcript_truncates_long_tool_args_from_middle():
    """Long tool-call argument blobs keep head + tail."""
    head = "ARG_HEAD_"
    tail = "_ARG_TAIL"
    blob = head + "y" * (agent_mod.REVIEWER_TRANSCRIPT_ARG_LIMIT * 3) + tail
    msg = LLMMessage(role="assistant", content="")
    msg.model_info = {
        "tool_calls": [{"name": "trilogy", "arguments": {"content": blob}}]
    }
    rendered = _render_reviewer_transcript([msg])
    assert head in rendered
    assert tail in rendered
    assert "truncated" in rendered
    assert len(rendered) < len(blob)


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


def test_build_provider_cli_provider_override(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("TRILOGY_AGENT_MODEL", raising=False)
    provider = _build_provider(AgentConfig(), "gpt-5-chat-latest", "openai")
    assert isinstance(provider, OpenAIProvider)
    assert provider.model == "gpt-5-chat-latest"


def test_build_provider_cli_provider_override_rejects_unknown():
    import click

    with pytest.raises(click.ClickException, match="Unknown provider"):
        _build_provider(AgentConfig(), "m", "not-a-provider")


def test_build_provider_reports_missing_custom_env(monkeypatch):
    import click

    monkeypatch.delenv("MY_CUSTOM_KEY", raising=False)
    cfg = AgentConfig(api_key_env="MY_CUSTOM_KEY")
    with pytest.raises(click.ClickException, match="MY_CUSTOM_KEY"):
        _build_provider(cfg, None)


def test_build_provider_errors_when_no_model_for_non_default_provider(monkeypatch):
    """Non-default provider (OpenAI here) with no model anywhere — neither CLI,
    env, nor [agent].model — must fail with a clear ClickException."""
    import click

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("TRILOGY_AGENT_MODEL", raising=False)
    cfg = AgentConfig(provider=Provider.OPENAI, model=None)
    with pytest.raises(click.ClickException, match="No model configured"):
        _build_provider(cfg, None)


def test_read_context_files_rejects_missing_and_directory(tmp_path):
    """Context paths must exist as files; missing/dir paths surface a clear error."""
    import click

    from trilogy.scripts.agent import _read_context_files

    assert _read_context_files(()) == ""
    with pytest.raises(click.ClickException, match="does not exist"):
        _read_context_files((str(tmp_path / "nope.preql"),))
    with pytest.raises(click.ClickException, match="is a directory"):
        _read_context_files((str(tmp_path),))


def test_read_context_files_concatenates_paths(tmp_path):
    """Multiple files are wrapped in <context> blocks and joined."""
    from trilogy.scripts.agent import _read_context_files

    a = tmp_path / "a.preql"
    a.write_text("first", encoding="utf-8")
    b = tmp_path / "b.preql"
    b.write_text("second", encoding="utf-8")
    out = _read_context_files((str(a), str(b)))
    assert "<context " in out and "first" in out and "second" in out


def test_dump_conversation_renders_tool_call_model_info(tmp_path):
    """Assistant turns that carry tool_calls in model_info round-trip to the
    sidecar dump so the exact history sent to the model is auditable."""
    from trilogy.ai.conversation import Conversation
    from trilogy.scripts.agent import _dump_conversation

    provider = ScriptedProvider(responses=[])
    conv = Conversation.create(provider, model_prompt="sys")
    conv.add_message("hi", role="user")
    msg = LLMMessage(
        role="assistant",
        content="thinking",
        model_info={"tool_calls": [{"name": "todo", "arguments": {"action": "list"}}]},
    )
    conv.messages.append(msg)
    log_path = tmp_path / "session.log"
    log_path.write_text("", encoding="utf-8")
    _dump_conversation(conv, log_path)
    dump = (tmp_path / "session.conversation.txt").read_text(encoding="utf-8")
    assert "[model_info]" in dump
    assert "tool_calls" in dump


# --- tool call logging / status formatting ---


def test_format_call_short_args():
    formatted = _format_call(LLMToolCall(name="todo", arguments={"action": "add"}))
    assert formatted == "→ todo(action=add)"


def test_format_call_truncates_long_values():
    formatted = _format_call(
        LLMToolCall(name="show_message", arguments={"message": "x" * 500})
    )
    assert len(formatted) < 200
    assert "…" in formatted


def test_format_call_json_encodes_non_string_args():
    formatted = _format_call(
        LLMToolCall(name="trilogy", arguments={"args": ["run", "q.preql"]})
    )
    assert 'args=["run", "q.preql"]' in formatted


def test_status_message_for_trilogy_includes_subcommand():
    call = LLMToolCall(
        name="trilogy", arguments={"args": ["file", "write", "trilogy.toml"]}
    )
    assert _status_message(call) == "trilogy file write trilogy.toml"


def test_status_message_defaults_to_tool_name():
    assert _status_message(LLMToolCall(name="todo", arguments={"action": "list"})) == (
        "todo"
    )


def test_run_turn_logs_each_tool_call(capsys):
    from trilogy.scripts import display_core

    provider = ScriptedProvider(
        responses=[
            make_response(call("show_message", message="hi")),
            make_response(call("return_control_to_user", message="done")),
        ]
    )
    conv = make_conv(provider)
    with display_core.set_rich_mode(False):
        _run_turn(conv, AgentState(), max_iterations=5)
    out = capsys.readouterr().out
    assert "→ show_message(" in out
    assert "→ return_control_to_user(" in out


# --- --log-file ---


def test_run_turn_writes_jsonl_log(tmp_path):
    log_path = tmp_path / "trace.jsonl"
    provider = ScriptedProvider(
        responses=[
            make_response(call("show_message", message="hi")),
            make_response(call("return_control_to_user", message="done")),
        ]
    )
    conv = make_conv(provider)
    _run_turn(conv, AgentState(), max_iterations=5, log_path=log_path)

    events = [
        __import__("json").loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
    ]
    kinds = [e["type"] for e in events]
    assert kinds.count("llm_response") == 2
    assert kinds.count("tool_call") == 2
    assert kinds.count("tool_result") == 2
    first_call = next(e for e in events if e["type"] == "tool_call")
    assert first_call["name"] == "show_message"
    assert first_call["arguments"]["message"] == "hi"
    assert all("ts" in e for e in events)


def test_log_file_cli_flag_creates_and_truncates(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from trilogy.scripts.trilogy import cli

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    log_path = tmp_path / "trace.jsonl"
    log_path.write_text("stale content\n", encoding="utf-8")

    def fake_run_turn(conv, state, max_iterations, log_path=None, tools=None, **_):
        import trilogy.scripts.agent as mod

        mod._log_event(log_path, {"type": "llm_response", "tool_calls": []})
        state.done = True
        state.farewell = "done"

    monkeypatch.setattr(agent_mod, "_run_turn", fake_run_turn)

    result = CliRunner().invoke(
        cli, ["agent", "--log-file", str(log_path), "bootstrap"]
    )
    assert result.exit_code == 0, result.output
    content = log_path.read_text(encoding="utf-8")
    assert "stale content" not in content
    lines = [__import__("json").loads(line) for line in content.splitlines()]
    assert lines[0]["type"] == "session_start"
    assert lines[0]["command"] == "bootstrap"
    assert any(ln["type"] == "llm_response" for ln in lines)


# --- --env flag (CLI) ---


def test_env_flag_applies_kv_pair_via_cli(monkeypatch):
    from click.testing import CliRunner

    from trilogy.scripts.trilogy import cli

    monkeypatch.delenv("AGENT_TEST_VAR", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    seen: dict = {}

    def fake_run_turn(conv, state, max_iterations, log_path=None, tools=None, **_):
        import os

        seen["AGENT_TEST_VAR"] = os.environ.get("AGENT_TEST_VAR")
        state.done = True
        state.farewell = "done"

    monkeypatch.setattr(agent_mod, "_run_turn", fake_run_turn)

    result = CliRunner().invoke(
        cli, ["agent", "--env", "AGENT_TEST_VAR=hello", "do thing"]
    )
    assert result.exit_code == 0, result.output
    assert seen["AGENT_TEST_VAR"] == "hello"


def test_env_flag_loads_file_via_cli(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from trilogy.scripts.trilogy import cli

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("FROM_FILE_VAR", raising=False)

    env_file = tmp_path / "secrets.env"
    env_file.write_text("ANTHROPIC_API_KEY=sk-file\nFROM_FILE_VAR=xyz\n")

    seen: dict = {}

    def fake_run_turn(conv, state, max_iterations, log_path=None, tools=None, **_):
        import os

        seen["anthropic"] = os.environ.get("ANTHROPIC_API_KEY")
        seen["from_file"] = os.environ.get("FROM_FILE_VAR")
        state.done = True
        state.farewell = "done"

    monkeypatch.setattr(agent_mod, "_run_turn", fake_run_turn)

    result = CliRunner().invoke(cli, ["agent", "--env", str(env_file), "bootstrap"])
    assert result.exit_code == 0, result.output
    assert seen["anthropic"] == "sk-file"
    assert seen["from_file"] == "xyz"


def test_quiet_flag_drops_show_message_tool(monkeypatch):
    """`--quiet` drops show_message from the toolbox and switches the system
    prompt — verify both by inspecting what _run_turn receives."""
    from click.testing import CliRunner

    from trilogy.scripts.trilogy import cli

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    captured: dict = {}

    def fake_run_turn(conv, state, max_iterations, log_path=None, tools=None, **_):
        captured["tool_names"] = {t.name for t in (tools or [])}
        captured["system_prompt"] = conv.messages[0].content
        state.done = True
        state.farewell = "done"

    monkeypatch.setattr(agent_mod, "_run_turn", fake_run_turn)

    result = CliRunner().invoke(cli, ["agent", "--quiet", "do thing"])
    assert result.exit_code == 0, result.output
    assert "show_message" not in captured["tool_names"]
    assert captured["system_prompt"] == agent_mod.QUIET_SYSTEM_PROMPT


def test_interactive_followup_runs_then_exits(monkeypatch):
    """`-i` keeps the REPL going across followups until the user types `exit`."""
    from click.testing import CliRunner

    from trilogy.scripts.trilogy import cli

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    turn_count = {"n": 0}

    def fake_run_turn(conv, state, max_iterations, log_path=None, tools=None, **_):
        turn_count["n"] += 1
        state.done = True
        state.farewell = f"turn {turn_count['n']} done"

    monkeypatch.setattr(agent_mod, "_run_turn", fake_run_turn)

    # First prompt response: a follow-up message. Second: `exit`.
    result = CliRunner().invoke(
        cli, ["agent", "-i", "first task"], input="follow up\nexit\n"
    )
    assert result.exit_code == 0, result.output
    assert turn_count["n"] == 2  # initial command + one followup


def test_interactive_aborts_cleanly_on_eof(monkeypatch):
    """Ctrl-D / EOF in the REPL must return cleanly, not crash."""
    from click.testing import CliRunner

    from trilogy.scripts.trilogy import cli

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    def fake_run_turn(conv, state, max_iterations, log_path=None, tools=None, **_):
        state.done = True
        state.farewell = "done"

    monkeypatch.setattr(agent_mod, "_run_turn", fake_run_turn)

    # No input → click.prompt raises Abort on first read → loop returns.
    result = CliRunner().invoke(cli, ["agent", "-i", "task"], input="")
    assert result.exit_code == 0, result.output


def test_env_flag_invalid_value_raises_click_exception(monkeypatch):
    from click.testing import CliRunner

    from trilogy.scripts.trilogy import cli

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    result = CliRunner().invoke(
        cli, ["agent", "--env", "not_a_kv_and_not_a_file", "cmd"]
    )
    assert result.exit_code != 0
    assert "KEY=VALUE" in result.output


# --- tool definitions surface check ---


def test_all_tools_registered():
    names = {t.name for t in ALL_TOOLS}
    assert names == {
        "show_message",
        "trilogy",
        "read_file",
        "list_files",
        "todo",
        "return_control_to_user",
    }
    assert set(agent_tools_mod.TOOL_HANDLERS) == names


def test_tool_schemas_are_valid_json_schema_objects():
    for tool in ALL_TOOLS:
        assert tool.input_schema["type"] == "object"
        assert "properties" in tool.input_schema
