import json

import pytest
from click.testing import CliRunner

from trilogy.ai.enums import Provider
from trilogy.ai.models import (
    LLMMessage,
    LLMRequestOptions,
    LLMResponse,
    LLMToolCall,
    UsageDict,
)
from trilogy.ai.providers.base import LLMProvider
from trilogy.scripts import agent as agent_mod
from trilogy.scripts.agent_sessions import (
    AgentSession,
    SessionError,
    list_sessions,
    project_dir,
    project_slug,
    resolve_session,
    session_home,
)


class ScriptedProvider(LLMProvider):
    def __init__(self, responses):
        super().__init__("scripted", "test-key", "test-model", Provider.OPENAI)
        self.responses = responses
        self.histories: list[list[LLMMessage]] = []

    def generate_completion(
        self, options: LLMRequestOptions, history: list[LLMMessage]
    ) -> LLMResponse:
        self.histories.append(list(history))
        return self.responses.pop(0)


def _done(message: str) -> LLMResponse:
    return LLMResponse(
        text="",
        usage=UsageDict(prompt_tokens=0, completion_tokens=0, total_tokens=0),
        tool_calls=[
            LLMToolCall(name="return_control_to_user", arguments={"message": message})
        ],
    )


@pytest.fixture
def session_env(tmp_path, monkeypatch):
    home = tmp_path / "sessions"
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.setenv("TRILOGY_AGENT_SESSION_HOME", str(home))
    monkeypatch.chdir(work)
    return work


def _invoke(args, catch_exceptions=False):
    from trilogy.scripts.trilogy import cli

    return CliRunner().invoke(cli, ["agent", *args], catch_exceptions=catch_exceptions)


def _run(monkeypatch, responses, args):
    provider = ScriptedProvider(responses)
    monkeypatch.setattr(agent_mod, "_build_provider", lambda *a, **kw: provider)
    return _invoke(args), provider


def test_session_home_honors_env(tmp_path, monkeypatch):
    monkeypatch.setenv("TRILOGY_AGENT_SESSION_HOME", str(tmp_path / "elsewhere"))
    assert session_home() == tmp_path / "elsewhere"


def test_project_slug_is_filesystem_safe(tmp_path):
    slug = project_slug(tmp_path)
    assert "/" not in slug and "\\" not in slug and ":" not in slug


def test_start_flush_load_round_trip(session_env):
    session = AgentSession.start(
        session_env, provider="openai", model="m", toolset="trilogy", command="first"
    )
    messages = [
        LLMMessage(role="system", content="sys"),
        LLMMessage(role="user", content="first"),
        LLMMessage(role="assistant", content="", model_info={"tool_calls": [{"a": 1}]}),
    ]
    session.flush(messages)
    session.flush(messages)

    loaded, restored = AgentSession.load(session.path)
    assert [m.role for m in restored] == ["system", "user", "assistant"]
    assert restored[2].model_info == {"tool_calls": [{"a": 1}]}
    assert loaded.id == session.id
    assert loaded.flushed == 3
    assert loaded.meta.message_count == 3


def test_flush_appends_only_new_messages(session_env):
    session = AgentSession.start(
        session_env, provider="openai", model="m", toolset="trilogy", command="first"
    )
    messages = [LLMMessage(role="user", content="one")]
    session.flush(messages)
    messages.append(LLMMessage(role="assistant", content="two"))
    session.flush(messages)
    records = [
        json.loads(line)
        for line in session.path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    assert [r["type"] for r in records] == ["command", "message", "message"]


def test_resolve_session_last_and_prefix(session_env):
    first = AgentSession.start(
        session_env, provider="openai", model="m", toolset="trilogy", command="a"
    )
    second = AgentSession.start(
        session_env, provider="openai", model="m", toolset="trilogy", command="b"
    )
    second.flush([LLMMessage(role="user", content="b")])
    assert resolve_session(session_env, "last") == second.path
    assert resolve_session(session_env, first.id) == first.path
    assert resolve_session(session_env, first.id[:4]) == first.path


def test_resolve_session_errors(session_env):
    with pytest.raises(SessionError):
        resolve_session(session_env, "last")
    AgentSession.start(
        session_env, provider="openai", model="m", toolset="trilogy", command="a"
    )
    with pytest.raises(SessionError):
        resolve_session(session_env, "nosuchid")


def test_list_sessions_skips_corrupt_sidecar(session_env):
    session = AgentSession.start(
        session_env, provider="openai", model="m", toolset="trilogy", command="a"
    )
    (project_dir(session_env) / "bogus.meta.json").write_text("{", encoding="utf-8")
    metas = list_sessions(session_env)
    assert [m.id for m in metas] == [session.id]


def test_agent_run_persists_session(session_env, monkeypatch):
    result, _ = _run(monkeypatch, [_done("all set")], ["hello there"])
    assert result.exit_code == 0
    metas = list_sessions(session_env)
    assert len(metas) == 1
    assert metas[0].first_command == "hello there"
    assert metas[0].id in result.output
    _, restored = AgentSession.load(project_dir(session_env) / f"{metas[0].id}.jsonl")
    assert any(m.content == "hello there" for m in restored)


def test_agent_no_save_writes_nothing(session_env, monkeypatch):
    _run(monkeypatch, [_done("done")], ["--no-save", "hello"])
    assert list_sessions(session_env) == []


def test_agent_resume_seeds_prior_history(session_env, monkeypatch):
    _run(monkeypatch, [_done("first answer")], ["first question"])
    session_id = list_sessions(session_env)[0].id

    result, provider = _run(
        monkeypatch, [_done("second answer")], ["--resume", session_id, "follow up"]
    )
    assert result.exit_code == 0
    history = provider.histories[0]
    contents = [m.content for m in history]
    assert "first question" in contents
    assert contents[-1] == "follow up"
    assert sum(1 for m in history if m.role == "system") == 1

    metas = list_sessions(session_env)
    assert len(metas) == 1
    assert metas[0].turns == 2
    assert metas[0].last_command == "follow up"


def test_agent_resume_last_uses_latest(session_env, monkeypatch):
    _run(monkeypatch, [_done("a")], ["first"])
    result, provider = _run(monkeypatch, [_done("b")], ["--resume", "last", "second"])
    assert result.exit_code == 0
    assert "first" in [m.content for m in provider.histories[0]]
    assert len(list_sessions(session_env)) == 1


def test_agent_resume_appends_without_duplicating(session_env, monkeypatch):
    _run(monkeypatch, [_done("a")], ["first"])
    session_id = list_sessions(session_env)[0].id
    _run(monkeypatch, [_done("b")], ["--resume", session_id, "second"])
    _, restored = AgentSession.load(project_dir(session_env) / f"{session_id}.jsonl")
    assert [m.content for m in restored].count("first") == 1
    assert sum(1 for m in restored if m.role == "system") == 1


def test_agent_resume_unknown_id_errors(session_env, monkeypatch):
    provider = ScriptedProvider([])
    monkeypatch.setattr(agent_mod, "_build_provider", lambda *a, **kw: provider)
    result = _invoke(["--resume", "deadbeef", "q"], catch_exceptions=True)
    assert result.exit_code != 0
    assert "No agent session matching" in result.output


def test_agent_list_sessions_and_missing_command(session_env, monkeypatch):
    _run(monkeypatch, [_done("a")], ["a question"])
    result = _invoke(["--list-sessions"])
    assert result.exit_code == 0
    assert "a question" in result.output

    missing = _invoke([], catch_exceptions=True)
    assert missing.exit_code != 0
    assert "COMMAND" in missing.output
