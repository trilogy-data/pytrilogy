"""Tests for session-scoped explore dedup (``explore_seen``)."""

import json
import tempfile
from pathlib import Path
from textwrap import dedent

import pytest
from click.testing import CliRunner

from trilogy.scripts import explore_seen
from trilogy.scripts.trilogy import cli

MODEL = dedent("""
        key id int;
        property id.carrier string; # operating carrier
        property id.distance int;

        datasource flights (
            id,
            carrier,
            distance
        )
        grain(id)
        query '''select 1 as id, 'AA' as carrier, 100 as distance''';
        """).strip() + "\n"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def model(tmp_path: Path) -> Path:
    path = tmp_path / "flight.preql"
    path.write_text(MODEL)
    return path


@pytest.fixture
def session_env(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path / "seen"))
    monkeypatch.setenv("TRILOGY_OUTPUT_FORMAT", "json")
    monkeypatch.delenv("TRILOGY_EXPLORE_RECORD_LIMIT", raising=False)

    def activate(session: str) -> None:
        monkeypatch.setenv("TRILOGY_EXPLORE_SESSION", session)

    return activate


def explore_payload(runner: CliRunner, path: Path, *args: str) -> dict:
    result = runner.invoke(cli, ["explore", str(path), *args])
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def test_no_session_never_stubs(runner, model, session_env, monkeypatch):
    monkeypatch.delenv("TRILOGY_EXPLORE_SESSION", raising=False)
    first = explore_payload(runner, model)
    second = explore_payload(runner, model)
    assert first == second
    assert "already_shown" not in json.dumps(second)


def test_repeat_explore_collapses_to_stub(runner, model, session_env):
    session_env("s1")
    first = explore_payload(runner, model)
    assert "carrier" in json.dumps(first)
    second = explore_payload(runner, model)
    assert second["namespaces"][""] == {"already_shown": str(model)}
    assert "reshow" in second["already_shown_note"]
    assert "carrier" not in json.dumps(second["namespaces"])


def test_reshow_reprints_in_full(runner, model, session_env):
    session_env("s2")
    first = explore_payload(runner, model)
    explore_payload(runner, model)
    reshown = explore_payload(runner, model, "--reshow")
    assert reshown == first


def test_modified_file_prints_in_full(runner, model, session_env):
    session_env("s3")
    explore_payload(runner, model)
    model.write_text(MODEL + "property id.origin string;\n")
    second = explore_payload(runner, model)
    assert "origin" in json.dumps(second["namespaces"])
    assert "already_shown" not in json.dumps(second["namespaces"])


def test_regex_filtered_output_not_stubbed(runner, model, session_env):
    """A filtered rendering differs from the full one, so neither suppresses
    the other."""
    session_env("s4")
    explore_payload(runner, model)
    filtered = explore_payload(runner, model, "--regex", "carrier")
    assert "carrier" in json.dumps(filtered)
    full = explore_payload(runner, model)
    assert full["namespaces"][""] == {"already_shown": str(model)}


def test_oversized_payload_never_recorded(runner, model, session_env, monkeypatch):
    """A payload bigger than the wrapper's truncation cap was never fully seen
    by the agent, so it must not suppress a later explore."""
    session_env("s5")
    monkeypatch.setenv("TRILOGY_EXPLORE_RECORD_LIMIT", "10")
    explore_payload(runner, model)
    second = explore_payload(runner, model)
    assert "already_shown" not in json.dumps(second)


def test_store_survives_and_names_first_file(runner, model, tmp_path, session_env):
    session_env("s6")
    explore_payload(runner, model)
    twin = tmp_path / "flight_twin.preql"
    twin.write_text(MODEL)
    stubbed = explore_payload(runner, twin)
    assert stubbed["namespaces"][""] == {"already_shown": str(model)}


def test_unreadable_store_disables_dedup_without_breaking(monkeypatch, tmp_path):
    """A corrupt seen-store reads as empty: dedup silently restarts rather
    than failing the explore."""
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    store = explore_seen._store_path("s7")
    store.parent.mkdir(parents=True)
    store.write_text("not json", encoding="utf-8")
    payload = {"namespaces": {"": [{"keys": ["id int;"]}]}}
    out = explore_seen.apply_seen_dedup(payload, "x.preql", "s7")
    assert out["namespaces"] == payload["namespaces"]
