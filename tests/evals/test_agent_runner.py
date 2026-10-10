from __future__ import annotations

import subprocess
from unittest.mock import Mock

from evals.common import agent_runner


def test_is_provider_crash_requires_nonzero_exit_and_provider_traceback():
    assert agent_runner.is_provider_crash(
        {"exit_code": 1, "output": "ProviderError: DeepSeek API error"}
    )
    assert not agent_runner.is_provider_crash(
        {"exit_code": 0, "output": "ProviderError: recovered"}
    )
    assert not agent_runner.is_provider_crash(
        {"exit_code": 1, "output": "ValueError: bad query"}
    )


def test_kill_process_tree_uses_taskkill_on_windows(monkeypatch):
    proc = Mock(spec=subprocess.Popen)
    proc.pid = 123
    proc.poll.return_value = None
    run = Mock(return_value=Mock(returncode=0))
    monkeypatch.setattr(agent_runner.sys, "platform", "win32")
    monkeypatch.setattr(agent_runner.subprocess, "run", run)

    agent_runner._kill_process_tree(proc)

    run.assert_called_once_with(
        ["taskkill", "/PID", "123", "/T", "/F"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    proc.kill.assert_not_called()


def test_kill_process_tree_falls_back_when_taskkill_fails(monkeypatch):
    proc = Mock(spec=subprocess.Popen)
    proc.pid = 123
    proc.poll.return_value = None
    monkeypatch.setattr(agent_runner.sys, "platform", "win32")
    monkeypatch.setattr(
        agent_runner.subprocess, "run", Mock(return_value=Mock(returncode=1))
    )

    agent_runner._kill_process_tree(proc)

    proc.kill.assert_called_once_with()


def _seeded_workspace(tmp_path):
    src = tmp_path / "workspace"
    (src / "raw").mkdir(parents=True)
    (src / "raw" / "store_sales.preql").write_text("key x int;")
    (src / "warehouse.duckdb").write_bytes(b"db")
    (src / "trilogy.toml").write_text("[engine]")
    (src / "schema.md").write_text("# schema")
    return src


def test_reset_worker_workspace_drops_a_previous_agents_files(tmp_path):
    src = _seeded_workspace(tmp_path)
    worker = agent_runner.prepare_worker_workspace(src, 0, "warehouse.duckdb")
    (worker / "probe_rowset.preql").write_text("select 1 -> x;")
    (worker / "scratch").mkdir()
    (worker / "raw" / "probe.preql").write_text("select 1 -> x;")
    (worker / "raw" / "store_sales.preql").write_text("edited")
    (worker / "warehouse.duckdb").write_bytes(b"db+writes")

    agent_runner.reset_worker_workspace(src, worker, "warehouse.duckdb")

    assert sorted(p.name for p in worker.iterdir()) == [
        "raw",
        "schema.md",
        "trilogy.toml",
        "warehouse.duckdb",
    ]
    assert [p.name for p in (worker / "raw").iterdir()] == ["store_sales.preql"]
    assert (worker / "raw" / "store_sales.preql").read_text() == "key x int;"
    assert (worker / "warehouse.duckdb").read_bytes() == b"db+writes"
