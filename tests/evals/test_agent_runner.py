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
