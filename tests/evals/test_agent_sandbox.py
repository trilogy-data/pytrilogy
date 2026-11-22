from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common import agent_runner
from tpcds_agent.spec import SPEC

from trilogy.dialect.config import DuckDBConfig
from trilogy.execution.config import load_config_file


def test_agent_env_keeps_only_its_provider_key(monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "ds")
    monkeypatch.setenv("OPENAI_API_KEY", "oa")
    monkeypatch.setenv("TRILOGY_CLICKHOUSE_PASSWORD", "pw")
    env = agent_runner.agent_env("deepseek")
    assert env["DEEPSEEK_API_KEY"] == "ds"
    assert "OPENAI_API_KEY" not in env
    assert "TRILOGY_CLICKHOUSE_PASSWORD" not in env
    assert "PATH" in {k.upper() for k in env}


def test_eval_workspace_is_sandboxed(tmp_path):
    agent_runner.write_trilogy_toml(tmp_path, SPEC, "deepseek", "m", 10)
    config = load_config_file(tmp_path / "trilogy.toml")
    assert isinstance(config.engine_config, DuckDBConfig)
    assert Path(config.engine_config.sandbox_root) == tmp_path.resolve()
    assert config.agent.allow_script_write is False
