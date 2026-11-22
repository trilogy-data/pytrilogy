from pathlib import Path

import pytest
from sqlalchemy.exc import DBAPIError

from trilogy import Dialects
from trilogy.dialect.config import DuckDBConfig
from trilogy.execution.config import load_config_file


def _executor(root: Path):
    return Dialects.DUCK_DB.default_executor(
        conf=DuckDBConfig(path=str(root / "w.duckdb"), sandbox_root=str(root))
    )


def test_sandbox_confines_file_access(tmp_path):
    root = tmp_path / "ws"
    root.mkdir()
    (root / "in.csv").write_text("a\n1\n", encoding="utf-8")
    (tmp_path / "secret.txt").write_text("hush", encoding="utf-8")
    exec = _executor(root)
    inside = (root / "in.csv").as_posix()
    assert exec.execute_raw_sql(f"select a from read_csv('{inside}')").fetchall() == [
        (1,)
    ]
    for sql, error in [
        (
            f"select * from read_text('{(tmp_path / 'secret.txt').as_posix()}')",
            "Permission",
        ),
        (f"select * from read_text('{root.as_posix()}/../secret.txt')", "Permission"),
        ("SET enable_external_access = true", "Cannot change configuration"),
        ("SET allowed_directories = ['/']", "Cannot change configuration"),
    ]:
        with pytest.raises(DBAPIError, match=error):
            exec.execute_raw_sql(sql)
    exec.close()


def test_sandbox_key_resolves_to_toml_directory(tmp_path):
    (tmp_path / "trilogy.toml").write_text(
        '[engine]\ndialect = "duck_db"\n\n[engine.config]\n'
        'db_location = "w.duckdb"\nsandbox = true\n',
        encoding="utf-8",
    )
    config = load_config_file(tmp_path / "trilogy.toml")
    assert isinstance(config.engine_config, DuckDBConfig)
    assert Path(config.engine_config.sandbox_root) == tmp_path.resolve()
