from pathlib import Path

from sqlalchemy import text

from trilogy import Dialects
from trilogy.dialect.config import DuckDBConfig
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.refresh import execute_script_for_refresh

PREQL_PATH = Path(__file__).parent / "union_refresh_case.preql"


def test_union_refresh_produces_twenty_rows():
    executor = Dialects.DUCK_DB.default_executor(
        working_path=Path(__file__).parent,
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    node = ScriptNode(path=PREQL_PATH)

    stats = execute_script_for_refresh(executor, node, quiet=True)

    assert stats.update_count > 0

    results = executor.execute_text("select count(id) as results;")[-1].fetchone()

    assert results == 20, results
