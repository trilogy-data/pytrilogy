from pathlib import Path

from trilogy import Dialects
from trilogy.dialect.config import DuckDBConfig
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.refresh import execute_script_for_refresh

PREQL_PATH = Path(__file__).parent / "union_refresh_case.preql"


def _make_executor():
    return Dialects.DUCK_DB.default_executor(
        working_path=Path(__file__).parent,
        conf=DuckDBConfig(enable_python_datasources=True),
    )


def test_a_full_refreshed_from_a_raw():
    """a_full_data must have exactly 10 rows, all with source='a'."""
    executor = _make_executor()
    execute_script_for_refresh(executor, ScriptNode(path=PREQL_PATH), quiet=True)

    count = executor.execute_raw_sql("SELECT count(*) FROM a_full_data").fetchone()[0]
    assert count == 10, f"a_full_data: expected 10 rows, got {count}"

    sources = {
        r[0]
        for r in executor.execute_raw_sql(
            "SELECT DISTINCT source FROM a_full_data"
        ).fetchall()
    }
    assert sources == {"a"}, f"a_full_data sources: expected {{'a'}}, got {sources}"


def test_b_full_refreshed_from_b_raw():
    """b_full_data must have exactly 10 rows, all with source='b'."""
    executor = _make_executor()
    execute_script_for_refresh(executor, ScriptNode(path=PREQL_PATH), quiet=True)

    count = executor.execute_raw_sql("SELECT count(*) FROM b_full_data").fetchone()[0]
    assert count == 10, f"b_full_data: expected 10 rows, got {count}"

    sources = {
        r[0]
        for r in executor.execute_raw_sql(
            "SELECT DISTINCT source FROM b_full_data"
        ).fetchall()
    }
    assert sources == {"b"}, f"b_full_data sources: expected {{'b'}}, got {sources}"


def test_union_full_refreshed_from_partials():
    """union_data must combine a_full and b_full for 20 total rows."""
    executor = _make_executor()
    stats = execute_script_for_refresh(
        executor, ScriptNode(path=PREQL_PATH), quiet=True
    )

    count = executor.execute_raw_sql("SELECT count(*) FROM union_data").fetchone()[0]
    assert count == 20, f"union_data: expected 20 rows, got {count}"
    assert any("union_data" in q.sql for q in stats.refresh_queries)
    for x in stats.refresh_queries:
        print(x.sql)
    assert not any(
        "raw" in q.sql and "union_data" in q.sql for q in stats.refresh_queries
    ), [x for x in stats.refresh_queries if "raw" in x.sql and "union_data" in x.sql]


def test_union_refresh_produces_twenty_rows():
    executor = _make_executor()
    node = ScriptNode(path=PREQL_PATH)

    stats = execute_script_for_refresh(executor, node, quiet=True)

    assert stats.update_count > 0

    results = executor.execute_text("select count(id) as results;")[-1].fetchone()

    assert results.results == 20, results
