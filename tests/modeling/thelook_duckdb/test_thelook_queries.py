"""Partial-bridge regression battery over a thelook-style model.

order_items binds BOTH ~user.id and ~product.id with no complete sibling —
the shape that makes unpinned user x product spans unsafe. Queries cover the
pinned INNER-star heal, extension-family and fact-anchored controls, and the
UnconstrainedPartialBridgeException boundary.
"""

import os
import platform
from pathlib import Path

import tomli_w
import tomllib
from pytest import raises

from tests.modeling._benchmark_timing import benchmark_query
from tests.modeling._row_compare import rows_match
from tests.modeling.thelook_duckdb.query_size import query_size
from trilogy import Executor
from trilogy.core.exceptions import UnconstrainedPartialBridgeException
from trilogy.core.models.environment import Environment

machine = platform.machine()
cpu_name = platform.processor()
cpu_count = os.cpu_count()

fingerprint = (
    f"{machine}-{cpu_name}-{cpu_count}".lower().replace(" ", "_").replace(",", "")
)

working_path = Path(__file__).parent

REPEAT_TIME_CUTOFF = 0.15
REPEAT_COUNT = 3


def _load_toml_mapping(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8")
    if not raw.strip():
        return {}
    try:
        loaded = tomllib.loads(raw)
    except tomllib.TOMLDecodeError:
        return {}
    if isinstance(loaded, dict):
        return loaded
    return {}


def run_query(engine: Executor, idx: int, label: str | None = None) -> str:
    engine.environment = Environment(working_path=working_path)
    query_label = label or f"{idx:02d}"
    text = (working_path / f"query{idx:02d}.preql").read_text()
    preql_size = query_size(text, "preql")

    sql_path = working_path / f"query{idx:02d}.sql"
    rquery = sql_path.read_text()
    comp_size = query_size(rquery, "sql")

    benchmark = benchmark_query(
        generate=lambda: engine.generate_sql(text)[-1],
        execute_candidate=lambda query: list(engine.execute_raw_sql(query).fetchall()),
        execute_reference=lambda: list(engine.execute_raw_sql(rquery).fetchall()),
        repeat_time_cutoff=REPEAT_TIME_CUTOFF,
        repeat_count=REPEAT_COUNT,
    )
    query = benchmark.query
    comp_results = benchmark.candidate_result
    base_results = benchmark.reference_result

    if len(base_results) > 0:
        assert len(comp_results) > 0, "No results returned"

    assert len(base_results) == len(
        comp_results
    ), f"Row count mismatch: expected {len(base_results)}, got {len(comp_results)}"
    for qidx, row in enumerate(base_results):
        assert rows_match(
            row, comp_results[qidx]
        ), f"Row mismatch in row {qidx} (expected v actual): {row} != {comp_results[qidx]}"

    with open(
        working_path / f"zquery{query_label}.log",
        "w",
        encoding="utf-8",
        newline="\n",
    ) as f:
        f.write(
            tomli_w.dumps(
                {
                    "query_id": query_label,
                    "gen_length": query_size(query, "sql"),
                    "preql_size": preql_size,
                    "comp_size": comp_size,
                    "generated_sql": query,
                },
                multiline_strings=True,
            )
        )

    timing = Path(working_path / f"zquery_timing_{fingerprint}.log")
    current = _load_toml_mapping(timing)
    current[f"query_{query_label}"] = {
        "parse_time": benchmark.parse_time,
        "exec_time": benchmark.candidate_time,
        "comp_time": benchmark.reference_time,
    }
    final = {x: current[x] for x in sorted(current.keys())}
    temp_timing = timing.with_suffix(f"{timing.suffix}.tmp")
    temp_timing.write_text(
        tomli_w.dumps(
            final,
            multiline_strings=True,
        ),
        encoding="utf-8",
        newline="\n",
    )
    temp_timing.replace(timing)
    return query


def test_adhoc01_unpinned_span_errors(engine: Executor):
    engine.environment = Environment(working_path=working_path)
    text = (working_path / "adhoc01.preql").read_text()
    with raises(UnconstrainedPartialBridgeException) as err:
        engine.generate_sql(text)
    exc = err.value
    assert any("order_items" in ds for ds in exc.datasources), exc.datasources
    assert "user.id is not null" in exc.suggestion, exc.suggestion
    assert "product.id is not null" in exc.suggestion, exc.suggestion
    assert exc.suggestion in exc.message


def test_one(engine):
    query = run_query(engine, 1)
    assert "FULL" not in query, query
    for table in ('"users" as', '"products" as', '"order_items" as'):
        assert query.count(table) == 1, query


def test_two(engine):
    query = run_query(engine, 2)
    assert "FULL" not in query, query


def test_three(engine):
    query = run_query(engine, 3)
    assert "FULL" not in query, query


def test_four(engine):
    query = run_query(engine, 4)
    assert "FULL" not in query, query


def test_five(engine):
    query = run_query(engine, 5)
    assert "FULL" not in query, query


def test_six(engine):
    run_query(engine, 6)


def test_seven(engine):
    run_query(engine, 7)


def test_eight(engine):
    run_query(engine, 8)


def test_nine(engine):
    run_query(engine, 9)


def test_ten(engine):
    query = run_query(engine, 10)
    assert "FULL" not in query, query


def test_eleven(engine):
    query = run_query(engine, 11)
    assert '"order_items"' not in query, query


def test_twelve(engine):
    query = run_query(engine, 12)
    assert '"order_items"' not in query, query


def test_thirteen(engine):
    query = run_query(engine, 13)
    assert "FULL" not in query, query


def test_adhoc02_agg_does_not_anchor_span(engine: Executor):
    engine.environment = Environment(working_path=working_path)
    text = (working_path / "adhoc02.preql").read_text()
    with raises(UnconstrainedPartialBridgeException) as err:
        engine.generate_sql(text)
    exc = err.value
    for name in ("order_items", "user_product_sales"):
        assert any(name in ds for ds in exc.datasources), exc.datasources


def test_fourteen(engine):
    query = run_query(engine, 14)
    assert '"daily_sales"' in query, query
    assert '"order_items"' not in query, query
    assert '"orders"' not in query, query


def test_fifteen(engine):
    query = run_query(engine, 15)
    assert '"user_product_sales"' in query, query
    assert '"order_items"' not in query, query
    assert "JOIN" not in query, query


def test_sixteen(engine):
    query = run_query(engine, 16)
    assert "FULL" not in query, query


def test_seventeen(engine):
    query = run_query(engine, 17)
    assert "FULL" not in query, query
    assert '"user_product_sales"' in query, query
    assert '"order_items"' not in query, query
    for alias in (
        '"user_id"',
        '"product_id"',
        '"user_state"',
        '"product_brand"',
        '"revenue"',
        '"sale_line_count"',
    ):
        assert f" as {alias}" in query, query


def test_eighteen(engine):
    query = run_query(engine, 18)
    assert '"user_product_sales"' in query, query
    assert '"order_items"' not in query, query
