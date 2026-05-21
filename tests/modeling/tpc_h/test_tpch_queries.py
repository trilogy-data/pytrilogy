import os
import platform
import re
import time
from pathlib import Path
from typing import Callable, TypeVar

import tomli_w
import tomllib
from pytest import raises

from tests.modeling.tpc_h.query_size import query_size
from trilogy import Executor, parse
from trilogy.core.exceptions import InvalidSyntaxException
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

T = TypeVar("T")


def _time(fn: Callable[[], T]) -> tuple[float, T]:
    start = time.perf_counter()
    value = fn()
    return time.perf_counter() - start, value


def _load_toml_mapping(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    raw = path.read_text()
    if not raw.strip():
        return {}
    try:
        loaded = tomllib.loads(raw)
    except tomllib.TOMLDecodeError:
        return {}
    if isinstance(loaded, dict):
        return loaded
    return {}


def _execute(engine: Executor, text: str) -> list:
    return list(engine.execute_raw_sql(engine.generate_sql(text)[-1]).fetchall())


def run_query(
    engine: Executor,
    idx: int,
    sql_override: bool = False,
    preql_file: str | None = None,
    label: str | None = None,
):
    engine.environment = Environment(working_path=working_path)
    filename = preql_file or f"query{idx:02d}.preql"
    query_label = label or f"{idx:02d}"
    with open(working_path / filename) as f:
        text = f.read()
    preql_size = query_size(text, "preql")

    if sql_override:
        rquery = (working_path / f"query{idx:02d}.sql").read_text()
    else:
        rquery = f"PRAGMA tpch({idx});"

    def _exec_trilogy() -> list:
        return list(engine.execute_raw_sql(query).fetchall())

    def _exec_reference() -> list:
        return list(engine.execute_raw_sql(rquery).fetchall())

    parse_time, query = _time(lambda: engine.generate_sql(text)[-1])
    exec_time, comp_results = _time(_exec_trilogy)
    comp_time, base_results = _time(_exec_reference)

    if min(exec_time, comp_time) < REPEAT_TIME_CUTOFF:
        for _ in range(REPEAT_COUNT):
            parse_time = min(parse_time, _time(lambda: engine.generate_sql(text))[0])
            exec_time = min(exec_time, _time(_exec_trilogy)[0])
            comp_time = min(comp_time, _time(_exec_reference)[0])

    sql_path = working_path / f"query{idx:02d}.sql"
    comp_source = sql_path.read_text() if sql_path.exists() else rquery
    comp_size = query_size(comp_source, "sql")

    if len(base_results) > 0:
        assert len(comp_results) > 0, "No results returned"

    if len(base_results) != len(comp_results):
        assert (
            False
        ), f"Row count mismatch: expected {len(base_results)}, got {len(comp_results)}"
    for qidx, row in enumerate(base_results):
        assert (
            row == comp_results[qidx]
        ), f"Row mismatch in row {qidx} (expected v actual): {row} != {comp_results[qidx]}"

    with open(working_path / f"zquery{query_label}.log", "w") as f:
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
        "parse_time": parse_time,
        "exec_time": exec_time,
        "comp_time": comp_time,
    }
    final = {x: current[x] for x in sorted(current.keys())}
    temp_timing = timing.with_suffix(f"{timing.suffix}.tmp")
    temp_timing.write_text(
        tomli_w.dumps(
            final,
            multiline_strings=True,
        )
    )
    temp_timing.replace(timing)
    return query


def test_adhoc02_error():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc02.preql") as f:
        text = f.read()
        with raises(InvalidSyntaxException):
            env, queries = parse(text, env)


def test_adhoc03(engine: Executor):
    engine.environment = Environment(working_path=working_path)
    with open(working_path / "adhoc03.preql") as f:
        text = f.read()
    results = _execute(engine, text)
    # dbgen sf=0.1 dataset: top-10-customer order rollup
    assert results[0][0] == 333
    assert results[0][1] == 333


def test_adhoc04(engine: Executor):
    engine.environment = Environment(working_path=working_path)
    with open(working_path / "adhoc04.preql") as f:
        text = f.read()
    query = engine.generate_sql(text)[0]
    # really, this is checking that there is no extra inner join with the filtered quantity
    assert len(query) < 1400, query


def test_adhoc05(engine: Executor):
    engine.environment = Environment(working_path=working_path)
    with open(working_path / "adhoc05.preql") as f:
        text = f.read()
    results = _execute(engine, text)
    assert results[0][1] == 150000, results[0]


def test_adhoc06(engine: Executor):
    engine.environment = Environment(working_path=working_path)
    with open(working_path / "adhoc06.preql") as f:
        text = f.read()
    with open(working_path / "cache_warm.preql") as f:
        cache_sql = f.read()
    # warm cache
    engine.execute_raw_sql(engine.generate_sql(cache_sql)[-1])
    assert "local._total_customers" in engine.environment.concepts
    assert "local.total_customers" not in engine.environment.concepts
    engine.environment.parse("import cache;")
    cache_table = engine.environment.datasources["dashboard_agg_1"]
    for x in cache_table.columns:
        alias = x.alias
        assert isinstance(alias, str) and alias.startswith("_")

    query = engine.generate_sql(text)[0]
    results = list(engine.execute_raw_sql(query).fetchall())
    assert "dashboard_agg_1" in query, query
    # adhoc06 selects total_orders as the second column
    assert results[0][1] == 150000, results[0]


def test_one(engine):
    run_query(engine, 1)


def test_two(engine):
    run_query(engine, 2, sql_override=True)


def test_three(engine):
    run_query(engine, 3)


def test_four(engine):
    run_query(engine, 4)


def test_five(engine):
    run_query(engine, 5)


def test_six(engine):
    run_query(engine, 6)


def test_seven(engine):
    run_query(engine, 7)


def test_eight(engine):
    run_query(engine, 8)


def test_nine(engine):
    run_query(engine, 9)


def test_ten(engine):
    run_query(engine, 10)


def test_eleven(engine):
    run_query(engine, 11, sql_override=True)


def test_twelve(engine):
    run_query(engine, 12)


def test_thirteen(engine):
    run_query(engine, 13)


def test_fourteen(engine):
    run_query(engine, 14)


def test_fifteen(engine):
    run_query(engine, 15, sql_override=True)


def test_sixteen(engine):
    run_query(engine, 16)


def test_seventeen(engine):
    run_query(engine, 17)


def test_eighteen(engine):
    run_query(engine, 18)


def test_nineteen(engine):
    run_query(engine, 19)


def test_twenty(engine):
    run_query(engine, 20)


def test_twenty_one(engine):
    run_query(engine, 21)


def test_twenty_one_pushes_first_where_before_order_aggregates(engine):
    engine.environment = Environment(working_path=working_path)
    query = engine.generate_sql((working_path / "query21.preql").read_text())[-1]

    assert query.index('"order_orders"."o_orderstatus" = \'F\'') < query.index(
        "count(distinct"
    )
    assert re.search(r'FROM\s+"[^"]+"\s+INNER JOIN "memory"\."lineitem"', query)


def test_twenty_two(engine):
    run_query(engine, 22)
