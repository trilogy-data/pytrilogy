import os
import platform
from datetime import datetime
from pathlib import Path

import pytest
import tomli_w
import tomllib

from trilogy import Executor
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import process_query
from trilogy.parser import parse_text

# Get aggregate info
machine = platform.machine()
cpu_name = platform.processor()
cpu_count = os.cpu_count()

fingerprint = (
    f"{machine}-{cpu_name}-{cpu_count}".lower().replace(" ", "_").replace(",", "")
)

working_path = Path(__file__).parent


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

    # fetch our results
    parse_start = datetime.now()
    query = engine.generate_sql(text)[-1]
    parse_time = datetime.now() - parse_start
    exec_start = datetime.now()
    results = engine.execute_raw_sql(query)
    exec_time = datetime.now() - exec_start
    # assert results == ''
    comp_results = list(results.fetchall())
    assert len(comp_results) > 0, "No results returned"
    # run the built-in comp
    comp_start = datetime.now()
    if sql_override:
        with open(working_path / f"query{idx:02d}.sql") as f:
            rquery = f.read()
    else:
        rquery = f"PRAGMA tpcds({idx});"
    base = engine.execute_raw_sql(rquery)
    base_results = list(base.fetchall())
    comp_time = datetime.now() - comp_start

    # # check we got it
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
                    "gen_length": len(query),
                    "generated_sql": query,
                },
                multiline_strings=True,
            )
        )

    timing = Path(working_path / f"zquery_timing_{fingerprint}.log")
    current = _load_toml_mapping(timing)
    current[f"query_{query_label}"] = {
        "parse_time": parse_time.total_seconds(),
        "exec_time": exec_time.total_seconds(),
        "comp_time": comp_time.total_seconds(),
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


def test_adhoc_one(engine: Executor):
    engine.environment = Environment(working_path=working_path)
    idx = 1
    with open(working_path / f"adhoc{idx:02d}.preql") as f:
        text = f.read()
    # find better non-hacky way to do this
    with open(working_path / f"adhoc{idx:02d}_imports.preql", "r") as f:
        text2 = f.read()
        engine.execute_text(text2, non_interactive=True)
    query = engine.generate_sql(text)[-1]

    engine.execute_raw_sql(query)


def test_adhoc_two(engine: Executor):
    engine.environment = Environment(working_path=working_path)
    idx = 2
    with open(working_path / f"adhoc{idx:02d}.preql") as f:
        text = f.read()
    # find better non-hacky way to do this
    with open(working_path / f"adhoc{idx:02d}_imports.preql", "r") as f:
        text2 = f.read()
        engine.execute_text(text2, non_interactive=True)
    print(text)
    query = engine.generate_sql(text)[-1]

    results = engine.execute_raw_sql(query)
    assert "is not distinct from" in query, query
    assert len(results.fetchall()) == 6, query


# def test_adhoc_shape_two(engine: Executor):
#     engine.environment = Environment(working_path=working_path)
#     from trilogy.core.processing.node_generators import gen_group_node
#     from trilogy.core.processing.concept_strategies_v3 import search_concepts, History
#     idx = 11
#     with open(working_path / f"adhoc{idx:02d}.preql") as f:
#         text = f.read()
#     engine.environment.parse(text)
#     built = engine.environment.materialize_for_select()

#     base = gen_group_node(
#         environment=built,
#         depth = 0,
#         source_concepts = search_concepts,
#         history = History(),
#         conditions=[]
#     )


def test_one(engine):
    query = run_query(engine, 1)
    assert len(query) < 4300, query


def test_two(engine):
    query = run_query(engine, 2, sql_override=True)
    assert len(query) < 7500, query


def test_two_merge_aggregate_compacts_inline_window_query():
    query = """
    import catalog_sales as catalog_sales;
    import web_sales as web_sales;
    import date as date;

    merge catalog_sales.date.* into ~date.*;
    merge web_sales.date.* into ~date.*;

    auto relevent_week_seq <- filter date.week_seq where date.year in (2001, 2002);

    def weekday_sales(weekday) ->
        (SUM(CASE WHEN date.day_of_week = weekday THEN web_sales.ext_sales_price ELSE 0.0 END) by date.week_seq +
        SUM(CASE WHEN date.day_of_week = weekday THEN catalog_sales.ext_sales_price ELSE 0.0 END) by date.week_seq)
    ;

    def round_lag(sales)-> round(sales / (lead 53 sales by date.week_seq asc), 2);

    WHERE
        date.week_seq in relevent_week_seq
    SELECT
        date.week_seq,
        @round_lag(@weekday_sales(0)) as sunday_increase,
        @round_lag(@weekday_sales(1)) as monday_increase,
        @round_lag(@weekday_sales(2)) as tuesday_increase,
        @round_lag(@weekday_sales(3)) as wednesday_increase,
        @round_lag(@weekday_sales(4)) as thursday_increase,
        @round_lag(@weekday_sales(5)) as friday_increase,
        @round_lag(@weekday_sales(6)) as saturday_increase
    having sunday_increase is not null
    ORDER BY date.week_seq asc NULLS FIRST
    LIMIT 100;
    """

    original = CONFIG.optimizations.merge_aggregate
    try:
        CONFIG.optimizations.merge_aggregate = False
        off_env = Environment(working_path=working_path)
        _, off_statements = parse_text(query, off_env)
        off_processed = process_query(off_env, off_statements[-1])

        CONFIG.optimizations.merge_aggregate = True
        on_env = Environment(working_path=working_path)
        _, on_statements = parse_text(query, on_env)
        on_processed = process_query(on_env, on_statements[-1])
    finally:
        CONFIG.optimizations.merge_aggregate = original

    assert len(off_processed.ctes) == 9
    assert len(on_processed.ctes) == 5


def test_three(engine):
    query = run_query(engine, 3)
    assert len(query) < 2000, query


@pytest.mark.skip(reason="Is duckdb correct??")
def test_four(engine):
    run_query(engine, 4)


@pytest.mark.skip(reason="Is duckdb correct??")
def test_five(engine):
    run_query(engine, 5)


def test_six(engine):
    query = run_query(engine, 6)
    assert len(query) < 2700, query


def test_seven(engine):
    query = run_query(engine, 7)
    assert len(query) < 2500, query


def test_eight(engine):
    query = run_query(engine, 8)
    assert len(query) < 3800, query


def test_ten(engine):
    query = run_query(engine, 10)
    assert len(query) < 6500, query


def test_twelve(engine):
    query = run_query(engine, 12)
    assert len(query) <= 3200, query


def test_fifteen(engine):
    query = run_query(engine, 15)
    assert len(query) < 2300, query


def test_sixteen(engine):
    query = run_query(engine, 16)
    # size gating
    assert len(query) < 5500, query


def test_twenty(engine):
    query = run_query(engine, 20)
    # size gating
    assert len(query) < 3200, query


def test_twenty_one(engine):
    query = run_query(engine, 21)
    # size gating
    assert len(query) < 3000, query


def test_twenty_two(engine):
    _ = run_query(engine, 22)


def test_twenty_four(engine):
    _ = run_query(engine, 24)
    # size gating
    # assert len(query) < 1000, query


def test_twenty_five(engine):
    query = run_query(engine, 25)
    # size gating
    assert len(query) < 8500, query


def test_forty_two(engine):
    _ = run_query(engine, 42)


@pytest.mark.skip(reason="Still cooking")
def test_forty_four(engine):
    _ = run_query(engine, 44)


def test_twenty_six(engine):
    _ = run_query(engine, 26)
    # size gating
    # assert len(query) < 6000, query


def test_thirty(engine):
    query = run_query(engine, 30)
    # size gating
    assert len(query) < 12000, query


def test_thirty_two(engine):
    query = run_query(engine, 32)
    # size gating
    assert len(query) < 12640, query


def test_fifty_five(engine):
    _ = run_query(engine, 55)


# def test_fifty_six(engine):
#     _ = run_query(engine, 56)


def test_ninety_five(engine):
    query = run_query(engine, 95)
    assert len(query) < 6000, query


def test_ninety_seven(engine):
    query = run_query(engine, 97)
    assert len(query) < 5000, query


def test_ninety_seven_alt(engine):
    query = run_query(engine, 97, preql_file="query97-one.preql", label="97.1")
    assert len(query) < 4250, query


def test_ninety_eight(engine):
    _ = run_query(engine, 98)
    # assert len(query) < 4200, query


def test_ninety_nine(engine):
    query = run_query(engine, 99)
    assert len(query) < 4000, query


def run_adhoc(number: int, text: str | None = None):
    from logging import INFO

    from trilogy import Dialects
    from trilogy.hooks.query_debugger import DebuggingHook

    env = Environment(working_path=Path(__file__).parent)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(INFO)]
    )
    engine.execute_raw_sql("""INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=1);""")
    if text:
        rows = engine.execute_raw_sql(text)
        for row in rows:
            print(row)
    run_query(engine, number)


if __name__ == "__main__":

    run_adhoc(95)
