from datetime import datetime
from pathlib import Path

import pytest
import tomli_w
import tomllib

from trilogy import Executor
from trilogy.core.models.environment import Environment

working_path = Path(__file__).parent


def run_query(engine: Executor, idx: int, sql_override: bool = False):
    engine.environment = Environment(working_path=working_path)
    with open(working_path / f"query{idx:02d}.preql") as f:
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

    with open(working_path / f"zquery{idx:02d}.log", "w") as f:
        f.write(
            tomli_w.dumps(
                {
                    "query_id": idx,
                    "gen_length": len(query),
                    "generated_sql": query,
                },
                multiline_strings=True,
            )
        )

    timing = Path(working_path / "zquery_timing.log")

    if not timing.exists():
        with open(working_path / "zquery_timing.log", "w") as f:
            pass

    with open(working_path / "zquery_timing.log", "r+") as f:
        # seek to 0, as we use append to ensure it exists
        current = tomllib.loads(f.read())
        # go back to 0, as we will rewrite the whole thing

        # modify the current dict
        current[f"query_{idx:02d}"] = {
            "parse_time": parse_time.total_seconds(),
            "exec_time": exec_time.total_seconds(),
            "comp_time": comp_time.total_seconds(),
        }
        final = {x: current[x] for x in sorted(list(current.keys()))}
        # dump it all back
        f.seek(0)
        f.write(
            tomli_w.dumps(
                final,
                multiline_strings=True,
            )
        )
        f.truncate()
    return query


def test_one(engine):
    query = run_query(engine, 1)
    assert len(query) < 2000, query


def test_two(engine):
    query = run_query(engine, 2, sql_override=True)
    assert len(query) < 8800, query


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
    assert len(query) < 7100, query


def test_seven(engine):
    query = run_query(engine, 7)
    assert len(query) < 2500, query


def test_eight(engine):
    query = run_query(engine, 8)
    assert len(query) < 3550, query


def test_ten(engine):
    query = run_query(engine, 10)
    assert len(query) < 7000, query


def test_twelve(engine):
    run_query(engine, 12)


def test_fifteen(engine):
    query = run_query(engine, 15)
    assert len(query) < 2300, query


def test_sixteen(engine):
    query = run_query(engine, 16)
    # size gating
    assert len(query) < 6000, query


def test_twenty(engine):
    _ = run_query(engine, 20)
    # size gating
    # assert len(query) < 6000, query


def test_twenty_one(engine):
    _ = run_query(engine, 21)
    # size gating
    # assert len(query) < 6000, query


def test_twenty_four(engine):
    _ = run_query(engine, 24)
    # size gating
    # assert len(query) < 6000, query


def test_twenty_five(engine):
    query = run_query(engine, 25)
    # size gating
    assert len(query) < 12000, query


def test_twenty_six(engine):
    _ = run_query(engine, 26)
    # size gating
    # assert len(query) < 6000, query


def test_thirty(engine):
    _ = run_query(engine, 30)
    # size gating
    # assert len(query) < 6000, query


def test_ninety_five(engine):
    _ = run_query(engine, 95)


def test_ninety_seven(engine):
    query = run_query(engine, 97)
    assert len(query) < 4200, query


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
    engine.execute_raw_sql(
        """INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=1);"""
    )
    if text:
        rows = engine.execute_raw_sql(text)
        for row in rows:
            print(row)
    run_query(engine, number)


if __name__ == "__main__":

    run_adhoc(95)
