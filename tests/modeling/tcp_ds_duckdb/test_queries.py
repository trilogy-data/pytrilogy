from pathlib import Path

from trilogy import Executor
import pytest
from datetime import datetime
import json

working_path = Path(__file__).parent


def run_query(engine: Executor, idx: int):

    with open(working_path / f"query{idx:02d}.preql") as f:
        text = f.read()

    # fetch our results
    parse_start = datetime.now()
    query = engine.generate_sql(text)[-1]
    parse_time = datetime.now() - parse_start
    # raise SyntaxError(query)
    exec_start = datetime.now()
    results = engine.execute_raw_sql(query)
    exec_time = datetime.now() - exec_start
    # assert results == ''
    comp_results = list(results.fetchall())
    assert len(comp_results) > 0, "No results returned"
    # run the built-in comp
    comp_start = datetime.now()
    base = engine.execute_raw_sql(f"PRAGMA tpcds({idx});")
    base_results = list(base.fetchall())
    comp_time = datetime.now() - comp_start

    # # check we got it
    if len(base_results) != len(comp_results):
        assert False, f"Row count mismatch: {len(base_results)} != {len(comp_results)}"
    for qidx, row in enumerate(base_results):
        assert (
            row == comp_results[qidx]
        ), f"Row mismatch in row {qidx} (expected v actual): {row} != {comp_results[qidx]}"

    with open(working_path / f"zquery{idx:02d}.log", "w") as f:
        f.write(
            json.dumps(
                {
                    "query_id": idx,
                    "parse_time": parse_time.total_seconds(),
                    "exec_time": exec_time.total_seconds(),
                    "comp_time": comp_time.total_seconds(),
                    "generated_sql": query,
                },
                indent=4,
            )
        )
    return query


def test_one(engine):
    run_query(engine, 1)


@pytest.mark.skip(reason="Is duckdb correct??")
def test_two(engine):
    run_query(engine, 2)


def test_three(engine):
    run_query(engine, 3)


@pytest.mark.skip(reason="Is duckdb correct??")
def test_four(engine):
    run_query(engine, 4)


@pytest.mark.skip(reason="Is duckdb correct??")
def test_five(engine):
    run_query(engine, 5)


def test_six(engine):
    query = run_query(engine, 6)
    assert len(query) < 5500, query


def test_seven(engine):
    run_query(engine, 7)


def test_eight(engine):
    run_query(engine, 8)


def test_ten(engine):
    query = run_query(engine, 10)
    assert len(query) < 10000, query


def test_twelve(engine):
    run_query(engine, 12)


def test_fifteen(engine):
    run_query(engine, 15)


def test_sixteen(engine):
    query = run_query(engine, 16)
    # size gating
    assert len(query) < 7500, query


def run_adhoc(number: int):
    from trilogy import Environment, Dialects
    from trilogy.hooks.query_debugger import DebuggingHook
    from logging import DEBUG

    env = Environment(working_path=Path(__file__).parent)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook(DEBUG)]
    )
    engine.execute_raw_sql(
        """INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=1);"""
    )
    run_query(engine, number)


if __name__ == "__main__":
    run_adhoc(10)
