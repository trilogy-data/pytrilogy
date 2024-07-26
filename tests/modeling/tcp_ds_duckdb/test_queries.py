from pathlib import Path

from trilogy import Executor
import pytest


working_path = Path(__file__).parent


def run_query(engine: Executor, idx: int):

    with open(working_path / f"query{idx:02d}.preql") as f:
        text = f.read()

    # fetch our results
    # query = engine.generate_sql(text)[-1]
    # raise SyntaxError(query)
    results = engine.execute_text(text)
    # assert results == ''
    comp_results = list(results[-1].fetchall())
    assert len(comp_results) > 0, "No results returned"
    # run the built-in comp
    base = engine.execute_raw_sql(f"PRAGMA tpcds({idx});")
    base_results = list(base.fetchall())

    # # check we got it
    if len(base_results) != len(comp_results):
        assert False, f"Row count mismatch: {len(base_results)} != {len(comp_results)}"
    for idx, row in enumerate(base_results):
        assert (
            row == comp_results[idx]
        ), f"Row mismatch (expected v actual): {row} != {comp_results[idx]}"


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
    run_query(engine, 6)


def test_seven(engine):
    run_query(engine, 7)


def test_eight(engine):
    run_query(engine, 8)


def test_ten(engine):
    run_query(engine, 10)


def test_twelve(engine):
    run_query(engine, 12)


# def test_eleven(engine):
#     run_query(engine, 11)


def run_adhoc(number: int):
    from trilogy import Environment, Dialects
    from trilogy.hooks.query_debugger import DebuggingHook

    env = Environment(working_path=Path(__file__).parent)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook()]
    )
    engine.execute_raw_sql(
        """INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=1);"""
    )
    run_query(engine, number)


if __name__ == "__main__":
    run_adhoc(2)
