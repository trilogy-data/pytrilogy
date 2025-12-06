import os
import platform
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import tomli_w
import tomllib

from trilogy import Executor
from trilogy.core.models.environment import Environment

# Get aggregate info
machine = platform.machine()
cpu_name = platform.processor()
cpu_count = os.cpu_count()

fingerprint = (
    f"{machine}-{cpu_name}-{cpu_count}".lower().replace(" ", "_").replace(",", "")
)

working_path = Path(__file__).parent.parent


def compare_values(a, b, rel_tol=1e-5, abs_tol=0.01):
    """Compare two values with tolerance for numeric types.

    Uses relative tolerance of 1e-5 (0.001%) and absolute tolerance of 0.01
    to handle floating-point rounding and decimal precision differences.
    """
    if type(a) is not type(b):
        # Handle Decimal vs float comparisons
        if isinstance(a, (int, float, Decimal)) and isinstance(
            b, (int, float, Decimal)
        ):
            a_float = float(a)
            b_float = float(b)
            return abs(a_float - b_float) <= max(
                rel_tol * max(abs(a_float), abs(b_float)), abs_tol
            )
        return False
    if isinstance(a, (int, float, Decimal)):
        a_float = float(a)
        b_float = float(b)
        return abs(a_float - b_float) <= max(
            rel_tol * max(abs(a_float), abs(b_float)), abs_tol
        )
    return a == b


def compare_rows(row1, row2, rel_tol=1e-5, abs_tol=0.01):
    """Compare two rows with tolerance for numeric values."""
    if len(row1) != len(row2):
        return False
    return all(compare_values(a, b, rel_tol, abs_tol) for a, b in zip(row1, row2))


def run_query(engine: Executor, idx: int, sql_override: bool = False):
    print("working_path")
    engine.environment = Environment(working_path=working_path)

    # build our aggregates
    with open(working_path / "aggregates" / f"query{idx:02d}_build.preql") as f:
        agg_text = f.read()
        engine.execute_text(agg_text)
    # now build our query
    with open(working_path / "aggregates" / f"query{idx:02d}.preql") as f:
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
        with open(working_path / "aggregates" / f"query{idx:02d}.sql") as f:
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
        assert compare_rows(
            row, comp_results[qidx]
        ), f"Row mismatch in row {qidx} (expected v actual): {row} != {comp_results[qidx]}"

    with open(working_path / "aggregates" / f"zquery{idx:02d}.log", "w") as f:
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

    timing = Path(working_path / "aggregates" / f"zquery_timing_{fingerprint}.log")

    if not timing.exists():
        with open(timing, "w") as f:
            pass

    with open(timing, "r+") as f:
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


def test_three(engine):
    query = run_query(engine, 3)
    assert len(query) < 7000, query
