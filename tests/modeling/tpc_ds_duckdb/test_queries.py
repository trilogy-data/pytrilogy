from datetime import datetime
from pathlib import Path

import pytest
import tomli_w
import tomllib

from trilogy import Environment, Executor

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
    assert len(query) < 9000, query


def test_two(engine):
    run_query(engine, 2, sql_override=True)


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
    assert len(query) < 7100, query


def test_seven(engine):
    query = run_query(engine, 7)
    assert len(query) < 3000, query


def test_eight(engine):
    run_query(engine, 8)


def test_ten(engine):
    query = run_query(engine, 10)
    assert len(query) < 7000, query


def test_twelve(engine):
    run_query(engine, 12)


def test_fifteen(engine):
    run_query(engine, 15)


def test_sixteen(engine):
    query = run_query(engine, 16)
    # size gating
    assert len(query) < 16000, query


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


def test_ninety_seven(engine):
    _ = run_query(engine, 97)


def test_ninety_eight(engine):
    _ = run_query(engine, 98)


def test_ninety_nine(engine):
    _ = run_query(engine, 99)


def run_adhoc(number: int, text: str | None = None):
    from logging import INFO

    from trilogy import Dialects, Environment
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
    TEST = """WITH wscs AS
  (SELECT sold_date_sk,
          sales_price
   FROM
     (SELECT ws_sold_date_sk sold_date_sk,
             ws_ext_sales_price sales_price
      FROM web_sales
      UNION ALL SELECT cs_sold_date_sk sold_date_sk,
                       cs_ext_sales_price sales_price
      FROM catalog_sales) sq1),
     wswscs AS
  (SELECT d_week_seq,
          sum(CASE
                  WHEN (d_day_name='Sunday') THEN sales_price
                  ELSE NULL
              END) sun_sales,
          sum(CASE
                  WHEN (d_day_name='Monday') THEN sales_price
                  ELSE NULL
              END) mon_sales,
          sum(CASE
                  WHEN (d_day_name='Tuesday') THEN sales_price
                  ELSE NULL
              END) tue_sales,
          sum(CASE
                  WHEN (d_day_name='Wednesday') THEN sales_price
                  ELSE NULL
              END) wed_sales,
          sum(CASE
                  WHEN (d_day_name='Thursday') THEN sales_price
                  ELSE NULL
              END) thu_sales,
          sum(CASE
                  WHEN (d_day_name='Friday') THEN sales_price
                  ELSE NULL
              END) fri_sales,
          sum(CASE
                  WHEN (d_day_name='Saturday') THEN sales_price
                  ELSE NULL
              END) sat_sales
   FROM wscs,
        date_dim
   WHERE d_date_sk = sold_date_sk
   GROUP BY d_week_seq)
SELECT d_week_seq1,
       round(sun_sales1/sun_sales2, 2) r1,
       round(mon_sales1/mon_sales2, 2) r2,
       round(tue_sales1/tue_sales2, 2) r3,
       round(wed_sales1/wed_sales2, 2) r4,
       round(thu_sales1/thu_sales2, 2) r5,
       round(fri_sales1/fri_sales2, 2) r6,
       round(sat_sales1/sat_sales2, 2)
FROM
  (SELECT wswscs.d_week_seq d_week_seq1,
          sun_sales sun_sales1,
          mon_sales mon_sales1,
          tue_sales tue_sales1,
          wed_sales wed_sales1,
          thu_sales thu_sales1,
          fri_sales fri_sales1,
          sat_sales sat_sales1
   FROM wswscs,
        date_dim
   WHERE date_dim.d_week_seq = wswscs.d_week_seq
     AND d_year = 2001) y,
  (SELECT wswscs.d_week_seq d_week_seq2,
          sun_sales sun_sales2,
          mon_sales mon_sales2,
          tue_sales tue_sales2,
          wed_sales wed_sales2,
          thu_sales thu_sales2,
          fri_sales fri_sales2,
          sat_sales sat_sales2
   FROM wswscs,
        date_dim
   WHERE date_dim.d_week_seq = wswscs.d_week_seq
     AND d_year = 2001+1) z
WHERE d_week_seq1 = d_week_seq2-53
ORDER BY d_week_seq1 NULLS FIRST;"""
    run_adhoc(2, TEST)
