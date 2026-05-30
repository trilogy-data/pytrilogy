import os
import platform
import time
from pathlib import Path
from typing import Callable, TypeVar

import tomli_w
import tomllib

from tests.modeling.tpc_ds_duckdb.query_size import query_size
from trilogy import Executor
from trilogy.core.models.environment import Environment

# Get aggregate info
machine = platform.machine()
cpu_name = platform.processor()
cpu_count = os.cpu_count()

fingerprint = (
    f"{machine}-{cpu_name}-{cpu_count}".lower().replace(" ", "_").replace(",", "")
)

working_path = Path(__file__).parent

# Sub-cutoff timings are dominated by scheduler/timer jitter rather than real
# query cost, so a single sample is not reproducible. When any stage lands
# under the cutoff we re-run it REPEAT_COUNT extra times (trilogy/reference
# interleaved so cache warmth stays symmetric) and keep the minimum -- noise
# only ever adds time, so the min is the most faithful estimate of true cost.
REPEAT_TIME_CUTOFF = 0.15
REPEAT_COUNT = 3

T = TypeVar("T")


def _time(fn: Callable[[], T]) -> tuple[float, T]:
    """Monotonic, high-resolution elapsed time around fn (perf_counter, not
    datetime.now, which is wall-clock and ~15ms-quantized on Windows)."""
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

    # Resolve the reference SQL up front so file IO never lands inside a
    # timing window.
    if sql_override:
        rquery = (working_path / f"query{idx:02d}.sql").read_text()
    else:
        rquery = f"PRAGMA tpcds({idx});"

    # Time exec *including* fetch on both sides -- DuckDB materializes lazily,
    # so excluding fetch on one side only would bias the comparison.
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

    # Always prefer the on-disk reference SQL for size comparison when available,
    # so the PRAGMA-driven runs still report a meaningful comp_size.
    sql_path = working_path / f"query{idx:02d}.sql"
    comp_source = sql_path.read_text() if sql_path.exists() else rquery
    comp_size = query_size(comp_source, "sql")

    if len(base_results) > 0:
        assert len(comp_results) > 0, "No results returned"

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
    assert '"memory"."store_sales"' not in query


def test_two_one(engine):
    query = run_query(
        engine, 2, sql_override=True, preql_file="query02-one.preql", label="2.1"
    )
    assert len(query) < 7500, query


def test_two_two(engine):
    query = run_query(
        engine, 2, sql_override=True, preql_file="query02-two.preql", label="2.2"
    )
    assert len(query) < 7500, query


def test_three(engine):
    query = run_query(engine, 3)
    assert len(query) < 2000, query


def test_four(engine):
    run_query(engine, 4)


def test_five(engine):
    run_query(engine, 5)


def test_six(engine):
    query = run_query(engine, 6)
    assert len(query) < 3200, query


def test_seven(engine):
    query = run_query(engine, 7)
    assert len(query) < 2500, query


def test_eight(engine):
    query = run_query(engine, 8)
    assert len(query) < 3800, query


def test_nine(engine):
    query = run_query(engine, 9)
    assert len(query) < 8000, query


def test_ten(engine):
    query = run_query(engine, 10)
    assert len(query) < 6500, query


def test_eleven(engine):
    query = run_query(engine, 11)
    assert len(query) < 12000, query


def test_twelve(engine):
    query = run_query(engine, 12)
    assert len(query) <= 3200, query


def test_thirteen(engine):
    query = run_query(engine, 13)
    assert len(query) < 5500, query


def test_fourteen(engine):
    run_query(engine, 14)


def test_fifteen(engine):
    query = run_query(engine, 15)
    assert len(query) < 2300, query


def test_sixteen(engine):
    query = run_query(engine, 16)
    # size gating
    assert len(query) < 5500, query


def test_seventeen(engine):
    query = run_query(engine, 17, sql_override=True)
    assert len(query) < 12000, query


def test_eighteen(engine):
    _ = run_query(engine, 18)


def test_nineteen(engine):
    query = run_query(engine, 19, sql_override=True)
    assert len(query) < 4000, query


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


def test_twenty_three(engine_sf001):
    query = run_query(engine_sf001, 23)
    assert len(query) < 8500, query


def test_twenty_four(engine):
    _ = run_query(engine, 24)
    # size gating
    # assert len(query) < 1000, query


def test_twenty_five(engine):
    query = run_query(engine, 25)
    # size gating
    assert len(query) < 8500, query


def test_twenty_six(engine):
    _ = run_query(engine, 26)
    # size gating
    # assert len(query) < 6000, query


def test_twenty_seven(engine):
    query = run_query(engine, 27)
    assert len(query) < 14000, query


def test_twenty_eight(engine):
    _ = run_query(engine, 28)


def test_twenty_nine(engine):
    query = run_query(engine, 29)
    assert len(query) < 12000, query
    assert "is not distinct from" not in query, query


def test_thirty(engine):
    query = run_query(engine, 30)
    # size gating
    assert len(query) < 12000, query


def test_thirty_alt(engine):
    # Non-rowset conditional-aggregate form; equivalent to PRAGMA tpcds(30).
    query = run_query(engine, 30, preql_file="query30-alt.preql", label="30.alt")
    assert len(query) < 12000, query
    assert query.count('"memory"."web_returns"') == 1, query
    assert query.count("GROUP BY") == 2, query
    assert '"WR_RETURNING_ADDR_SK" as "web_returns_return_address_id"' not in query
    assert 'LEFT OUTER JOIN "abundant"' not in query
    assert 'web_returns_return_address_state" is not distinct from' in query


def test_thirty_one(engine):
    query = run_query(engine, 31)
    # Larger after UnionDimPushdown: dim joins + WHEREs land per branch.
    assert len(query) < 7500, query


def test_thirty_two(engine):
    # Our query32.preql uses manufacturer 48 (sf=1 has zero catalog sales for
    # the spec's 977 in the date window); compare against the matching .sql
    # file rather than PRAGMA, which still uses the spec constant.
    query = run_query(engine, 32, sql_override=True)
    # size gating
    assert len(query) < 12640, query


def test_thirty_three(engine):
    query = run_query(engine, 33)
    # Larger after UnionDimPushdown: dim joins + WHEREs land per branch.
    assert len(query) < 5500, query


def test_thirty_four(engine):
    query = run_query(engine, 34, sql_override=True)
    assert len(query) < 6000, query


def test_thirty_five(engine):
    query = run_query(engine, 35)
    assert len(query) < 11000, query


def test_thirty_six(engine):
    query = run_query(engine, 36)
    assert len(query) < 8500, query


def test_thirty_seven(engine):
    query = run_query(engine, 37)
    assert len(query) < 2200, query


def test_thirty_eight(engine):
    _ = run_query(engine, 38)


def test_thirty_nine(engine):
    query = run_query(engine, 39)
    assert len(query) < 4500, query


def test_forty(engine):
    query = run_query(engine, 40, sql_override=True)
    assert len(query) < 8000, query


def test_forty_one(engine):
    # query41.preql uses manufacturer_id BETWEEN 1 AND 500 (spec's narrow
    # profiles produce zero rows at this scale); compare against the matching
    # .sql file rather than PRAGMA, which still uses spec constants.
    query = run_query(engine, 41, sql_override=True)
    assert len(query) < 8000, query


def test_forty_two(engine):
    _ = run_query(engine, 42)


def test_forty_three(engine):
    query = run_query(engine, 43)
    assert len(query) < 5000, query


def test_forty_four(engine):
    # query44.preql/.sql both filter to store id=1 (spec uses 4, which has
    # no rows at this scale); compare against the matching .sql file rather
    # than PRAGMA, which still uses the spec constant.
    _ = run_query(engine, 44, sql_override=True)


def test_forty_five(engine):
    query = run_query(engine, 45)
    assert len(query) < 6000, query


def test_forty_six(engine):
    query = run_query(engine, 46)
    assert len(query) < 8000, query
    assert '"memory"."customer" as "physical_sales_customer_customers"' not in query, query
    assert query.count("GROUP BY") == 1, query


def test_forty_seven(engine):
    query = run_query(engine, 47)
    assert len(query) < 6800, query


def test_forty_eight(engine):
    query = run_query(engine, 48)
    assert len(query) < 3000, query


def test_forty_nine(engine):
    query = run_query(engine, 49)
    assert len(query) < 13000, query


def test_fifty(engine):
    query = run_query(engine, 50)
    assert len(query) < 7000, query


def test_fifty_one(engine):
    query = run_query(engine, 51)
    assert len(query) < 8000, query


def test_fifty_two(engine):
    query = run_query(engine, 52)
    assert len(query) < 2000, query


def test_fifty_three(engine):
    query = run_query(engine, 53)
    assert len(query) < 6000, query


def test_fifty_four(engine):
    _ = run_query(engine, 54)


def test_fifty_five(engine):
    _ = run_query(engine, 55)


def test_fifty_six(engine):
    _ = run_query(engine, 56, sql_override=True)


def test_fifty_seven(engine):
    query = run_query(engine, 57)
    assert len(query) < 6500, query


def test_fifty_eight(engine):
    query = run_query(engine, 58)
    assert len(query) < 7000, query


# Override: reference y/x subqueries cross-join wss to date_dim on week_seq,
# producing ~49 duplicate rows per (store, week_seq) pair. The override
# pre-dedups the week filter so row-by-row comparison is meaningful.
def test_fifty_nine(engine):
    query = run_query(engine, 59, sql_override=True)
    assert len(query) < 12000, query


def test_sixty(engine):
    query = run_query(engine, 60)
    assert len(query) < 5000, query


def test_sixty_one(engine):
    query = run_query(engine, 61)
    assert len(query) < 8000, query


def test_sixty_two(engine):
    query = run_query(engine, 62)
    assert len(query) < 2500, query


def test_sixty_three(engine):
    query = run_query(engine, 63)
    assert len(query) < 6000, query


def test_sixty_four(engine_sf001):
    _ = run_query(engine_sf001, 64, sql_override=True)


def test_sixty_five(engine):
    query = run_query(engine, 65)
    assert len(query) < 5000, query
    assert query.count('"memory"."store_sales"') == 1, query


def test_sixty_six(engine):
    query = run_query(engine, 66)
    assert len(query) < 38000, query


def test_sixty_seven(engine):
    query = run_query(engine, 67)
    assert len(query) < 5000, query


def test_sixty_eight(engine):
    query = run_query(engine, 68)
    assert len(query) < 8000, query
    assert '"memory"."customer" as "physical_sales_customer_customers"' not in query, query


def test_sixty_nine(engine):
    query = run_query(engine, 69)
    assert len(query) < 5000, query


def test_seventy(engine):
    _ = run_query(engine, 70)


def test_seventy_one(engine):
    query = run_query(engine, 71)
    # Larger after UnionDimPushdown: dim joins + WHEREs land per branch.
    assert len(query) < 5500, query


def test_seventy_two(engine_sf001):
    query = run_query(engine_sf001, 72)
    assert len(query) < 8000, query


def test_seventy_three(engine):
    query = run_query(engine, 73)
    assert len(query) < 3000, query


def test_seventy_four(engine):
    _ = run_query(engine, 74)


def test_seventy_five(engine):
    _ = run_query(engine, 75)


def test_seventy_six(engine):
    query = run_query(engine, 76)
    assert len(query) < 10000, query


def test_seventy_seven(engine):
    _ = run_query(engine, 77)


def test_seventy_eight(engine):
    _ = run_query(engine, 78)


def test_seventy_nine(engine):
    query = run_query(engine, 79)
    assert len(query) < 8000, query


def test_eighty(engine):
    query = run_query(engine, 80)
    # size gating
    assert len(query) < 8000, query


def test_eighty_one(engine):
    query = run_query(engine, 81)
    # size gating
    assert len(query) < 8000, query


def test_eighty_two(engine):
    query = run_query(engine, 82, sql_override=True)
    assert len(query) < 4000, query


def test_eighty_three(engine):
    query = run_query(engine, 83)
    # Larger after UnionDimPushdown: dim joins + WHEREs land per branch.
    assert len(query) < 9500, query


def test_eighty_four(engine):
    query = run_query(engine, 84)
    assert len(query) < 4000, query


def test_eighty_five(engine_sf001):
    query = run_query(engine_sf001, 85)
    assert len(query) < 12000, query


def test_eighty_six(engine):
    query = run_query(engine, 86)
    assert len(query) < 6000, query


def test_eighty_seven(engine):
    _ = run_query(engine, 87)


def test_eighty_eight(engine):
    query = run_query(engine, 88)
    assert len(query) < 5000, query


def test_eighty_nine(engine):
    query = run_query(engine, 89)
    assert len(query) < 8000, query


def test_ninety(engine):
    query = run_query(engine, 90)
    assert len(query) < 2500, query


def test_ninety_one(engine):
    query = run_query(engine, 91)
    assert len(query) < 8000, query


def test_ninety_two(engine):
    query = run_query(engine, 92)
    assert len(query) < 2000, query


def test_ninety_three(engine):
    query = run_query(engine, 93)
    assert len(query) < 6000, query


def test_ninety_four(engine):
    query = run_query(engine, 94)
    assert len(query) < 5000, query


def test_ninety_five(engine):
    query = run_query(engine, 95)
    assert len(query) < 6000, query


def test_ninety_six(engine):
    query = run_query(engine, 96)
    assert len(query) < 3000, query


def test_ninety_seven(engine):
    query = run_query(engine, 97)
    assert len(query) < 3000, query


def test_ninety_seven_one(engine):
    query = run_query(engine, 97, preql_file="query97-one.preql", label="97.1")
    assert len(query) < 4250, query


def test_ninety_seven_two(engine):
    query = run_query(engine, 97, preql_file="query97-two.preql", label="97.2")
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
