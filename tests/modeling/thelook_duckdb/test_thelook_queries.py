"""Partial-bridge regression battery over a thelook-style model.

order_items binds BOTH ~user.id and ~product.id with no complete sibling —
the two-`~` span shape. Queries cover the pinned INNER-star heal,
extension-family and fact-anchored controls, and the unpinned span itself:
fact pairs at the requested grain plus one extension row per unmatched
member of each `~` dimension, with the extension families never
cross-pairing.
"""

from pathlib import Path

from tests.modeling._benchmark_artifacts import record_timing, write_query_log
from tests.modeling._benchmark_timing import benchmark_query, repeat_count_for_env
from tests.modeling._query_size import query_size
from tests.modeling._row_compare import rows_match
from trilogy import Executor
from trilogy.core.models.environment import Environment

working_path = Path(__file__).parent

# state x brand span truth: recorded pairs aggregated, plus each side's
# extension rows (states with a never-ordering user / brands never sold).
_SPAN_TRUTH = """
select u.state as state, p.brand as brand, sum(oi.sale_price) as revenue
from order_items oi
join users u on oi.user_id = u.id
join products p on oi.product_id = p.id
group by 1, 2
union all
select distinct u.state, null, null from users u
where u.id not in (select user_id from order_items where user_id is not null)
union all
select distinct null, p.brand, null from products p
where p.id not in (select product_id from order_items where product_id is not null)
"""


# Key-grain span truth for the multi-metric-family adhocs: fact rows at the
# four-key grain, plus each `~` side's extension rows, never cross-paired.
_KEY_SPAN_TRUTH = """
with fact as (
    select oi.order_id as order_id, oi.id as id, oi.user_id as user_id,
        oi.product_id as product_id,
        oi.sale_price as revenue,
        oi.sale_price - p.cost as margin,
        1 as sale_line_count,
        sum(oi.sale_price) over (partition by oi.order_id) as total_order_revenue
    from order_items oi
    left join products p on oi.product_id = p.id
)
select order_id, id, user_id, product_id, {metrics} from fact
union all
select null, null, u.id, null, {extension} from users u
where not exists (select 1 from order_items oi where oi.user_id = u.id)
union all
select null, null, null, p.id, {extension} from products p
where not exists (select 1 from order_items oi where oi.product_id = p.id)
"""


def _row_sort_key(t):
    return tuple((v is None, str(v)) for v in t)


def _assert_key_span_matches_truth(engine: Executor, name: str, truth_sql: str):
    truth = sorted(
        (tuple(r) for r in engine.execute_raw_sql(truth_sql).fetchall()),
        key=_row_sort_key,
    )
    assert any(row[2] is not None and row[1] is None for row in truth)
    assert any(row[3] is not None and row[1] is None for row in truth)
    engine.environment = Environment(working_path=working_path)
    text = (working_path / f"{name}.preql").read_text()
    sql = engine.generate_sql(text)[-1]
    rows = sorted(
        (tuple(r) for r in engine.execute_raw_sql(sql).fetchall()),
        key=_row_sort_key,
    )
    assert len(rows) == len(truth), (len(rows), len(truth))
    for got, want in zip(rows, truth):
        assert rows_match(got, want), (got, want)


def _span_sort_key(t):
    return (t[0] is None, t[0] or "", t[1] is None, t[1] or "")


def _assert_span_matches_truth(engine: Executor, name: str) -> None:
    truth = sorted(
        (tuple(r) for r in engine.execute_raw_sql(_SPAN_TRUTH).fetchall()),
        key=_span_sort_key,
    )
    # without extension rows on both sides the span is just the pinned INNER
    # star and this comparison proves nothing
    assert any(row[0] is None for row in truth)
    assert any(row[1] is None for row in truth)
    engine.environment = Environment(working_path=working_path)
    text = (working_path / f"{name}.preql").read_text()
    sql = engine.generate_sql(text)[-1]
    rows = sorted(
        (tuple(r) for r in engine.execute_raw_sql(sql).fetchall()),
        key=_span_sort_key,
    )
    assert len(rows) == len(truth)
    for got, want in zip(rows, truth):
        assert rows_match(got, want), (got, want)


REPEAT_TIME_CUTOFF = 0.15
REPEAT_COUNT = repeat_count_for_env(3)


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

    assert len(base_results) > 0, f"query{idx:02d} reference returned no rows"

    assert len(base_results) == len(
        comp_results
    ), f"Row count mismatch: expected {len(base_results)}, got {len(comp_results)}"
    for qidx, row in enumerate(base_results):
        assert rows_match(
            row, comp_results[qidx]
        ), f"Row mismatch in row {qidx} (expected v actual): {row} != {comp_results[qidx]}"

    write_query_log(
        working_path,
        query_label,
        query,
        gen_length=query_size(query, "sql"),
        preql_size=preql_size,
        comp_size=comp_size,
    )
    record_timing(
        working_path,
        query_label,
        benchmark.parse_time,
        benchmark.candidate_time,
        benchmark.reference_time,
    )
    return query


def test_adhoc01_unpinned_span(engine: Executor):
    _assert_span_matches_truth(engine, "adhoc01")


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


def test_adhoc02_span_through_agg(engine: Executor):
    """Same span sourced through the multi-`~` rollup: identical output."""
    _assert_span_matches_truth(engine, "adhoc02")


def test_adhoc03_span_with_dim_metric(engine: Executor):
    """A `~`-dim-content metric must not split the span: product extensions
    stay, once each."""
    _assert_key_span_matches_truth(
        engine,
        "adhoc03",
        _KEY_SPAN_TRUTH.format(
            metrics="revenue, margin, sale_line_count", extension="null, null, 0"
        ),
    )


def test_adhoc04_two_metric_families(engine: Executor):
    """Sibling metric contributors stitch on the fact key without null-pairing
    the extension families (no user x product cross rows)."""
    _assert_key_span_matches_truth(
        engine,
        "adhoc04",
        _KEY_SPAN_TRUTH.format(
            metrics="revenue, margin, total_order_revenue",
            extension="null, null, null",
        ),
    )


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


def test_nineteen(engine):
    query = run_query(engine, 19)
    assert "is not distinct from" not in query, query
    assert query.count("FULL") == 1, query


def test_twenty(engine):
    query = run_query(engine, 20)
    assert "is not distinct from" not in query, query
    assert query.count("FULL") == 1, query


def test_twenty_one(engine):
    query = run_query(engine, 21)
    assert "is not distinct from" not in query, query
    assert query.count("FULL") == 1, query


def test_twenty_two(engine):
    query = run_query(engine, 22)
    assert "is not distinct from" not in query, query
    assert "FULL" not in query, query
