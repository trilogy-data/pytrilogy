"""q49 (bug_q49_binder_ungrouped_channel): ORDER BY on an expression that
duplicates a select output must bind to that output, not re-render raw source
columns a grouped final node no longer exposes (DuckDB BinderException:
"column must appear in the GROUP BY clause").

Trigger: a window output forces a group_to_grain wrapper over the window CTE;
an ORDER BY kept as an anonymous expression then re-derives ungrouped columns.
"""

from trilogy import Dialects

MODEL = r"""
key sale_id int;
property sale_id.channel string;
property sale_id.item_id int;
property sale_id.quantity int;

datasource sales (
    s: sale_id,
    ch: channel,
    i: item_id,
    q: quantity)
grain (sale_id)
query '''
select 1 as s, 'WEB' as ch, 100 as i, 5 as q
union all
select 2, 'WEB', 101, 3
union all
select 3, 'STORE', 100, 7
union all
select 4, 'STORE', 102, 2
union all
select 5, 'CATALOG', 101, 1
''';
"""

WINDOW_SELECT = r"""
select
    lower(channel) as chan,
    item_id,
    rank(item_id) over (partition by channel order by sum(quantity) asc) as r
"""


def _run(query: str):
    executor = Dialects.DUCK_DB.default_executor()
    sql = executor.generate_sql(MODEL + query)[-1]
    assert "INVALID_REFERENCE" not in sql
    return executor.execute_raw_sql(sql).fetchall(), sql


def test_order_by_derived_expr_duplicating_output():
    rows, _ = _run(
        WINDOW_SELECT + "order by lower(channel) asc, item_id asc limit 100;"
    )
    assert [(r[0], r[1]) for r in rows] == [
        ("catalog", 101),
        ("store", 100),
        ("store", 102),
        ("web", 100),
        ("web", 101),
    ]


def test_order_by_output_alias_matches_derived_expr():
    derived, _ = _run(
        WINDOW_SELECT + "order by lower(channel) asc, item_id asc limit 100;"
    )
    alias, _ = _run(WINDOW_SELECT + "order by chan asc, item_id asc limit 100;")
    assert derived == alias


def test_order_by_inline_window_duplicating_output():
    rows, _ = _run(
        WINDOW_SELECT
        + """order by rank(item_id) over (partition by channel order by sum(quantity) asc) asc,
    chan asc limit 100;"""
    )
    assert [r[2] for r in rows] == sorted(r[2] for r in rows)


def test_order_by_raw_source_column_of_derived_output():
    # `channel` is only an alias source (not projected); the grouped final node
    # cannot reference it raw, so the renderer must aggregate-wrap it.
    rows, sql = _run(WINDOW_SELECT + "order by channel desc, item_id asc limit 100;")
    assert "MIN(" in sql
    assert [r[0] for r in rows] == ["web", "web", "store", "store", "catalog"]


def test_order_by_compound_aggregate_duplicating_output():
    # q80 case C: a compound of aggregates projected as an output must be
    # matchable from ORDER BY, not rejected as an unprojected inline aggregate
    rows, _ = _run(r"""
select
    channel,
    item_id,
    sum(quantity) as total,
    grouping(channel) + grouping(item_id) as lvl
by rollup (channel, item_id)
order by grouping(channel) + grouping(item_id) asc, channel asc nulls last, item_id asc nulls last
limit 100;""")
    assert [r[3] for r in rows] == sorted(r[3] for r in rows)
    assert rows[-1][3] == 2


def test_order_by_derived_expr_no_window_unchanged():
    rows, sql = _run(r"""
select
    lower(channel) as chan,
    sum(quantity) as total
order by lower(channel) asc
limit 100;""")
    assert "MIN(LOWER" not in sql
    assert [r[0] for r in rows] == ["catalog", "store", "web"]
