"""`by rollup (channel)` over two `union join`-ed rowsets, selecting a
normalized label that is NOT a rollup key — the q05 family.

Two defects, fixed together:
1. The scoped-join key exposure surfaced the coalescing group's members
   (`r_channel`, `r_entity`) on the ROLLUP CTE itself, so it projected bare,
   ungrouped columns next to `GROUP BY ROLLUP(...)` (BinderException).
2. With those keys gone the leaf join-back paired on the rollup key, and both
   sides being nullable made it INNER — silently dropping the grand-total row,
   whose key NULL is grouping-set padding and can never find a partner.
"""

import re

import pytest

from trilogy import Dialects

FIXTURE = """
key sale_id int;
property sale_id.s_channel string;
property sale_id.s_entity string;
property sale_id.amount float;

key ret_id int;
property ret_id.r_channel string;
property ret_id.r_entity string;
property ret_id.loss float;

datasource sale_rows (
    sale_id: sale_id, channel: s_channel, entity: s_entity, amount: amount,
)
grain (sale_id)
query '''
select 1 as sale_id, 'STORE' as channel, 's1' as entity, 10.0 as amount union all
select 2, 'STORE', 's2', 20.0 union all
select 3, 'CATALOG', 'c1', 30.0
''';

datasource return_rows (
    ret_id: ret_id, channel: r_channel, entity: r_entity, loss: loss,
)
grain (ret_id)
query '''
select 1 as ret_id, 'STORE' as channel, 's1' as entity, 1.0 as loss union all
select 2, 'CATALOG', 'c1', 3.0 union all
select 3, 'WEB', 'w9', 5.0
''';

with s as
select s_channel as channel, s_entity as entity, sum(amount) as sales;

with r as
select r_channel as channel, r_entity as entity, sum(loss) as returns;
"""

SELECTION = """
select
    case coalesce(s.channel, r.channel)
        when 'STORE' then 'store channel'
        when 'CATALOG' then 'catalog channel'
        else 'web channel'
    end as channel,
    concat(
        case coalesce(s.channel, r.channel)
            when 'STORE' then 'store'
            when 'CATALOG' then 'catalog_page'
            else 'web_site'
        end,
        coalesce(s.entity, r.entity)
    ) as entity,
    sum(coalesce(s.sales, 0)) as sales,
    sum(coalesce(r.returns, 0)) as returns
union join s.channel = r.channel
union join s.entity = r.entity
"""

CHANNEL_ROLLUP = SELECTION + "by rollup (channel)\norder by channel asc nulls last;"
LEAF_ROLLUP = (
    SELECTION + "by rollup (channel, entity)\n"
    "order by channel asc nulls last, entity asc nulls last;"
)

GROUP_BY_ROLLUP = "\nGROUP BY\n    ROLLUP ("


def rollup_cte_projections(sql: str) -> tuple[list[str], list[str]]:
    """The select items of the CTE that renders GROUP BY ROLLUP, and its keys."""
    assert sql.count(GROUP_BY_ROLLUP) == 1, "expected exactly one ROLLUP CTE"
    marker = sql.index(GROUP_BY_ROLLUP)
    body = sql[sql.index("SELECT\n", sql.rindex(" as (\n", 0, marker)) :]
    body = body[len("SELECT\n") : body.index("\nFROM\n")]
    # select items end with a comma at end of line; a CASE body spans lines
    items = [item.strip() for item in re.split(r",\n(?=    \S)", body)]
    keys_at = marker + len(GROUP_BY_ROLLUP)
    keys = [k.strip() for k in sql[keys_at : sql.index(")", keys_at)].split(",")]
    return items, keys


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(FIXTURE)
    return exec


def test_rollup_cte_projects_only_grouped_or_aggregated(executor):
    items, keys = rollup_cte_projections(executor.generate_sql(CHANNEL_ROLLUP)[0])
    ungrouped = [
        item
        for position, item in enumerate(items, start=1)
        if "sum(" not in item and str(position) not in keys
    ]
    assert not ungrouped, f"ungrouped projections in the rollup CTE: {ungrouped}"


def test_partial_rollup_over_union_joined_rowsets_executes(executor):
    rows = executor.execute_text(CHANNEL_ROLLUP)[0].fetchall()
    channels = [row[0] for row in rows]
    assert None in channels, "grand-total row missing"
    grand_total = [row for row in rows if row[0] is None]
    assert [(row[2], row[3]) for row in grand_total] == [(60.0, 9.0)]
    # every leaf row still carries its channel's rollup totals
    assert sorted((row[0], row[1]) for row in rows if row[0]) == [
        ("catalog channel", "catalog_pagec1"),
        ("store channel", "stores1"),
        ("store channel", "stores2"),
        ("web channel", "web_sitew9"),
    ]


NET_SELECTION = SELECTION.replace(
    "    sum(coalesce(r.returns, 0)) as returns\n",
    "    sum(coalesce(r.returns, 0)) as returns,\n"
    "    sum(coalesce(s.sales, 0)) - sum(coalesce(r.returns, 0)) as net\n",
)
NET_CHANNEL_ROLLUP = (
    NET_SELECTION + "by rollup (channel)\n"
    "order by channel asc nulls last, entity asc nulls last;"
)


def test_rollup_with_basic_over_aggregates_executes(executor):
    """A BASIC derived from the rollup's aggregates (`sum(...) - sum(...)`)
    must ride the rollup CTE, not re-join the raw rowset feeder: the feeder's
    pre-aggregation axis (`r.channel`/`r.entity`) no longer exists after the
    ROLLUP, so demanding it resurrects the feeder as a keyless (ON 1=1)
    parent — the q05 rollup-over-union-join shape."""
    rows = [
        tuple(row) for row in executor.execute_text(NET_CHANNEL_ROLLUP)[0].fetchall()
    ]
    leaves = [row for row in rows if row[0] is not None]
    totals = [row for row in rows if row[0] is None]
    assert leaves == [
        ("catalog channel", "catalog_pagec1", 30.0, 3.0, 27.0),
        ("store channel", "stores1", 30.0, 1.0, 29.0),
        ("store channel", "stores2", 30.0, 1.0, 29.0),
        ("web channel", "web_sitew9", 0.0, 5.0, -5.0),
    ]
    assert [(row[2], row[3], row[4]) for row in totals] == [(60.0, 9.0, 51.0)]


def test_full_grain_rollup_returns_leaf_subtotal_and_total(executor):
    rows = [tuple(row) for row in executor.execute_text(LEAF_ROLLUP)[0].fetchall()]
    leaves = [row for row in rows if row[0] is not None and row[1] is not None]
    subtotals = [row for row in rows if row[0] is not None and row[1] is None]
    totals = [row for row in rows if row[0] is None]
    # sales-only (s2) and returns-only (w9) entities both survive the union join
    assert leaves == [
        ("catalog channel", "catalog_pagec1", 30.0, 3.0),
        ("store channel", "stores1", 10.0, 1.0),
        ("store channel", "stores2", 20.0, 0.0),
        ("web channel", "web_sitew9", 0.0, 5.0),
    ]
    assert subtotals == [
        ("catalog channel", None, 30.0, 3.0),
        ("store channel", None, 30.0, 1.0),
        ("web channel", None, 0.0, 5.0),
    ]
    assert totals == [(None, None, 60.0, 9.0)]
