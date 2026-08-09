"""Regression lock for the composite-key dimension peel
(`_composite_determining_grain`, used by `_split_root_dimension_clusters`).

A property functionally determined by a MULTI-key grouping grain -- and by no
single entity key -- used to stay on the fact row bucket and be deduped back to
grain in a sibling GROUP node. That gave the row scan two consumers, which
blocks `CollapseSingleParent` and forces the aggregate into its own CTE (TPC-H
q20's `wakeful` / `cooperative` / `cheerful` split, 5 CTEs where 2 suffice).
The property must instead source from its own table keyed by the composite
grain. The aggregate is conditional here because that is what puts a row-grain
FILTER bucket between the scan and the aggregate -- without it the peel is
unnecessary and the split never appears."""

import re

from trilogy import Dialects, Environment

_MODEL = """
key part_id int;
key supplier_id int;
property <part_id,supplier_id>.avail_qty int;

key line_id int;
property line_id.qty int;
property line_id.ship_year int;

property supplier_id.supplier_name string;

datasource partsupp (
    p: part_id,
    s: supplier_id,
    aq: avail_qty,
)
grain (part_id, supplier_id)
query '''
select 1 p, 1 s, 100 aq union all
select 1 p, 2 s, 5 aq union all
select 2 p, 1 s, 50 aq
''';

datasource lineitem (
    lid: line_id,
    p: part_id,
    s: supplier_id,
    q: qty,
    y: ship_year,
)
grain (line_id)
query '''
select 1 lid, 1 p, 1 s, 10 q, 1994 y union all
select 2 lid, 1 p, 1 s, 20 q, 1994 y union all
select 3 lid, 1 p, 2 s, 40 q, 1994 y union all
select 4 lid, 2 p, 1 s, 5 q, 1993 y
''';

datasource suppliers (
    s: supplier_id,
    sn: supplier_name,
)
grain (supplier_id)
query '''
select 1 s, 'alpha' sn union all select 2 s, 'beta' sn
''';

auto qty_total <- sum(qty ? ship_year = 1994) by part_id, supplier_id;
"""

_QUERY = """
where avail_qty > qty_total
select supplier_name
order by supplier_name asc;
"""

_CTE = re.compile(r"\n(\w+) as \(\n")
_AGG = re.compile(r"\b(sum|count|min|max|avg)\s*\(", re.IGNORECASE)


def _cte_bodies(sql: str) -> list[tuple[str, str]]:
    """(name, body) per CTE, delimited by paren depth so the trailing final
    SELECT is not swallowed into the last CTE."""
    out: list[tuple[str, str]] = []
    for match in _CTE.finditer("\n" + sql):
        start = match.end()
        depth = 1
        cursor = start
        text = "\n" + sql
        while cursor < len(text) and depth:
            depth += (text[cursor] == "(") - (text[cursor] == ")")
            cursor += 1
        out.append((match.group(1), text[start : cursor - 1]))
    return out


def _dedup_group_ctes(sql: str) -> list[str]:
    """CTE names whose GROUP BY carries no aggregate -- a pure dedup bucket."""
    return [
        name
        for name, body in _cte_bodies(sql)
        if "\nGROUP BY" in body.upper() and not _AGG.search(body)
    ]


def test_composite_key_property_sources_from_its_own_table():
    env = Environment()
    env, _ = env.parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(_QUERY)[-1]
    rows = executor.execute_text(_QUERY)[-1].fetchall()
    assert [tuple(r) for r in rows] == [("alpha",)]
    assert _dedup_group_ctes(sql) == [], sql
