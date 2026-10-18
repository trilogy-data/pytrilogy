"""Resolving a select's anchors through a wide rowset walks each column once.

A rowset property's grain lists every sibling column, so a per-path walk over
them was factorial in the rowset's width: a 14-column rowset read through a
3-key union join planned for minutes.
"""

from unittest import mock

from trilogy import Dialects, Environment
from trilogy.core import grain_pin

MODEL = """
key ticket int;
key item int;
key cust int;
key store int;
property store.store_name string;
{props}
datasource sales (t: ticket, i: item, c: cust, s: store)
grain (ticket, item)
query '''select 1 t, 1 i, 1 c, 1 s''';

datasource stores (s: store, n: store_name{cols})
grain (store)
query '''select 1 s, 'a' n{vals}''';

key r_ticket int;
key r_item int;
key r_cust int;
datasource returns (t: r_ticket, i: r_item, c: r_cust)
grain (r_ticket, r_item)
query '''select 1 t, 1 i, 1 c''';
"""

QUERY = """
rowset sale_lines <- select ticket, item, cust, store_name{selected};
rowset return_lines <- select r_ticket as ticket, r_item as item, r_cust as cust;
where return_lines.ticket is not null
select sale_lines.store_name, count(return_lines.ticket) as n
union join sale_lines.ticket = return_lines.ticket
and sale_lines.item = return_lines.item
and sale_lines.cust = return_lines.cust;
"""


def _walks(width: int) -> int:
    names = [f"p{i}" for i in range(width)]
    model = MODEL.format(
        props="\n".join(f"property store.{n} string;" for n in names),
        cols="".join(f", {n}: {n}" for n in names),
        vals="".join(f", 'x' {n}" for n in names),
    )
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = QUERY.format(selected="".join(f", {n}" for n in names))
    with mock.patch.object(
        grain_pin, "_walk_entity_keys", wraps=grain_pin._walk_entity_keys
    ) as walk:
        assert executor.execute_query(query).fetchall() == [("a", 1)]
    return walk.call_count


def test_anchor_walk_is_linear_in_rowset_width():
    narrow = _walks(2)
    wide = _walks(10)
    assert wide <= narrow * 6
