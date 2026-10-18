"""Resolving a select's anchors through a wide rowset walks each column once.

A rowset property's grain lists every sibling column, so a per-path walk over
them was factorial in the rowset's width: a 14-column rowset read through a
3-key union join planned for minutes.
"""

from unittest import mock

from trilogy import Dialects, Environment
from trilogy.core import grain_pin
from trilogy.core.domain_graph import DomainGraph

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


def _environment(names: list[str]) -> Environment:
    env = Environment()
    env.parse(
        MODEL.format(
            props="\n".join(f"property store.{n} string;" for n in names),
            cols="".join(f", {n}: {n}" for n in names),
            vals="".join(f", 'x' {n}" for n in names),
        )
    )
    return env


def _walks(width: int) -> int:
    names = [f"p{i}" for i in range(width)]
    executor = Dialects.DUCK_DB.default_executor(environment=_environment(names))
    query = QUERY.format(selected="".join(f", {n}" for n in names))
    with mock.patch.object(
        grain_pin, "_entity_reads", wraps=grain_pin._entity_reads
    ) as walk:
        assert executor.execute_query(query).fetchall() == [("a", 1)]
    return walk.call_count


def test_anchor_walk_is_linear_in_rowset_width():
    narrow = _walks(2)
    wide = _walks(10)
    assert wide <= narrow * 6


def test_keyless_rowset_column_is_identified_by_the_whole_rowset():
    names = ["p0", "p1", "p2"]
    env = _environment(names)
    env.parse("rowset rs <- select store_name, p0, p1, p2;")
    columns = {f"rs.{n}" for n in ["store_name", *names]}
    for address in columns:
        keys = grain_pin._entity_keys(env.concepts[address], {}, env, DomainGraph())
        assert keys == columns
