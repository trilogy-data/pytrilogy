"""Composite-key `union join` presence probes hold the full authored join grain.

`union join ss.ticket = sr.ticket and ss.item = sr.item` between two facts at
grain (ticket, item) declares the relation at the compound grain. The presence
probe minted for `sr.ticket is [not] null` is pinned to sr's own datasource
(`gen_presence_probe_node`); pre-fix that scan carried only the probed group's
key, grouping the returns fact to DISTINCT tickets and re-joining on ticket
alone — "did this (ticket, item) match?" silently coarsened to "did this
ticket match ANY row?", admitting sales whose ticket matched on a different
item (TPC-DS q64 is_returned: reference 2 rows, candidate 247). The pinned
scan now also carries every co-declared statement join-key group with a member
bound in the same datasource (`co_declared_group_keys`), so the probe joins at
the authored compound grain. Global `merge` groups stay excluded: ambient
identity is not a join constraint of the statement.

The two facts import the same item model separately (the conformed-dimension
shape of the TPC-DS workspace), so the item keys are distinct addresses that
only the statement's join relates.
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

ITEM = """
key item_id int;
property item_id.iname string;
datasource items (i: item_id, n: iname) grain (item_id)
query '''
select 10 i, 'I10' n union all select 20 i, 'I20' n union all select 30 i, 'I30' n
''';
"""

SALES = """
import item as item;
key ticket int;
properties <ticket, item.item_id> (amt int?,);
datasource sales (t: ticket, i: item.item_id, a: amt) grain (ticket, item.item_id)
query '''
select 1 t, 10 i, 100 a union all select 1 t, 20 i, 200 a union all select 2 t, 10 i, 300 a
''';
"""

RETURNS = """
import item as item;
key ticket int;
properties <ticket, item.item_id> (fee int?,);
datasource returns (t: ticket, i: item.item_id, f: fee) grain (ticket, item.item_id)
query '''
select 1 t, 10 i, 5 f union all select 3 t, 30 i, 7 f
''';
"""

HEADER = "import sales as ss;\nimport returns as sr;\n"
JOIN = "union join ss.ticket = sr.ticket and ss.item.item_id = sr.item.item_id\n"


def _run(tmp_path: Path, query: str) -> list[tuple]:
    (tmp_path / "item.preql").write_text(ITEM)
    (tmp_path / "sales.preql").write_text(SALES)
    (tmp_path / "returns.preql").write_text(RETURNS)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(HEADER + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


def test_presence_holds_compound_grain(tmp_path: Path):
    # Only (ticket 1, item 10) was returned. Ticket 1 item 20 must NOT be
    # admitted by ticket 1's other-item return; sr-only (3, 30) is preserved
    # by the union with its item readable off the coalesced axis.
    rows = _run(
        tmp_path,
        "select ss.ticket, ss.item.item_id, ss.amt\n"
        + JOIN
        + "where sr.ticket is not null;",
    )
    assert rows == sort_rows([(1, 10, 100), (3, 30, None)])


def test_absence_is_compound_complement(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select ss.ticket, ss.item.item_id, ss.amt\n"
        + JOIN
        + "where sr.ticket is null;",
    )
    assert rows == sort_rows([(1, 20, 200), (2, 10, 300)])


def test_presence_without_projected_item(tmp_path: Path):
    # The q64 ss_base shape: the compound key constrains the probe even when
    # the co-key is not projected.
    rows = _run(
        tmp_path,
        "select ss.ticket, ss.amt\n" + JOIN + "where sr.ticket is not null;",
    )
    assert rows == sort_rows([(1, 100), (3, None)])


def test_union_no_condition_both_sides(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select ss.ticket, ss.item.item_id, ss.amt, sr.fee\n" + JOIN + ";",
    )
    assert rows == sort_rows(
        [(1, 10, 100, 5), (1, 20, 200, None), (2, 10, 300, None), (3, 30, None, 7)]
    )
