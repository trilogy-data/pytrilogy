"""Dimension extent of a two-fact select under nullable fact FKs.

See docs/handoff_multi_fact_nullable_fk_extent.md. The settled contract:

- A required FK is an EQUAL-domain claim, so the merge of the two fact
  aggregates narrows to INNER: only members both facts cover survive.
- A `?` FK weakens that side's claim to "some subset, plus a NULL group".
  The grain-aligned merge then preserves BOTH sides padded: the union of
  the two facts' members, NULL in the aggregate a side does not cover.
  Padding NULLs are not values, so the preserved rows never null-pair
  across sides, and the result is symmetric in the two facts.

`visits` covers alpha/beta/gamma, `events` covers alpha/beta/delta, so each
fact has exactly one exclusive member.
"""

import pathlib
import tempfile

import pytest

from trilogy import Dialects, Environment

GROUPS = """select 1 as gid, 'alpha' as name
union all select 2 as gid, 'beta' as name
union all select 3 as gid, 'gamma' as name
union all select 4 as gid, 'delta' as name"""
VISITS = """select 1 as id, 1 as gid, 5 as amount
union all select 2 as id, 2 as gid, 7 as amount
union all select 3 as id, 3 as gid, 11 as amount"""
EVENTS = """select 1 as id, 1 as gid, 100 as amount
union all select 2 as id, 2 as gid, 200 as amount
union all select 3 as id, 4 as gid, 400 as amount"""

QUERY = "select gname, sum(vamt) as v, sum(eamt) as e order by gname asc;"

UNION_ROWS = {
    ("alpha", 5, 100),
    ("beta", 7, 200),
    ("gamma", 11, None),
    ("delta", None, 400),
}


def _model(visit_fk: str, event_fk: str) -> str:
    return f"""key gid int;
property gid.gname string;
datasource groups (gid: gid, name: gname) grain (gid) query '''{GROUPS}''';

key vid int;
property vid.vamt int;
datasource visits (id: vid, gid: {visit_fk}, amount: vamt) grain (vid) query '''{VISITS}''';

key eid int;
property eid.eamt int;
datasource events (id: eid, gid: {event_fk}, amount: eamt) grain (eid) query '''{EVENTS}''';
"""


def _rows(visit_fk: str, event_fk: str, query: str = QUERY) -> set[tuple]:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tempfile.mkdtemp())
    )
    executor.execute_text(_model(visit_fk, event_fk))
    return set(executor.execute_text(query)[-1].fetchall())


def test_required_fks_narrow_to_shared_members():
    """Both facts claim the full gid domain; INNER keeps the shared members."""
    assert _rows("gid", "gid") == {("alpha", 5, 100), ("beta", 7, 200)}


@pytest.mark.parametrize(
    "visit_fk,event_fk",
    [("?gid", "gid"), ("gid", "?gid"), ("?gid", "?gid")],
)
def test_nullable_fk_pads_the_union_of_members(visit_fk: str, event_fk: str):
    assert _rows(visit_fk, event_fk) == UNION_ROWS


THREE_FACT_MODEL = f"""key gid int;
property gid.gname string;
datasource groups (gid: gid, name: gname) grain (gid) query '''{GROUPS}''';

key vid int;
property vid.vamt int;
datasource visits (id: vid, gid: ?gid, amount: vamt) grain (vid) query '''
select 1 as id, 1 as gid, 5 as amount
union all select 2 as id, 3 as gid, 11 as amount
union all select 3 as id, null as gid, 99 as amount''';

key eid int;
property eid.eamt int;
datasource events (id: eid, gid: gid, amount: eamt) grain (eid) query '''
select 1 as id, 1 as gid, 100 as amount
union all select 2 as id, 2 as gid, 200 as amount''';

key wid int;
property wid.wamt int;
datasource web (id: wid, gid: ?gid, amount: wamt) grain (wid) query '''
select 1 as id, 2 as gid, 1000 as amount
union all select 2 as id, null as gid, 3000 as amount''';
"""


def test_three_fact_chain_coalesces_the_merge_axis():
    """Chained padded merges must join later sides on the COALESCE of every
    prior side's axis: after one FULL, the axis is NULL on rows exclusive to
    the other side, and a single-source ON splits a member's aggregates
    across two output rows. NULL-key rows (a value) pair across the `?`
    sides; padded members never pair."""
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tempfile.mkdtemp())
    )
    executor.execute_text(THREE_FACT_MODEL)
    query = (
        "select gname, sum(vamt) as v, sum(eamt) as e, sum(wamt) as w "
        "order by gname asc nulls first;"
    )
    assert set(executor.execute_text(query)[-1].fetchall()) == {
        (None, 99, None, 3000),
        ("alpha", 5, 100, None),
        ("beta", None, 200, 1000),
        ("gamma", 11, None, None),
    }


def test_value_nullable_attribute_does_not_degrade_solid_key_joins():
    """The tpc-ds q98 shape: a `?` dim attribute (item.class) rides
    everywhere the solid item key goes — the null-safe ratio merge grouped
    by it, and the post-aggregation rejoin to the item scan. The `?` weakens
    only the attribute's own claim; every join still pairs totally on the
    key, so the plan must stay free of outer joins. Row results cannot see a
    regression here (the padded rows never exist), so assert the SQL."""
    qdir = pathlib.Path(__file__).parents[1] / "modeling" / "tpc_ds_duckdb"
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=str(qdir))
    )
    sql = executor.generate_sql((qdir / "query98.preql").read_text())[-1]
    assert "is not distinct from" in sql, sql
    assert "LEFT OUTER JOIN" not in sql, sql
    assert "RIGHT OUTER JOIN" not in sql, sql
    assert "FULL" not in sql, sql


def test_single_fact_extent_is_that_facts_members():
    assert _rows("?gid", "gid", "select gname, sum(vamt) as v;") == {
        ("alpha", 5),
        ("beta", 7),
        ("gamma", 11),
    }
    assert _rows("?gid", "gid", "select gname, sum(eamt) as e;") == {
        ("alpha", 100),
        ("beta", 200),
        ("delta", 400),
    }
