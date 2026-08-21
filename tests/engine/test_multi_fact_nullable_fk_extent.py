"""A nullable fact FK changes which dimension members a two-fact select returns.

See docs/handoff_multi_fact_nullable_fk_extent.md. `visits` and `events` each
own one exclusive group, so a symmetric contract has to treat them alike:
either both exclusive members appear or neither does. Declaring `visits.gid`
nullable preserves the visits-exclusive member and not the events-exclusive
one, so the same query answers differently based on a NULL declaration.

The xfails are strict: whichever extent the fix settles on, they start passing
and say so.
"""

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


def _model(visit_fk: str) -> str:
    return f"""key gid int;
property gid.gname string;
datasource groups (gid: gid, name: gname) grain (gid) query '''{GROUPS}''';

key vid int;
property vid.vamt int;
datasource visits (id: vid, gid: {visit_fk}, amount: vamt) grain (vid) query '''{VISITS}''';

key eid int;
property eid.eamt int;
datasource events (id: eid, gid: gid, amount: eamt) grain (eid) query '''{EVENTS}''';
"""


def _groups_returned(visit_fk: str) -> set[str]:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tempfile.mkdtemp())
    )
    executor.execute_text(_model(visit_fk))
    return {row[0] for row in executor.execute_text(QUERY)[-1].fetchall()}


def test_required_fk_drops_both_exclusive_members():
    """The consistent half: an INNER over both facts, symmetric on both sides."""
    assert _groups_returned("gid") == {"alpha", "beta"}


@pytest.mark.xfail(
    strict=True,
    reason="nullable FK preserves the visits-exclusive member only; "
    "docs/handoff_multi_fact_nullable_fk_extent.md",
)
def test_nullable_fk_extent_matches_required_fk_extent():
    assert _groups_returned("?gid") == _groups_returned("gid")


@pytest.mark.xfail(
    strict=True,
    reason="delta is dropped while gamma survives; "
    "docs/handoff_multi_fact_nullable_fk_extent.md",
)
def test_nullable_fk_extent_is_symmetric_across_the_two_facts():
    returned = _groups_returned("?gid")
    assert ("gamma" in returned) == ("delta" in returned)
