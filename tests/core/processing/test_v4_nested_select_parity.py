"""Parity locks for the three nested-select sourcing paths in v4 discovery.

`resolve_rowset`, `gen_multiselect` and `gen_union_select` each plan a
nested select through `search_concepts`, but each was written from the same
template by hand and drifted: a per-arm HAVING is dropped by the union path, a
body LIMIT is dropped by both arm paths, and neither arm path gates
connectivity, so a disconnected arm silently cross-joins instead of raising.
The rowset path handles all three and serves as the control.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import DisconnectedConceptsException

_MODEL = """
key line_id int;
property line_id.item_id int;
property line_id.cat string?;
property line_id.yr int;
auto cat_b <- cat;

datasource lines (
    line_id: line_id,
    item_id: item_id,
    cat: cat,
    yr: yr,
)
grain (line_id)
query '''
select 1 as line_id, 1 as item_id, 'a' as cat, 2001 as yr union all
select 2 as line_id, 1 as item_id, 'a' as cat, 2001 as yr union all
select 3 as line_id, 2 as item_id, cast(null as varchar) as cat, 2001 as yr union all
select 4 as line_id, 3 as item_id, 'b' as cat, 2001 as yr union all
select 5 as line_id, 1 as item_id, 'a' as cat, 2002 as yr union all
select 6 as line_id, 2 as item_id, cast(null as varchar) as cat, 2002 as yr union all
select 7 as line_id, 4 as item_id, 'c' as cat, 2003 as yr union all
select 8 as line_id, 1 as item_id, 'a' as cat, 2011 as yr union all
select 9 as line_id, 2 as item_id, 'b' as cat, 2011 as yr union all
select 10 as line_id, 1 as item_id, 'b' as cat, 2012 as yr union all
select 11 as line_id, 2 as item_id, 'a' as cat, 2012 as yr
''';

key other_id int;
property other_id.oname string;
datasource others (
    other_id: other_id,
    oname: oname,
)
grain (other_id)
query '''select 1 as other_id, 'x' as oname union all select 2 as other_id, 'y' as oname''';
"""


@pytest.fixture
def executor():
    env = Environment()
    env, _ = env.parse(_MODEL)
    yield Dialects.DUCK_DB.default_executor(environment=env)


def _rows(executor, query: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]


def test_union_arm_having_applies(executor):
    """Arm one keeps only years with more than 2 lines (2001); arm two is
    unfiltered over cat='a'. Dropping the HAVING leaks 2002/2003/2011/2012."""
    assert (
        _rows(
            executor,
            """
with combined as union(
    (select yr, count(line_id) -> c having c > 2),
    (where cat = 'a' select yr, count(line_id) -> c)
) -> (y, c);
select combined.y, combined.c order by combined.y asc, combined.c asc;
""",
        )
        == [(2001, 2), (2001, 4), (2002, 1), (2011, 1), (2012, 1)]
    )


def test_union_arm_limit_applies(executor):
    """Arm one is limited to the 2 latest years; arm two contributes the 4
    years having cat='a'. Dropping the LIMIT yields all 5 years from arm one."""
    assert (
        _rows(
            executor,
            """
with combined as union(
    (select yr order by yr desc limit 2),
    (where cat = 'a' select yr)
) -> (y);
select combined.y order by combined.y asc;
""",
        )
        == [(2001,), (2002,), (2011,), (2011,), (2012,), (2012,)]
    )


def test_multiselect_arm_limit_applies(executor):
    """Arm A keeps only its top category by line count; the other categories
    still surface through the FULL align join with NULL arm-A columns."""
    assert (
        _rows(
            executor,
            """
select cat, count(line_id) as lines order by lines desc limit 1
merge
select cat_b, count(item_id) as items
align k: cat, cat_b
order by k asc;
""",
        )
        == [
            ("a", "a", 5, "a", 5),
            ("b", None, None, "b", 3),
            ("c", None, None, "c", 1),
            (None, None, None, None, 2),
        ]
    )


def test_union_arm_disconnected_raises(executor):
    """`yr` and `oname` share no join path, so the arm would cross-join."""
    with pytest.raises(DisconnectedConceptsException):
        _rows(
            executor,
            """
with combined as union(
    (select yr, oname),
    (select yr, oname)
) -> (y, n);
select combined.y, combined.n;
""",
        )


def test_multiselect_arm_disconnected_raises(executor):
    with pytest.raises(DisconnectedConceptsException):
        _rows(
            executor,
            """
select cat, oname
merge
select cat_b, count(line_id) as lines
align k: cat, cat_b
order by k asc;
""",
        )


def test_rowset_control_having_limit_and_disconnect(executor):
    """The rowset path applies HAVING and LIMIT; its disconnect gate is reached
    but self-bridged, so it never fires."""
    assert _rows(
        executor,
        "with rs as select yr, count(line_id) -> c having c > 2;"
        "\nselect rs.yr, rs.c order by rs.yr asc;",
    ) == [(2001, 4)]
    assert _rows(
        executor,
        "with rs as select yr order by yr desc limit 2;"
        "\nselect rs.yr order by rs.yr asc;",
    ) == [(2011,), (2012,)]
    with pytest.raises(DisconnectedConceptsException):
        _rows(
            executor,
            "with rs as select yr, oname;\nselect rs.yr, rs.oname;",
        )
