"""A WHERE on a base concept reaches consumers of a ROWSET handle.

`WHERE filters data BEFORE it reaches aggregates` (trilogy/ai/constants.py), so a
selected aggregate's inputs are filtered. That holds when the aggregate reads the
base concept, and breaks when the same value arrives through a rowset handle: the
filter is emitted in a sibling CTE joined `ON 1=1`, which gates nothing.
"""

from decimal import Decimal

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException

MODEL = r"""
key oid int;
property oid.amt numeric(15,2);
property oid.cat string;
key uid int;
property uid.uname string;

datasource orders (
    oid: oid,
    amt: amt,
    cat: cat)
grain (oid)
query '''
select 1 as oid, cast(1.5 as numeric(15,2)) as amt, 'a' as cat
union all select 2, cast(0.5 as numeric(15,2)), 'a'
union all select 3, cast(4.0 as numeric(15,2)), 'b'
''';

datasource users (
    uid: uid,
    uname: uname)
grain (uid)
query '''select 7 as uid, 'zed' as uname''';
"""

FILTERED_TOTAL = Decimal("2.00")


def _total(query: str) -> Decimal:
    exec = Dialects.DUCK_DB.default_executor()
    return exec.execute_query(MODEL + "\n" + query).fetchall()[0][0]


def test_base_aggregate_filter_control():
    assert _total("select sum(amt) -> t where cat = 'a';") == FILTERED_TOTAL


def test_rowset_handle_aggregate_filter_applies():
    assert (
        _total("with rs as select oid, amt;\nselect sum(rs.amt) -> t where cat = 'a';")
        == FILTERED_TOTAL
    )


def test_rowset_handle_filter_applies():
    assert (
        _total(
            "with rs as select oid, amt, cat;\n"
            "select sum(rs.amt) -> t where rs.cat = 'a';"
        )
        == FILTERED_TOTAL
    )


def test_filtered_rowset_body_intersects_with_base_filter():
    """The rowset keeps oids 1 and 3; `cat = 'a'` keeps 1 and 2. Only the
    intersection may reach the aggregate, so the merge feeding it must not
    preserve either side."""
    assert (
        _total(
            "with rs as select oid, amt where amt > 1;\n"
            "select count(rs.oid) -> c where cat = 'a';"
        )
        == 1
    )


def test_grouped_rowset_handle_aggregate_filter_applies():
    exec = Dialects.DUCK_DB.default_executor()
    rows = exec.execute_query(
        MODEL + "\nwith rs as select oid, amt;\n"
        "select rs.oid, sum(rs.amt) -> t where cat = 'a' order by rs.oid asc;"
    ).fetchall()
    assert [tuple(r) for r in rows] == [
        (1, Decimal("1.50")),
        (2, Decimal("0.50")),
    ]


def _rows(query: str) -> list[tuple]:
    exec = Dialects.DUCK_DB.default_executor()
    return [tuple(r) for r in exec.execute_query(MODEL + "\n" + query).fetchall()]


ROW_LEVEL = "with rs as select oid, amt;\n"


def test_row_level_select_is_filtered_by_a_base_concept():
    """`select cat, rs.oid` already joins on the boundary's base key, so
    filtering on `cat` must use that same relation rather than raising."""
    assert _rows(
        ROW_LEVEL + "select rs.oid, rs.amt where cat = 'a' order by rs.oid asc;"
    ) == [(1, Decimal("1.50")), (2, Decimal("0.50"))]


def test_row_level_select_of_a_base_concept_control():
    assert _rows(
        ROW_LEVEL + "select cat, rs.oid where cat = 'a' order by rs.oid asc;"
    ) == [("a", 1), ("a", 2)]


def test_a_rowset_off_the_base_key_stays_an_island():
    """A boundary whose grain is not a base key has no relation to an unrelated
    concept, and must still be rejected."""
    with pytest.raises(DisconnectedConceptsException):
        _rows(
            "with agg_rs as select cat, sum(amt) -> cat_total;\n"
            "select agg_rs.cat, agg_rs.cat_total where uname = 'zed';"
        )
