"""A WHERE on a base concept reaches consumers of a ROWSET handle through a
declared relation.

`WHERE filters data BEFORE it reaches aggregates` (trilogy/ai/constants.py), so a
selected aggregate's inputs are filtered. A rowset is a materialized result:
its outputs pair with a base concept only through `subset join rs.key = key`
(or by projecting the concept inside the rowset and filtering on the handle).
Without either the statement is disconnected and says which join it wants;
planning it as a sibling CTE joined `ON 1=1` would gate nothing.
"""

from decimal import Decimal

import pytest

from tests.helpers.rows import executor_for, fetch_rows
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
JOIN = "subset join rs.oid = oid"


def _rows(query: str) -> list[tuple]:
    return fetch_rows(executor_for(MODEL), query)


def _total(query: str) -> Decimal:
    return _rows(query)[0][0]


def test_base_aggregate_filter_control():
    assert _total("select sum(amt) -> t where cat = 'a';") == FILTERED_TOTAL


def test_rowset_handle_aggregate_filter_applies():
    assert (
        _total(
            f"with rs as select oid, amt;\nselect sum(rs.amt) -> t {JOIN} where cat = 'a';"
        )
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
    """The rowset keeps oids 1 and 3; `cat = 'a'` keeps 1 and 2. The declared
    key is one axis read from the superset side, so the padded oid 2 carries a
    NULL value: counting the value counts the intersection, counting the key
    counts the superset's filtered rows."""
    assert _rows(
        "with rs as select oid, amt where amt > 1;\n"
        f"select count(rs.amt) -> c, count(rs.oid) -> k {JOIN} where cat = 'a';"
    ) == [(1, 2)]


def test_grouped_rowset_handle_aggregate_filter_applies():
    assert _rows(
        "with rs as select oid, amt;\n"
        f"select rs.oid, sum(rs.amt) -> t {JOIN} where cat = 'a' order by rs.oid asc;"
    ) == [(1, Decimal("1.50")), (2, Decimal("0.50"))]


ROW_LEVEL = "with rs as select oid, amt;\n"


def test_row_level_select_is_filtered_by_a_base_concept():
    assert _rows(
        ROW_LEVEL + f"select rs.oid, rs.amt {JOIN} where cat = 'a' order by rs.oid asc;"
    ) == [(1, Decimal("1.50")), (2, Decimal("0.50"))]


def test_row_level_select_of_a_base_concept_control():
    assert _rows(
        ROW_LEVEL + f"select cat, rs.oid {JOIN} where cat = 'a' order by rs.oid asc;"
    ) == [("a", 1), ("a", 2)]


@pytest.mark.parametrize(
    "query",
    [
        ROW_LEVEL + "select sum(rs.amt) -> t where cat = 'a';",
        ROW_LEVEL + "select rs.oid, sum(rs.amt) -> t where cat = 'a';",
        ROW_LEVEL + "select rs.oid, rs.amt where cat = 'a';",
        ROW_LEVEL + "select cat, rs.oid where cat = 'a';",
    ],
)
def test_undeclared_pairing_names_the_join(query: str):
    """A base concept beside a rowset output with no declared relation is a
    disconnect, and the error names the join that would relate them."""
    with pytest.raises(DisconnectedConceptsException) as exc:
        _rows(query)
    assert "`subset join rs.oid = oid`" in str(exc.value)


def test_a_rowset_off_the_base_key_stays_an_island():
    """A boundary whose grain is not a base key has no relation to an unrelated
    concept, and must still be rejected, with no join to suggest."""
    with pytest.raises(DisconnectedConceptsException) as exc:
        _rows(
            "with agg_rs as select cat, sum(amt) -> cat_total;\n"
            "select agg_rs.cat, agg_rs.cat_total where uname = 'zed';"
        )
    assert "subset join" not in str(exc.value)
