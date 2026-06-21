import pytest
from pytest import raises

from trilogy import Dialects
from trilogy.core.exceptions import InvalidSyntaxException, UnresolvableQueryException


def test_cannot_find():
    x = Dialects.DUCK_DB.default_executor()

    with raises(UnresolvableQueryException):
        x.generate_sql("""
    key x int;
    key y int;

    datasource fun (
    y: y,
        )
    address abc;


    select x;

    """)


def test_cannot_find_complex():
    x = Dialects.DUCK_DB.default_executor()
    with raises(UnresolvableQueryException):
        x.generate_sql("""
    key x int;
    key y int;

    auto sum <- x+y;

    datasource fun (
    y: y,
        )
    address abc;


    select sum(y) by x as fun;

    """)


@pytest.mark.parametrize("merge_modifier", ["", "~"])
def test_merged_in_subselect_collapses(merge_modifier):
    """A merge makes two keys one concept, so `b in a` on the merged key
    collapses to a self-comparison. The engine can't filter one model by another
    here (and compiling emits a self-referential subselect against a CTE not in
    scope -> opaque DB error), so raise a clear error consistently — for both a
    full (synonym) merge and a partial (`~`) merge."""
    x = Dialects.DUCK_DB.default_executor()
    with raises(InvalidSyntaxException, match="compares a concept to itself"):
        x.generate_sql(f"""
    key a int;
    key b int;
    property b.bval int;

    datasource sa (a: a) grain (a)
    query '''select 1 as a''';
    datasource sb (b: b, bval: bval) grain (b)
    query '''select 1 as b, 20 as bval''';

    merge a into {merge_modifier}b;

    select bval where b in a;
    """)
