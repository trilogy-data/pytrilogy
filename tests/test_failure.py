from pytest import raises

from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException


def test_cannot_find():
    x = Dialects.DUCK_DB.default_executor()

    with raises(UnresolvableQueryException):
        x.generate_sql(
            """
    key x int;
    key y int;

    datasource fun (
    y: y,
        )
    address abc;


    select x;

    """
        )


def test_cannot_find_complex():
    x = Dialects.DUCK_DB.default_executor()
    with raises(UnresolvableQueryException):
        x.generate_sql(
            """
    key x int;
    key y int;

    auto sum <- x+y;

    datasource fun (
    y: y,
        )
    address abc;


    select sum(y) by x as fun;

    """
        )
