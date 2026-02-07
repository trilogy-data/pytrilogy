from pytest import raises

from trilogy import Environment
from trilogy.parsing.exceptions import ParseError


def test_stdlib():
    env, _ = Environment().parse("""
import std.money;
                        
key order int;
property order.amount numeric(16,2)::usd;
property order.amount float::usd;                   
datasource orders (
    order:order,
    amount:amount)
query '''
SELECT
    1 AS order,
    2.0 as amount,
    union all
    select 1 as order,
    2.0 as amount
    ''';
""")

    assert "usd" in env.concepts["amount"].datatype.traits, (
        "usd" in env.concepts["amount"].datatype.traits
    )


def test_stdlib_failure():
    with raises(ParseError):
        Environment().parse("""
    import std.money;
                            
    key order int;
    property order.amount string::usd;                   
    datasource orders (
        order:order,
        amount:amount)
    query '''
    SELECT
        1 AS order,
        2.0 as amount,
        union all
        select 1 as order,
        2.0 as amount
        ''';

    """)
