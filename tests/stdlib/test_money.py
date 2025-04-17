from trilogy import Environment


def test_base_numeric():
    env, _ = Environment().parse(
        """
import std.money;
                        
key order int;
property order.amount numeric::usd;
property order.float_amount float::usd;
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
"""
    )

    assert "usd" in env.concepts["amount"].datatype.traits, (
        "usd" in env.concepts["amount"].datatype.traits
    )
