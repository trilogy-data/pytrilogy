from trilogy import Environment


def test_stdlib():
    env, _ = Environment().parse(
        """
import std.money;
                        
key order int;
property order.amount numeric(16,2)::std.money.usd;
                        
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

    assert "std.money.usd" in env.concepts["amount"].datatype.traits, (
        "std.money.usd" in env.concepts["amount"].datatype.traits
    )
