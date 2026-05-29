from decimal import Decimal
from pathlib import Path

from pytest import raises

from trilogy import Dialects

FILE = Path(__file__)


def test_invalid_typing():
    env = Dialects.DUCK_DB.default_executor()
    with raises(TypeError):
        env.environment.parse("""
    key customer_id int;
    property customer_id.email string::email;


    """)


def test_typing():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse("""

type email string;

key customer_id int;
property customer_id.email string::email;

def is_valid_email(email) -> contains(email, '@');

datasource customers (
    id:customer_id,
    email: email
)
grain (customer_id)
query '''
select 1 as id, 'bright@gmail.com' as email
union all
select 2 as id, 'funky@hotmail.com' as email
''';


""")

    results = env.execute_query("""SELECT
customer_id,
@is_valid_email(email)->valid;""")

    for row in results.fetchall():
        assert row.valid is True

    assert "email" in env.environment.data_types

    assert env.environment.concepts["email"].datatype.traits == ["email"]


def test_type_import_and_cast():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.add_file_import(FILE.parent / "test_env_types.preql", "dtypes")
    env.environment.parse("""

type year int;
key customer_id int;
property customer_id.signup_date date;

datasource customers (
    id:customer_id,
    signup_date: signup_date
)
grain (customer_id)
query '''
select 1 as id, cast('2023-01-01' as date) as signup_date
union all
select 2 as id, cast('2023-01-02' as date) as signup_date
''';

""")
    assert env.environment.concepts["signup_date"].keys == {"local.customer_id"}
    assert env.environment.concepts["signup_date.year"].keys == {"local.signup_date"}
    results = env.execute_query("""SELECT
    signup_date.year::int::year as signup_year;""")

    for row in results.fetchall():
        assert row.signup_year == 2023

    assert "year" in env.environment.data_types

    assert env.environment.concepts["signup_year"].datatype.traits == ["year"]

    results = env.execute_query("""SELECT
signup_date.year::int::dtypes.year as signup_year_two;""")

    for row in results.fetchall():
        assert row.signup_year_two == 2023

    assert "dtypes.year" in env.environment.data_types

    assert set(env.environment.concepts["signup_year_two"].datatype.traits) == set(
        [
            "year",
            "dtypes.year",
        ]
    )


def test_typing_aggregate():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse("""

type money float;

key revenue float::money;
key revenue_two float::money;
key multiplier float;

datasource orders (
    revenue:revenue,
    revenue_two:revenue_two,
    multiplier:multiplier
)
grain (revenue)
query '''
select 5.0 as revenue, 3.3 as revenue_two, 2.0 as multiplier
union all
select 10.0 as revenue, 13.1 as revenue_two, 3.0 as multiplier
''';


""")

    results = env.execute_query("""SELECT
sum(revenue)->direct_total,
sum(revenue*multiplier)->total,
direct_total-total -> diff;""")

    for row in results.fetchall():
        assert row.total == Decimal("40.00")

    assert "money" in env.environment.data_types
    assert env.environment.concepts["direct_total"].datatype.traits == ["money"]
    assert env.environment.concepts["total"].datatype.traits == ["money"]
    assert env.environment.concepts["diff"].datatype.traits == [
        "money"
    ], env.environment.concepts["diff"].datatype

    results = env.execute_query("""SELECT
revenue+revenue_two->add_total,
revenue-revenue_two->sub_total
;""")

    assert "money" in env.environment.data_types
    assert env.environment.concepts["add_total"].datatype.traits == ["money"]
    assert env.environment.concepts["sub_total"].datatype.traits == ["money"]


def test_custom_function_typing():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse("""

type money float;

key revenue float::money;

def revenue_times_2(revenue) -> revenue*2;

def revenue_times_multiplier(revenue, multiplier) -> revenue*multiplier;

datasource orders (
    revenue:revenue,

)
grain (revenue)
query '''
select 5.0 as revenue, 3.3 as revenue_two, 2.0 as multiplier
union all
select 10.0 as revenue, 13.1 as revenue_two, 3.0 as multiplier
''';


""")

    # TIS IS FAILING BECAUSE IT ALIASES REVENUE AS REVENUE
    _ = env.execute_query("""
with scaled as
SELECT
    @revenue_times_2(revenue)->revenue
;
        
SELECT
sum(
    round( @revenue_times_multiplier(lag 1 @revenue_times_2(scaled.revenue), 2.0), 2)
)->total;
""")

    assert env.environment.concepts["total"].datatype.traits == ["money"]


def test_case_with_trait_and_bare_numeric_literal():
    """Regression: a CASE branch that mixes ``numeric(p,s)::usd`` with a bare
    ``0.0::numeric(p,s)`` literal must not raise. The case-output validator
    previously called ``is_compatible_datatype`` against the TraitDataType
    wrapper, which dropped through to the ``isinstance(right, NumericType)``
    False branch and reported a spurious type mismatch."""
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse("""
import std.money;

key id int;
property id.ext_sales_price numeric(15,2)::usd;
property id.day_of_week int;

datasource sales (
    id: id,
    ext_sales_price: ext_sales_price,
    day_of_week: day_of_week,
)
grain (id)
query '''
select 1 as id, cast(10.00 as numeric(15,2)) as ext_sales_price, 1 as day_of_week
union all
select 2 as id, cast(20.00 as numeric(15,2)) as ext_sales_price, 2 as day_of_week
''';
""")

    results = env.execute_query("""SELECT
sum(
    case
        when day_of_week = 1 then ext_sales_price
        else 0.0::numeric(15,2)
    end
) -> monday_sales;""")

    rows = results.fetchall()
    assert rows[0].monday_sales == Decimal("10.00")
    assert env.environment.concepts["monday_sales"].datatype.traits == ["usd"]


def test_custom_trait_unnest_typing():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse("""
import std.geography;

const array <- ['VT', 'MA', 'NY', 'CA']::array<string::us_state_short>;

""")

    _ = env.execute_query("""
SELECT
    unnest(array)->state;
""")

    assert env.environment.concepts["state"].datatype.traits == ["us_state_short"]
