from decimal import Decimal
from pathlib import Path

from pytest import raises

from trilogy import Dialects
from trilogy.parsing.exceptions import ParseError

FILE = Path(__file__)


def test_invalid_typing():
    env = Dialects.DUCK_DB.default_executor()
    with raises(ParseError):
        env.environment.parse(
            """
    key customer_id int;
    property customer_id.email string::email;


    """
        )


def test_typing():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse(
        """

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


"""
    )

    results = env.execute_query(
        """SELECT
customer_id,
@is_valid_email(email)->valid;"""
    )

    for row in results.fetchall():
        assert row.valid is True

    assert "email" in env.environment.data_types

    assert env.environment.concepts["email"].datatype.traits == ["email"]


def test_type_import_and_cast():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.add_file_import(FILE.parent / "test_env_types.preql", "dtypes")
    env.environment.parse(
        """

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

"""
    )
    assert env.environment.concepts["signup_date"].keys == {"local.customer_id"}
    assert env.environment.concepts["signup_date.year"].keys == {"local.signup_date"}
    results = env.execute_query(
        """SELECT
    signup_date.year::int::year as signup_year;"""
    )

    for row in results.fetchall():
        assert row.signup_year == 2023

    assert "year" in env.environment.data_types

    assert env.environment.concepts["signup_year"].datatype.traits == ["year"]

    results = env.execute_query(
        """SELECT
signup_date.year::int::dtypes.year as signup_year_two;"""
    )

    for row in results.fetchall():
        assert row.signup_year_two == 2023

    assert "dtypes.year" in env.environment.data_types

    assert env.environment.concepts["signup_year_two"].datatype.traits == [
        "dtypes.year"
    ]


def test_typing_aggregate():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse(
        """

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


"""
    )

    results = env.execute_query(
        """SELECT
sum(revenue)->direct_total,
sum(revenue*multiplier)->total,
direct_total-total -> diff;"""
    )

    for row in results.fetchall():
        assert row.total == Decimal("40.00")

    assert "money" in env.environment.data_types
    assert env.environment.concepts["direct_total"].datatype.traits == ["money"]
    assert env.environment.concepts["total"].datatype.traits == ["money"]
    assert env.environment.concepts["diff"].datatype.traits == [
        "money"
    ], env.environment.concepts["diff"].datatype

    results = env.execute_query(
        """SELECT
revenue+revenue_two->add_total,
revenue-revenue_two->sub_total
;"""
    )

    assert "money" in env.environment.data_types
    assert env.environment.concepts["add_total"].datatype.traits == ["money"]
    assert env.environment.concepts["sub_total"].datatype.traits == ["money"]


def test_custom_function_typing():
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse(
        """

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


"""
    )

    _ = env.execute_query(
        """
with scaled as
SELECT
    @revenue_times_2(revenue)->revenue
;
        
SELECT
sum(
    round( @revenue_times_multiplier(lag 1 @revenue_times_2(scaled.revenue), 2.0), 2)
)->total;
"""
    )

    assert env.environment.concepts["total"].datatype.traits == ["money"]
