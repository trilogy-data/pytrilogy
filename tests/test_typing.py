from decimal import Decimal
from pathlib import Path

from pytest import raises

from trilogy import Dialects
from trilogy.core.models.core import (
    DataType,
    EnumType,
    TraitDataType,
    is_compatible_datatype,
)

FILE = Path(__file__)


def test_is_compatible_datatype_enum_and_trait():
    enum_str = EnumType(type=DataType.STRING, values=["a", "b"])
    enum_int = EnumType(type=DataType.INTEGER, values=[1, 2])
    # An enum is compatible with its base type and another enum over a
    # compatible base (q05: enum<string> store_id aligns with bare string).
    assert is_compatible_datatype(enum_str, DataType.STRING)
    assert is_compatible_datatype(DataType.STRING, enum_str)
    assert is_compatible_datatype(
        enum_str, EnumType(type=DataType.STRING, values=["x"])
    )
    # Integer-family stays mutually compatible through the enum/trait wrappers.
    assert is_compatible_datatype(enum_int, DataType.BIGINT)
    assert is_compatible_datatype(
        enum_int, TraitDataType(type=DataType.INTEGER, traits=["month"])
    )
    # Genuinely different bases remain incompatible.
    assert not is_compatible_datatype(enum_str, DataType.INTEGER)
    assert not is_compatible_datatype(enum_int, DataType.STRING)


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


def test_auto_concept_with_binding_prefers_binding_over_formula():
    """Regression: an ``auto x <- a * b`` concept that is *also* directly
    bound to a datasource column must source from the binding, not from
    decomposing into a*b. The bound value can diverge from the formula
    (e.g. TPC-DS stores CS_EXT_SALES_PRICE even on rows where CS_SALES_PRICE
    or CS_QUANTITY is NULL); summing the formula drops those rows."""
    env = Dialects.DUCK_DB.default_executor()
    env.environment.parse("""
key id int;
property id.qty int;
property id.price float;
property id.bucket int;

auto ext_price <- qty * price;

datasource sales (
    id: id,
    qty: qty,
    price: price,
    bucket: bucket,
    ext: ext_price,
)
grain (id)
query '''
select 1 as id, 5 as qty, 2.0 as price, 1 as bucket, 999.00 as ext
union all
select 2 as id, 3 as qty, 4.0 as price, 1 as bucket, 12.0 as ext
union all
select 3 as id, cast(null as int) as qty, 4.0 as price, 2 as bucket, 555.55 as ext
''';
""")

    rows = env.execute_query("SELECT bucket, sum(ext_price) -> total;").fetchall()
    by_bucket = {r.bucket: r.total for r in rows}
    # Both buckets must reflect the stored ext column. The formula
    # (qty * price) for row 3 is NULL because qty is NULL, so a
    # decomposition-based sum would yield NULL for bucket=2; the binding
    # has 555.55. Row 1 stores 999.00 even though qty*price=10.0, so a
    # decomposition would yield 22.00 for bucket=1.
    assert by_bucket[1] == Decimal("1011.00"), by_bucket
    assert by_bucket[2] == Decimal("555.55"), by_bucket

    sql = env.generate_sql("SELECT bucket, sum(ext_price) -> total;")[0]
    assert '"ext"' in sql, f"expected SQL to source the ext column, got: {sql}"
    assert '"qty"' not in sql, f"expected SQL to skip the formula, got: {sql}"


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
