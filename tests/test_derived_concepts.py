from trilogy import parse
from trilogy.core.models.environment import Environment


def test_derivations(test_environment):
    assert (
        test_environment.concepts["order_timestamp"].address == "local.order_timestamp"
    )
    assert (
        test_environment.concepts["order_timestamp.date"].address
        == "local.order_timestamp.date"
    )


def test_hour_derivation(test_environment):
    hour_derived = test_environment.concepts["order_timestamp.hour"]
    assert "hour" in hour_derived.datatype.traits, hour_derived

    year_derived = test_environment.concepts["order_timestamp.year"]
    assert "year" in year_derived.datatype.traits, year_derived


def test_filtering_where_on_derived_aggregate(test_environment):
    exception = False
    try:
        env, _ = parse("""key x int;
    property x.cost float;

    datasource x_source (
        x:x,
        cost:cost)
        grain(x)
        address x_source;


    SELECT
        sum(cost)-> filtered_cst
    where
    x > 10 and filtered_cst >1000;
        """)
    except Exception as e:
        exception = True
        assert (
            "Cannot reference an aggregate derived in the select (local.filtered_cst)"
            in str(e)
        )
    assert exception, "should have an exception"


def test_where_aggregate_also_in_select_rejected_cleanly():
    """The repro from the bug: ``where sum(x) > 1.2 * avg(x) by g`` with
    ``select sum(x) as total`` must not leak the internal MergeNode/_virt_agg
    state to the user.
    """
    from trilogy import parse
    from trilogy.core.exceptions import InvalidSyntaxException

    src = """key x int;
property x.cost float;
property x.state string;

datasource x_source (
    x:x,
    cost:cost,
    state:state)
    grain(x)
    address x_source;

where sum(cost) > 1.2 * avg(cost) by state
SELECT
    x,
    sum(cost) as total_cost;
"""
    raised = None
    try:
        parse(src)
    except Exception as e:
        raised = e
    assert raised is not None, "expected a validation exception"
    assert isinstance(
        raised, InvalidSyntaxException
    ), f"expected InvalidSyntaxException, got {type(raised).__name__}: {raised}"
    msg = str(raised)
    assert "HAVING" in msg
    # Internal resolver state must never reach the user.
    assert "MergeNode" not in msg
    assert "_virt_agg" not in msg
    assert "@Grain<" not in msg


def test_filtering_having_on_unincluded_value(test_environment):
    # A scalar SELECT (no grain key) cannot anchor a post-aggregation semijoin for
    # the finer `x > 10` predicate; the user is directed to WHERE instead.
    exception = False
    try:
        env, _ = parse("""key x int;
    property x.cost float;

    datasource x_source (
        x:x,
        cost:cost)
        grain(x)
        address x_source;


    SELECT
        sum(cost)-> filtered_cst
    having
    x > 10 and filtered_cst >1000;
        """)
    except Exception as e:
        exception = True
        assert "WHERE" in str(e), str(e)
    assert exception, "should have an exception"


def test_order_by_accepts_aliased_concept_source():
    parse("""key id int;
property id.value int;

datasource items (
    id: id,
    value: value)
    grain(id)
    address items;

select
    id,
    value as v
order by value asc;
""")


def test_having_accepts_aliased_aggregate_source():
    parse("""key id int;
property id.value int;
property id.category string;

datasource items (
    id: id,
    value: value,
    category: category)
    grain(id)
    address items;

select
    category,
    sum(value) as total
having sum(value) > 15;
""")


def test_filtering_valid(test_environment):
    env, _ = parse("""key x int;
property x.cost float;

datasource x_source (
    x:x,
    cost:cost)
    grain(x)
    address x_source;

where
    x>-10
SELECT
    sum(cost)-> filtered_cst
having
    filtered_cst >1000;
    """)


_HAVING_AGG_SCHEMA = """key store_id int;
key customer_id int;
property <store_id, customer_id>.amount float;

datasource sales (
    store_id: store_id,
    customer_id: customer_id,
    amount: amount)
    grain (store_id, customer_id)
    address sales;
"""


def test_having_off_grain_aggregate_promoted_to_hidden_output():
    # An off-grain aggregate in HAVING that is not a SELECT output is promoted to a
    # hidden output (computed in its own CTE at its `by` grain) and the HAVING
    # points at it — no longer an error.
    from trilogy import Dialects

    sql = "\n".join(
        Dialects.DUCK_DB.default_executor(environment=Environment()).generate_sql(
            _HAVING_AGG_SCHEMA + """
select
    store_id,
    customer_id,
    sum(amount) as total
having sum(amount) > 1.2 * sum(amount) by store_id;
"""
        )
    )
    assert "INVALID_REFERENCE_BUG" not in sql


def test_having_nested_aggregate_promoted_to_hidden_output():
    from trilogy import Dialects

    sql = "\n".join(
        Dialects.DUCK_DB.default_executor(environment=Environment()).generate_sql(
            _HAVING_AGG_SCHEMA + """
select
    store_id,
    customer_id,
    sum(amount) as total
having sum(amount) > 1.2 * avg(sum(amount) by store_id);
"""
        )
    )
    assert "INVALID_REFERENCE_BUG" not in sql


def test_having_substitutes_matching_off_grain_aggregate_with_alias():
    from trilogy import Dialects

    text = _HAVING_AGG_SCHEMA + """
select
    store_id,
    customer_id,
    sum(amount) as total,
    sum(amount) by store_id as store_total
having sum(amount) > 1.2 * sum(amount) by store_id;
"""
    sql = Dialects.DUCK_DB.default_executor().generate_sql(text)
    joined = "\n".join(sql)
    assert "INVALID_REFERENCE" not in joined, joined
    assert '"store_total"' in joined, joined
    # No nested aggregates in the rendered SQL.
    import re

    assert not re.search(
        r"\b(sum|avg|min|max|count)\s*\(\s*(sum|avg|min|max|count)\s*\(", joined
    ), joined


def test_having_substitutes_matching_nested_aggregate_with_alias():
    from trilogy import Dialects

    text = _HAVING_AGG_SCHEMA + """
select
    store_id,
    customer_id,
    sum(amount) as total,
    avg(sum(amount) by store_id) as avg_store_total
having sum(amount) > 1.2 * avg(sum(amount) by store_id);
"""
    sql = Dialects.DUCK_DB.default_executor().generate_sql(text)
    joined = "\n".join(sql)
    assert "INVALID_REFERENCE" not in joined, joined
    assert '"avg_store_total"' in joined, joined
    import re

    assert not re.search(
        r"\b(sum|avg|min|max|count)\s*\(\s*(sum|avg|min|max|count)\s*\(", joined
    ), joined
