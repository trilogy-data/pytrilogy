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


def test_where_aggregate_on_grouped_select_executes():
    """A WHERE aggregate alongside a grouped SELECT (``select x, sum(cost) ...``)
    is computed at the select grain over the WHERE-unfiltered universe, distinct
    from the SELECT aggregate. Formerly a hard error; now it plans and executes.
    """
    from trilogy import Dialects
    from trilogy.core.models.environment import Environment

    src = """key x int;
property x.cost float;
property x.state string;
datasource x_source ( x:x, cost:cost, state:state) grain(x)
query '''select 1 as x, 100.0 as cost, 'A' as state
union all select 2, 50.0, 'A'
union all select 3, 2000.0, 'B'
union all select 4, 10.0, 'B' ''';

where sum(cost) > 1.2 * avg(cost) by state
SELECT x, sum(cost) as total_cost;
"""
    sql = "\n".join(
        Dialects.DUCK_DB.default_executor(environment=Environment()).generate_sql(src)
    )
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = sorted(
        tuple(r)
        for r in Dialects.DUCK_DB.default_executor().execute_text(src)[-1].fetchall()
    )
    # state A 1.2*avg=90: x1=100>90 keep, x2=50 drop. state B 1.2*avg=1206: x3=2000 keep, x4=10 drop.
    assert rows == [(1, 100.0), (3, 2000.0)], rows


_WHERE_AGG_SCHEMA = """key id int;
property id.x int;
property id.z int;
property id.f int;
datasource d ( id, x, z, f ) grain (id)
query '''select 1 as id, 1 as x, 2 as z, 1 as f
union all select 2, 1, 10, 0
union all select 3, 2, 100, 1''';
"""


def _rows(query: str):
    from trilogy import Dialects

    return sorted(
        tuple(r)
        for r in Dialects.DUCK_DB.default_executor()
        .execute_text(_WHERE_AGG_SCHEMA + query)[-1]
        .fetchall()
    )


def test_where_aggregate_input_not_filtered_by_where():
    # group x=1 has rows (z=2,f=1),(z=10,f=0). The WHERE aggregate sees BOTH rows
    # (input unfiltered: sum=12 > 5 -> group survives); the SELECT aggregate sees
    # only f=1 (sum=2). A having-equivalent would have dropped x=1 (filtered sum 2).
    inline = _rows("where f = 1 and sum(z) by x > 5 select x, sum(z) as v;")
    alias = _rows("select x, sum(z) by x as sx where f = 1 and sx > 5;")
    assert inline == [(1, 2), (2, 100)], inline
    assert alias == [(1, 12), (2, 100)], alias


def test_where_aggregate_matching_select_output_executes():
    # The eval pattern: an inline WHERE aggregate identical to a SELECT output.
    matching = _rows("where f = 1 and sum(z) > 5 select x, sum(z) as v;")
    assert matching == [(1, 2), (2, 100)], matching


def test_scalar_select_where_aggregate_still_rejected():
    # A SCALAR select (no grouping key) keeps the clean redirect to HAVING: there is
    # no grain to anchor the WHERE aggregate and the planner drops sibling filters.
    from trilogy import parse

    for query in (
        "select sum(z) as v where v > 5;",
        "select sum(z) as v where sum(z) > 5;",
    ):
        raised = None
        try:
            parse(_WHERE_AGG_SCHEMA + query)
        except Exception as e:  # noqa: BLE001
            raised = e
        assert raised is not None, f"expected rejection for: {query}"
        assert "HAVING" in str(raised), str(raised)


_SCALAR_FILTER_SCHEMA = """key x int;
property x.cost float;
datasource d ( x, cost ) grain (x)
query '''select 1 as x, 100.0 as cost
union all select 2, 50.0
union all select 3, 2000.0
union all select 4, 10.0''';
auto grand_total <- sum(cost) by *;
"""


def test_where_filter_on_scalar_output_value_is_applied():
    # Regression: a WHERE predicate on a single-row scalar that IS the query output
    # (`where grand_total > N select grand_total`) was silently dropped — the scalar
    # node was treated as a cross-joined constant whose conditions are independent.
    # `_is_scalar_only` now declines the exemption when the predicate filters the
    # node's own output, so the gate actually filters.
    from trilogy import Dialects

    def rows(query):
        return [
            tuple(r)
            for r in Dialects.DUCK_DB.default_executor()
            .execute_text(_SCALAR_FILTER_SCHEMA + query)[-1]
            .fetchall()
        ]

    # grand_total is 2160; a passing gate yields it, a failing gate yields no rows.
    assert rows("where grand_total > 1000 select grand_total;") == [(2160.0,)]
    assert rows("where grand_total > 5000 select grand_total;") == []


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
