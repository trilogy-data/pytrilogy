from pytest import raises

from trilogy import Dialects
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.models.build import BuildGrain, BuildWindowItem
from trilogy.core.processing.concept_strategies_v3 import (
    History,
    generate_graph,
    search_concepts,
)
from trilogy.core.processing.utility import concept_to_relevant_joins
from trilogy.core.query_processor import get_query_datasources, process_query
from trilogy.core.statements.author import SelectStatement, WindowItem
from trilogy.dialect import duckdb
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.parser import parse
from trilogy.parsing.exceptions import ParseError


def test_select() -> None:
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.about_me string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);

property user_id.user_rank <- rank user_id by post_count desc;


datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: about_me,
    )
    grain (user_id)
    address `bigquery-public-data.stackoverflow.users`
;


select
    user_id,
    user_rank,
    post_count
where 
    user_rank<10
limit 100
;


    """
    orig_env, parsed = parse(declarations)
    history = History(base_environment=orig_env)
    env = orig_env.materialize_for_select()
    select: SelectStatement = parsed[-1]

    assert isinstance(env.concepts["user_rank"].lineage, BuildWindowItem)

    ds = search_concepts(
        [env.concepts["post_count"], env.concepts["user_id"]],
        history=history,
        environment=env,
        g=generate_graph(env),
        depth=0,
    ).resolve()
    # ds.validate()
    ds.get_alias(env.concepts["post_count"].with_grain(ds.grain))

    get_query_datasources(environment=orig_env, statement=select)
    # raise ValueError

    query = process_query(statement=select, environment=orig_env)
    query.ctes[0]

    generator = BigqueryDialect()
    generator.compile_statement(query)


def test_constant_rank():
    declarations = """
auto bar <- unnest([1,2,3]);
auto fun <- rank 1 by bar desc;
"""
    with raises(ParseError):
        _ = parse(declarations)


def test_rank_by():
    declarations = """
key user_id int metadata(description="the description");
property user_id.display_name string metadata(description="The display name ");
property user_id.country string metadata(description="User provided description");


key post_id int;
metric post_count <-count(post_id);

property user_id.user_country_rank <- rank user_id over country;

property user_id.rank_derived <- 100 + row_number user_id over country;

datasource posts (
    user_id: user_id,
    id: post_id
    )
    grain (post_id)
    address `bigquery-public-data.stackoverflow.post_history`
;


datasource users (
    id: user_id,
    display_name: display_name,
    about_me: country,
    )
    grain (user_id)
    address `bigquery-public-data.stackoverflow.users`
;


select
    user_id,
    user_country_rank,
    rank_derived,
    post_count
where 
    user_country_rank<10
limit 100
;


    """
    env, parsed = parse(declarations)
    orig_env = env

    env = env.materialize_for_select()
    select: SelectStatement = parsed[-1]

    assert env.concepts["rank_derived"].keys == {
        env.concepts["user_id"].address,
    }
    assert concept_to_relevant_joins(
        [env.concepts[x] for x in ["user_id", "rank_derived"]]
    ) == [env.concepts["user_id"]]
    assert isinstance(orig_env.concepts["user_country_rank"].lineage, WindowItem)

    get_query_datasources(environment=orig_env, statement=select)
    # raise ValueError

    query = process_query(statement=select, environment=orig_env)
    query.ctes[0]

    generator = BigqueryDialect()
    compiled = generator.compile_statement(query)
    assert "rank() over (partition" in compiled


def test_const_by():

    declarations = """
const x <- unnest([1,2,2,3]);
const y <- 5;
auto z <- rank x order by x desc;

select x, z 
order by x asc;"""
    org_env, parsed = parse(declarations)
    history = History(base_environment=org_env)
    env = org_env.materialize_for_select()
    select: SelectStatement = parsed[-1]
    x = env.concepts["x"]
    assert x.granularity == Granularity.MULTI_ROW

    z = env.concepts["z"]
    assert z.purpose == Purpose.PROPERTY
    assert set([x.address for x in z.lineage.concept_arguments]) == set(
        [
            x.address,
        ]
    )
    assert z.keys == {
        x.address,
    }

    ds = search_concepts(
        [z.with_grain(BuildGrain(components={x.address})), x],
        history=history,
        environment=env,
        g=generate_graph(env),
        depth=0,
    ).resolve()

    assert x.address in ds.output_concepts
    assert z.address in ds.output_concepts

    assert x.derivation == Derivation.UNNEST
    assert z.derivation != Derivation.CONSTANT

    generator = duckdb.DuckDBDialect()
    query = process_query(statement=select, environment=org_env)
    compiled = generator.compile_statement(query)
    assert "unnest" in compiled
    exec = Dialects.DUCK_DB.default_executor(environment=org_env)
    results = exec.execute_text(declarations)
    select = results[-1]
    assert [row.x for row in select] == [1, 2, 2, 3]


def test_inline_window():
    declarations = """
const x <- unnest([1,2,3,4]);


select
    x,
    row_number x order by x asc -> z,
order by x desc;"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.execute_text(declarations)
    select = results[-1].fetchall()
    assert [row.x for row in select] == [4, 3, 2, 1]
    assert [row.z for row in select] == [4, 3, 2, 1]


def test_sql_window_syntax_basic():
    """Test SQL-like window function syntax: row_number(field) over (order by field)"""
    declarations = """
const x <- unnest([1,2,3,4]);

select
    x,
    row_number(x) over (order by x asc) -> z,
order by x desc;"""
    exec = Dialects.DUCK_DB.default_executor()
    results = exec.execute_text(declarations)
    select = results[-1].fetchall()
    assert [row.x for row in select] == [4, 3, 2, 1]
    assert [row.z for row in select] == [4, 3, 2, 1]


def test_sql_window_syntax_partition():
    """Test SQL-like window with partition by: rank(field) over (partition by x order by y)"""
    declarations = """
key user_id int;
property user_id.country string;
property user_id.score int;
property user_id.country_rank <- rank(user_id) over (partition by country order by score desc);

datasource users (
    id: user_id,
    country: country,
    score: score,
)
grain (user_id)
address memory.users;

select user_id, country, score, country_rank;
"""
    env, _ = parse(declarations)
    assert isinstance(env.concepts["country_rank"].lineage, WindowItem)
    window_item = env.concepts["country_rank"].lineage
    assert len(window_item.over) == 1
    assert window_item.over[0].address == "local.country"
    assert len(window_item.order_by) == 1


def test_sql_window_lag_with_index():
    """Test SQL-like lag/lead with index inside parentheses: lag(field, 2)"""
    declarations = """
const x <- unnest([1,2,3,4,5]);

select
    x,
    lag(x, 2) over (order by x asc) -> lagged,
order by x asc;"""
    env, _ = parse(declarations)
    # Check that the lagged concept was created with the right index
    assert "lagged" in [c.split(".")[-1] for c in env.concepts.keys()]
    lineage = env.concepts["lagged"].lineage
    assert isinstance(lineage, WindowItem)
    assert lineage.index == 2


def test_sql_window_error_missing_field():
    """Test helpful error when window function is called without a field"""
    # Note: The grammar requires an expression inside the parentheses,
    # so row_number() without any argument would be a parse error
    # But we can test that a constant still gives a good error
    declarations = """
const x <- unnest([1,2,3]);
auto z <- row_number(1) over (order by x asc);
"""
    with raises(ParseError) as exc_info:
        parse(declarations)
    assert (
        "field" in str(exc_info.value).lower()
        or "constant" in str(exc_info.value).lower()
    )


def test_window_rendering_sql_style():
    """Test that window items render in SQL-like style"""
    from trilogy.parsing.render import Renderer

    declarations = """
key user_id int;
property user_id.country string;
property user_id.score int;
property user_id.country_rank <- rank(user_id) over (partition by country order by score desc);
"""
    env, parsed = parse(declarations)
    renderer = Renderer(environment=env)
    rendered = renderer.to_string(parsed[-1])
    # Should use SQL-like syntax
    assert "rank(user_id)" in rendered
    assert "over (" in rendered
    assert "partition by country" in rendered
    assert "order by score desc" in rendered


def test_window_rendering_round_trip():
    """Test that window functions can be parsed, rendered, and re-parsed"""
    declarations = """
key user_id int;
property user_id.country string;
property user_id.score int;
property user_id.country_rank <- rank(user_id) over (partition by country order by score desc);
"""
    from trilogy.parsing.render import Renderer

    env, parsed = parse(declarations)
    renderer = Renderer(environment=env)
    rendered = renderer.to_string(parsed[-1])

    # Re-parse the rendered output
    env2, _ = parse(rendered)
    assert "country_rank" in [c.split(".")[-1] for c in env2.concepts.keys()]


def test_window_lag_rendering_with_index():
    """Test that lag/lead with index renders correctly"""
    from trilogy.parsing.render import Renderer

    declarations = """
key x int;
property x.lagged <- lag(x, 3) over (order by x asc);
"""
    env, parsed = parse(declarations)
    renderer = Renderer(environment=env)
    rendered = renderer.to_string(parsed[-1])
    assert "lag(x,3)" in rendered
    assert "order by x asc" in rendered


def test_window_to_aggregate_optimization():
    """Window functions without ORDER BY should be converted to aggregates."""
    from trilogy.core.models.author import AggregateWrapper

    # Use legacy syntax: sum field over partition_field
    declarations = """
key user_id int;
property user_id.country string;
property user_id.score int;
# This should become an aggregate (no order by) - use auto since it's now a metric
auto country_total <- sum score over country;
# This should remain a window function (has order by)
property user_id.running_total <- sum score over country order by user_id asc;
"""
    env, _ = parse(declarations)

    # Without ORDER BY -> converted to AggregateWrapper (and becomes a metric)
    country_total = env.concepts["country_total"]
    assert isinstance(
        country_total.lineage, AggregateWrapper
    ), f"Expected AggregateWrapper but got {type(country_total.lineage)}"
    assert len(country_total.lineage.by) == 1
    assert country_total.lineage.by[0].address == "local.country"

    # With ORDER BY -> remains as WindowItem (stays as property)
    running_total = env.concepts["running_total"]
    assert isinstance(
        running_total.lineage, WindowItem
    ), f"Expected WindowItem but got {type(running_total.lineage)}"


def test_window_to_aggregate_execution():
    """Verify that converted window functions execute correctly."""
    from trilogy.core.models.author import AggregateWrapper

    declarations = """
key id int;
key category string;
property id.value int;

datasource data (
    id: id,
    category: category,
    value: value,
)
grain (id)
address memory.test_data;

# Window without order by - converted to aggregate internally
auto category_sum <- sum value over category;

select id, category, value, category_sum
order by id asc;
"""
    exec = Dialects.DUCK_DB.default_executor()
    # Create test data
    exec.execute_raw_sql(
        """
        CREATE TABLE test_data AS SELECT * FROM (VALUES
            (1, 'A', 10),
            (2, 'A', 20),
            (3, 'B', 5),
            (4, 'B', 15)
        ) AS t(id, category, value)
    """
    )

    # Verify the optimization occurred
    env = exec.environment
    exec.parse_text(declarations.split("select")[0])
    assert isinstance(env.concepts["category_sum"].lineage, AggregateWrapper)

    results = exec.execute_text(declarations)
    rows = results[-1].fetchall()

    # Check that category sums are correct
    # A: 10 + 20 = 30, B: 5 + 15 = 20
    for row in rows:
        if row.category == "A":
            assert row.category_sum == 30
        else:
            assert row.category_sum == 20
