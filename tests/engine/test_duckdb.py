from datetime import datetime, timedelta
from pathlib import Path

import networkx as nx
from pytest import mark, raises

from trilogy import Dialects
from trilogy.constants import Rendering
from trilogy.core.enums import Derivation, FunctionType, Granularity, Purpose
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.author import (
    Concept,
    Grain,
)
from trilogy.core.models.build import BuildFilterItem, BuildSubselectComparison, Factory
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.discovery_utility import get_upstream_concepts
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
    resolve_function_parent_concepts,
)
from trilogy.core.statements.author import ShowStatement
from trilogy.dialect.mock import DEFAULT_SCALE_FACTOR
from trilogy.executor import Executor
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parser import parse_text


def test_basic_query(duckdb_engine: Executor, expected_results):
    graph = generate_graph(duckdb_engine.environment.materialize_for_select())

    list(nx.neighbors(graph, "c~local.count@Grain<local.item,local.store_id>"))
    results = duckdb_engine.execute_text("""select total_count;""")[0].fetchall()
    assert results[0].total_count == expected_results["total_count"]


def test_concept_derivation():
    duckdb_engine = Dialects.DUCK_DB.default_executor()
    test_datetime = datetime(hour=12, day=1, month=2, year=2022, second=34)

    duckdb_engine.execute_text(
        f"""const test <- cast('{test_datetime.isoformat()}' as datetime);
    """
    )
    for property, check in [
        ["date", test_datetime.date()],
        ["hour", test_datetime.hour],
        ["second", test_datetime.second],
        ["minute", test_datetime.minute],
        ["year", test_datetime.year],
        ["month", test_datetime.month],
    ]:
        # {test_datetime.isoformat()}
        test_query = f""" 


        select local.test.{property};
        
        """
        query = duckdb_engine.parse_text(test_query)
        name = f"local.test.{property}"
        assert duckdb_engine.environment.concepts[name].address == name
        assert query[-1].output_columns[0].address == f"local.test.{property}"
        results = duckdb_engine.execute_text(test_query)[0].fetchall()
        assert results[0][0] == check
    for truncation in [
        "month",
        "year",
    ]:
        test_query = f"""
        select local.test.{truncation}_start;
        """
        query = duckdb_engine.parse_text(test_query)
        name = f"local.test.{truncation}_start"
        assert duckdb_engine.environment.concepts[name].address == name
        assert query[-1].output_columns[0].address == f"local.test.{truncation}_start"
        results = duckdb_engine.execute_text(test_query)[0].fetchall()
        assert results[0][0] == test_datetime.replace(
            day=1,
            month=1 if truncation == "year" else test_datetime.month,
            hour=0,
            minute=0,
            second=0,
        )


def test_boolean_derivation():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()

    results = executor.execute_text(
        """const test <- 1 is not null;

        select test;
    """
    )

    assert results[0].fetchall()[0][0] is True

    results = executor.execute_text(
        """ const rows <- unnest([1,2,3,4,5]);

    auto big <- rows >3;

    select rows, big
    order by rows asc;
    """
    )
    assert results[0].fetchall() == [
        (1, False),
        (2, False),
        (3, False),
        (4, True),
        (5, True),
    ]


def test_render_query(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.generate_sql("""select total_count;""")[0]

    assert "total" in results


def test_aggregate_at_grain(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""select avg_count_per_product;""")[
        0
    ].fetchall()
    assert results[0].avg_count_per_product == expected_results["avg_count_per_product"]


def test_empty_string(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text("""select '' as empty_string;""")[0].fetchall()
    assert results[0].empty_string == ""


def test_order_of_operations(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_query(
        """
    const x <- 7;
    const y <- 8;

    auto z <- x + y;
    auto a <- z/2;
                                          
    select a;
"""
    ).fetchall()
    assert results[0].a == 7.5, results[0].a


def test_constant_derivation(
    duckdb_engine: Executor,
):
    results = duckdb_engine.execute_text(
        """select 1 as test; key x int; datasource funky_monkey (x) query '''select 1 as x'''; select x+1 as test2;"""
    )
    assert results[0].fetchall()[0].test == 2


def test_constants(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text(
        """const usd_conversion <- 2;

    auto converted_total_count <-  total_count * usd_conversion;
    
    select converted_total_count ;
    """
    )[0].fetchall()
    # expected_results["converted_total_count"]

    scaled_metric = duckdb_engine.environment.concepts["converted_total_count"]

    assert (
        duckdb_engine.environment.concepts["usd_conversion"].granularity
        == Granularity.SINGLE_ROW
    )
    parent_arg: Concept = duckdb_engine.environment.concepts[
        [
            x.address
            for x in scaled_metric.lineage.concept_arguments
            if x.address == "local.total_count"
        ][0]
    ]
    assert (
        len(
            duckdb_engine.environment.concepts[
                parent_arg.lineage.concept_arguments[0].address
            ].grain.components
        )
        == 2
    )
    # assert Grain(components = [duckdb_engine.environment.concepts['usd_conversion']]) == Grain()
    assert results[0].converted_total_count == expected_results["converted_total_count"]


def test_constant_typing(duckdb_engine: Executor, expected_results):
    duckdb_engine.execute_text(
        """import std.net;

const image_url <- 'www.example.com'::string::url_image;

select
image_url, 
'www.example.com'::string::url_image as image_url2;

    """
    )
    for concept_name in ["image_url", "image_url2"]:
        concept = duckdb_engine.environment.concepts[concept_name]
        assert "url_image" in concept.datatype.traits, concept.lineage
        assert concept.purpose == Purpose.CONSTANT, concept.lineage


def test_unnest(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text(
        """const array <- [1,2,3];
    """
    )
    array = duckdb_engine.environment.concepts["array"]
    assert array.lineage
    assert array.lineage.arguments[0] == [1, 2, 3]

    results = duckdb_engine.execute_text(
        """const array <- [1,2,3];
const unnest_array <- unnest(array);
    
    select unnest_array
    order by unnest_array asc;
    """
    )[0].fetchall()
    assert [x.unnest_array for x in results] == [1, 2, 3]


def test_partial(duckdb_engine: Executor, expected_results):
    results = duckdb_engine.execute_text(
        """select item, sum(discount_value)-> total_discount
        order by item desc;
    """
    )[0].fetchall()
    assert len(results) == 2


def test_show(duckdb_engine: Executor, expected_results):
    test = """show 
select
    item, 
    sum(discount_value)-> total_discount
order by item desc;
    """

    _, parsed_0 = parse_text(test, duckdb_engine.environment)

    assert len(parsed_0) == 1
    assert isinstance(parsed_0[0], ShowStatement)

    parsed = duckdb_engine.parse_text(test)

    assert len(parsed) == 1
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert '"fact_items"."item" as "item"' in results[0]["__preql_internal_query_text"]


def test_show_persist(duckdb_engine: Executor):
    test = """
    
    auto random_data <- unnest([1,2,3,4, 88, 99]);

    datasource fact_random(
      random_data)
    grain (random_data)
    address fct_random
    state unpublished;
    
    show overwrite fact_random;"""

    duckdb_engine.parse_text(test)

    results = duckdb_engine.execute_text(test)[-1].fetchall()
    assert len(results) == 1


def test_show_concepts(duckdb_engine: Executor):
    test = """show concepts;"""
    parsed = duckdb_engine.parse_text(test)

    assert len(parsed) == 1
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == len(
        [k for k, v in duckdb_engine.environment.concepts.items() if not v.is_internal]
    )


def test_show_datasources(duckdb_engine: Executor):
    test = """show datasources;"""

    with raises(NotImplementedError):
        duckdb_engine.parse_text(test)
        duckdb_engine.execute_text(test)


def test_rollback(duckdb_engine: Executor, expected_results):
    try:
        _ = duckdb_engine.execute_raw_sql("select abc")
    except Exception:
        pass

    results = duckdb_engine.execute_raw_sql("select 1")
    assert results.fetchall()[0] == (1,)


def test_basic_dates(duckdb_engine: Executor):
    test = """
  auto today <- current_datetime();
  auto tomorrow <- date_add(today, day, 1);
  auto yesterday <- date_sub(today, day, 1);
  select 
    tomorrow,
    yesterday,
    date_diff(today, today, day)->zero,
    date_trunc(today, year) -> current_year,
    day_of_week(today) -> today_dow
  ;
    """
    duckdb_engine.parse_text(test)
    assert (
        duckdb_engine.environment.concepts["tomorrow"].granularity
        == Granularity.SINGLE_ROW
    )
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results[0]) == 5


def test_rowset(duckdb_engine: Executor):
    test = """const x <- unnest([1,2,2,3]);
const y <- 5;
auto z <- rank x order by x desc;

rowset my_rowset <- select x, z where z = 1;

select my_rowset.x, my_rowset.z;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain.components == {
        x.address,
    }
    assert str(z) == "local.z@Grain<local.x>"
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1


def test_rowset_agg(duckdb_engine: Executor):
    test = """const x <- unnest([1,2,2,3]);
const y <- 5;
auto z <- rank x order by x desc;

rowset my_rowset <- select x, max(z)->max_rank;

select my_rowset.x, my_rowset.max_rank;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain == Grain(components=[x])
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 4


def test_rowset_join(duckdb_engine: Executor):
    test = """
key x int;

datasource x_data (
x:x)
query '''
select unnest([1,2,2,3]) as x
'''
;
    
const y <- 5;
auto z <- rank x order by x desc;
auto w <- rank x order by x asc;

rowset my_rowset <- select x, max(z)->max_rank;

select x, w, my_rowset.max_rank;"""
    _, parsed_0 = parse_text(test, duckdb_engine.environment)
    z = duckdb_engine.environment.concepts["z"]
    x = duckdb_engine.environment.concepts["x"]
    assert z.grain == Grain(components=[x])
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 3
    for row in results:
        if row.x == 2:
            assert row.my_rowset_max_rank == 2


def test_default_engine(default_duckdb_engine: Executor):
    test = """
  auto today <- current_datetime();
  
  select 
    date_add(today, day, 1)->tomorrow,
    date_diff(today, today, day)->zero,
    date_trunc(today, year) -> current_year 
  ;
    """
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results[0]) == 3


def test_complex(default_duckdb_engine: Executor):
    test = """
const list <- [1,2,2,3];
const orid <- unnest(list);

auto half_orid <- ((orid+17)/2);

select 
    orid,
    half_orid,
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    listc = default_duckdb_engine.environment.concepts["list"]
    assert listc.purpose == Purpose.CONSTANT
    orid = default_duckdb_engine.environment.concepts["orid"]
    half = default_duckdb_engine.environment.concepts["half_orid"]
    assert orid.address in [x.address for x in half.concept_arguments]
    assert set([x for x in half.keys]) == {
        "local.orid",
    }
    assert half.lineage.operator == FunctionType.DIVIDE
    assert half.derivation == Derivation.BASIC
    assert half.granularity == Granularity.MULTI_ROW
    assert len(results) == 4


def test_array_inclusion(default_duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
const orid <- unnest(list);
const orid_2 <-unnest(list_2);
auto even_orders <- filter orid where (orid % 2) = 0;
auto filtered_even_orders <- filter orid_2 where orid_2 in even_orders;

select 
    filtered_even_orders
where
    filtered_even_orders
;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 3


def test_array_inclusion_aggregate_one(default_duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    default_duckdb_engine.hooks = [DebuggingHook()]
    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
auto orid <- unnest(list);
auto orid_2 <-unnest(list_2);
auto even_orders <- filter orid where (orid % 2) = 0;
auto filtered_even_orders <- filter orid_2 where orid_2 in even_orders;
metric f_ord_count <- count(filtered_even_orders);

select 
    f_ord_count
;
    """
    _ = default_duckdb_engine.parse_text(test)[-1]
    orig_env = default_duckdb_engine.environment
    env = orig_env.materialize_for_select()
    agg = env.concepts["f_ord_count"]
    agg_parent = resolve_function_parent_concepts(agg, environment=env)[0]
    assert agg_parent.address == "local.filtered_even_orders"
    assert isinstance(agg_parent.lineage, BuildFilterItem)
    assert isinstance(agg_parent.lineage.where.conditional, BuildSubselectComparison)
    _, existence = resolve_filter_parent_concepts(agg_parent, environment=env)
    assert len(existence) == 1
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].f_ord_count == 3


def test_array_inclusion_aggregate(default_duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    default_duckdb_engine.hooks = [DebuggingHook()]
    test = """
const list <- [1,2,3,4,5,6];
const list_2 <- [1,2,3,4,5,6,7,8,9,10];
auto orid <- unnest(list);
auto orid_2 <-unnest(list_2);
auto even_orders <- filter orid where (orid % 2) = 0;
auto filtered_even_orders <- filter orid_2 where 1=1 and orid_2 in even_orders;
metric f_ord_count <- count(filtered_even_orders);
metric ord_count <- count(orid_2);

select 
    f_ord_count
;
    """
    _ = default_duckdb_engine.parse_text(test)[-1]
    orig_env = default_duckdb_engine.environment
    env = orig_env.materialize_for_select()
    agg = env.concepts["f_ord_count"]
    agg_parent = resolve_function_parent_concepts(agg, environment=env)[0]
    assert agg_parent.address == "local.filtered_even_orders"
    assert isinstance(agg_parent.lineage, BuildFilterItem)
    _, existence = resolve_filter_parent_concepts(agg_parent, environment=env)
    assert len(existence) == 1
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].f_ord_count == 3
    comp = """
where orid_2 in even_orders
select
    ord_count
;
"""
    results = default_duckdb_engine.execute_text(comp)[0].fetchall()
    assert results[0].ord_count == 3


def test_agg_demo(default_duckdb_engine: Executor):
    test = """key orid int;
key store string;
key customer int;

auto customer_orders <- count(orid) by customer;
datasource agg_example(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', 244
union all
select 4, 'store3', 244
''';

auto avg_customer_orders <- avg(customer_orders);

select 
    avg_customer_orders,
    avg(count(orid) by store) -> avg_store_orders,
;"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    build_env = default_duckdb_engine.environment.materialize_for_select()
    customer_orders = build_env.concepts["customer_orders"]
    assert set([x for x in customer_orders.keys]) == {"local.customer"}
    assert set([x for x in customer_orders.grain.components]) == {"local.customer"}

    customer_orders_2 = customer_orders
    assert set([x for x in customer_orders_2.keys]) == {"local.customer"}
    assert set([x for x in customer_orders_2.grain.components]) == {"local.customer"}

    count_by_customer = build_env.concepts[
        "avg_customer_orders"
    ].lineage.concept_arguments[0]
    # assert isinstance(count_by_customer, AggregateWrapper)
    assert set([x for x in count_by_customer.keys]) == {"local.customer"}
    assert set([x for x in count_by_customer.grain.components]) == {"local.customer"}
    assert len(results) == 1
    assert results[0].avg_customer_orders == 2
    assert round(results[0].avg_store_orders, 2) == 1.33


def test_constant_group(default_duckdb_engine: Executor):
    test = """
const x <- 1;
const x2 <- x+1;

auto constant_group_orid <- unnest([1,2,3]);
property constan_group_orid.mod_two <- constant_group_orid % 2;

select 
    mod_two,
    x2
order by
    mod_two asc
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (0, 2)


def test_case_group():

    default_duckdb_engine = Dialects.DUCK_DB.default_executor(hooks=[DebuggingHook()])

    test = """
const x <- 1;
const x2 <- x+1;

auto orid <- unnest([1,2,3,6,10]);
property orid.mod_two <- orid % 2;

property orid.cased <-CASE WHEN mod_two = 0 THEN 1 ELSE 0 END;

auto total_mod_two <- sum(cased);

select 
    total_mod_two
  ;
    """
    factory = Factory(environment=default_duckdb_engine.environment)
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    cased = factory.build(default_duckdb_engine.environment.concepts["cased"])
    total = factory.build(default_duckdb_engine.environment.concepts["total_mod_two"])
    assert cased.purpose == Purpose.PROPERTY
    assert cased.keys == {"local.orid"}
    assert total.derivation == Derivation.AGGREGATE
    x = resolve_function_parent_concepts(
        total, environment=default_duckdb_engine.environment
    )
    assert len(cased.concept_arguments) == 1
    assert "local.orid" in get_upstream_concepts(cased)

    # for x in total.lineage.concept_arguments:
    #     if isinstance(x, Concept) and x.purpose == Purpose.PROPERTY and x.keys:
    #         raise SyntaxError(x.keys)
    assert "local.cased" in x
    assert "local.orid" in x
    # function_to_concept(
    #     parent=function_to_concept()
    # )
    assert results[0] == (3,)


def test_simple_case_duckdb():
    """Test simple CASE syntax execution in DuckDB."""
    executor = Dialects.DUCK_DB.default_executor()

    test = """
auto category <- unnest(['Seafood', 'Beverages', 'Meat', 'Dairy']);
property category.bucket <- CASE category
    WHEN 'Seafood' THEN 'sea'
    WHEN 'Beverages' THEN 'drink'
    ELSE 'other'
END;

select
    category,
    bucket
order by category asc;
    """
    results = executor.execute_text(test)[0].fetchall()
    # Results should be ordered by category: Beverages, Dairy, Meat, Seafood
    assert len(results) == 4
    assert results[0] == ("Beverages", "drink")
    assert results[1] == ("Dairy", "other")
    assert results[2] == ("Meat", "other")
    assert results[3] == ("Seafood", "sea")


def test_simple_case_duckdb_uses_native_syntax():
    """Test that DuckDB uses native simple CASE syntax (not expanded)."""
    from trilogy.core.query_processor import process_query
    from trilogy.dialect.duckdb import DuckDBDialect
    from trilogy.parser import parse_text

    env, parsed = parse_text(
        """
auto category <- unnest(['Seafood', 'Beverages']);
property category.bucket <- CASE category
    WHEN 'Seafood' THEN 'sea'
    WHEN 'Beverages' THEN 'drink'
    ELSE 'other'
END;

select
    category,
    bucket;
    """
    )
    select = parsed[-1]
    dialect = DuckDBDialect()

    processed = process_query(env, select)
    compiled = dialect.compile_statement(processed)
    # DuckDB should use native simple CASE syntax (CASE expr WHEN val THEN result)
    # not searched CASE (CASE WHEN expr = val THEN result)
    assert "CASE" in compiled
    # Simple CASE has "WHEN 'value'" directly without "= 'value'"
    assert "WHEN 'Seafood' THEN" in compiled
    # Should NOT have the expanded comparison form
    assert "= 'Seafood'" not in compiled


def test_simple_case_bigquery_expands_syntax():
    """Test that BigQuery expands simple CASE to searched CASE."""
    from trilogy.core.query_processor import process_query
    from trilogy.dialect.bigquery import BigqueryDialect
    from trilogy.parser import parse_text

    env, parsed = parse_text(
        """
auto category <- unnest(['Seafood', 'Beverages']);
property category.bucket <- CASE category
    WHEN 'Seafood' THEN 'sea'
    WHEN 'Beverages' THEN 'drink'
    ELSE 'other'
END;

select
    category,
    bucket;
    """
    )
    select = parsed[-1]
    dialect = BigqueryDialect()

    processed = process_query(env, select)
    compiled = dialect.compile_statement(processed)
    # BigQuery should expand to searched CASE (CASE WHEN expr = val THEN result)
    # not simple CASE (CASE expr WHEN val THEN result)
    assert "CASE" in compiled
    # BigQuery should use expanded form with equality comparison "= 'value'"
    assert "= 'Seafood'" in compiled
    # Searched CASE form should have WHEN with condition, not just value
    assert "WHEN" in compiled


def test_demo_filter():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([1,2,2,3]);

auto even_x <- filter x where (x % 2) = 0;

select 
    x, 
    even_x
order by x 
asc
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (1, None)
    assert results[1] == (2, 2)
    assert len(results) == 4


def test_demo_filter_select():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    test = """const x <- unnest([1,2,2,3]);

select
  ~x,
  x*x*x -> x_cubed
where 
  (x % 2) = 0;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (2, 8)
    assert len(results) == 2


def test_demo_filter_rowset():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
const x <- unnest([1,2,3,4]);

with even_squares as select 
    x, 
    x*x as x_squared
having (x_squared %2) = 0;

select 
    even_squares.x_squared
order by
    even_squares.x_squared asc
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (4,)
    assert len(results) == 2


def test_filter_count():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([1,2,2,3]);

auto y <- x+1;

auto odd_y <- filter x where (x % 2) = 0;

select 
    count(odd_y) -> odd_y_count,
    count(x) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (2, 4)
    assert len(results) == 1


def test_boolean_filter():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([0, 1,2,2,3]);

select 
    count(x ? x ) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (4,)


def test_nullif_filter():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """const x <- unnest([0, 1,2,2,3]);

select 
    count(x ? nullif(x, 2) ) -> x_count
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (2,)


def test_mod_parse_order():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
const x <- unnest([1,2,3,4]);

with even_squares as select 
    x, 
    x*x as x_squared
having x_squared %2  = 0;

select 
    even_squares.x_squared
order by
    even_squares.x_squared asc
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (4,)
    assert len(results) == 2


def test_raw_sql():
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
raw_sql('''
select unnest([1,2,3,4]) as x
order by x asc
''')
;"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (1,)
    assert len(results) == 4


def test_numeric():
    from decimal import Decimal

    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
const number <- 1.456789;
const reduced <- cast(number as numeric(3,2));

select reduced;
"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert results[0] == (Decimal("1.46"),)
    assert len(results) == 1


def test_cast_timestamptz_to_date():
    """Test casting TIMESTAMP WITH TIME ZONE to DATE.

    Some DuckDB versions throw "Conversion Error: Unimplemented type for cast
    (TIMESTAMP WITH TIME ZONE -> DATE)" when attempting this cast directly.
    This test ensures we handle this case properly by converting to UTC first.
    """
    from datetime import date

    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
key id int;
property id.data_updated_through timestamp;
property id.data_updated_through_no_tz datetime;

datasource test_data (
    id: id,
    data_updated_through: data_updated_through,
    data_updated_through_no_tz: data_updated_through_no_tz
)
grain (id)
query '''
select 1 as id, 
'2024-01-15 10:30:00-05:00'::timestamptz as data_updated_through,
'2024-01-15 10:30:00'::timestamp as data_updated_through_no_tz
union all
select 2 as id, 
'2024-01-15 22:30:00-05:00'::timestamptz as data_updated_through,
'2024-01-15 22:30:00'::timestamp as data_updated_through_no_tz
''';

auto update_date <- cast(data_updated_through as date);
auto update_date_no_tz <- cast(data_updated_through_no_tz as date);
select id, update_date, update_date_no_tz order by id asc;
"""
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    default_duckdb_engine.hooks = [DebuggingHook()]
    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 2
    # Row 1: 10:30:00-05:00 = 15:30:00 UTC, same day (2024-01-15)
    assert results[0].update_date == date(2024, 1, 15)
    assert results[0].update_date_no_tz == date(2024, 1, 15)

    # Row 2: 22:30:00-05:00 = 03:30:00+1 UTC, next day (2024-01-16)
    # This tests that we properly convert to UTC before extracting date
    assert results[1].update_date == date(2024, 1, 16)
    assert results[1].update_date_no_tz == date(2024, 1, 15)


def test_filter_promotion(duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
SELECT
    item
where
    value>1;

"""

    duckdb_engine.hooks = [DebuggingHook()]
    results = duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 2


def test_filter_promotion_complicated(duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """
auto all_store_count <- sum(count);
SELECT
    item,
    all_store_count
where
    store_id in (1,3)
    and item = 'hammer'
order by
    item desc;
"""

    duckdb_engine.hooks = [DebuggingHook()]
    results = duckdb_engine.execute_text(test)[0].fetchall()
    # derived = duckdb_engine.environment.concepts["all_store_count"]
    # assert isinstance(derived.lineage, AggregateWrapper)
    # assert derived.lineage.by == [duckdb_engine.environment.concepts["item"]]
    assert len(results) == 1
    assert results[0] == ("hammer", 4)


def test_filtered_datasource():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    test = """key orid int;
key store string;
key customer int;

auto customer_orders <- count(orid) by customer;
datasource filtered_orders(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', 244
union all
select 4, 'store3', 244
'''
where store = 'store2';


select 
    avg(customer_orders) -> avg_customer_orders,
    avg(count(orid) by store) -> avg_store_orders,
;"""
    results = executor.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_customer_orders == 2
    assert round(results[0].avg_store_orders, 2) == 2


def test_cte_filter_promotion():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    test = """key orid int;
key store string;
key customer int;


datasource filtered_orders(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', 244
union all
select 4, 'store3', 244
''';


with orders_145 as
SELECT
    store,
    count(orid) -> store_order_count
where
    customer=145
;

select
    orders_145.store,
    orders_145.store_order_count
;


"""
    results = executor.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert round(results[0].orders_145_store_order_count, 2) == 1


def test_filter_promotion_inline_aggregate_filtered(duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    test = """


WHERE
    store_id = 1
SELECT
    item,
    sum(count) -> all_store_count
having
    all_store_count > 1
order by
    item desc;
"""

    duckdb_engine.hooks = [DebuggingHook()]

    # assert target.grain.components == [duckdb_engine.environment.concepts["item"]]
    results = duckdb_engine.execute_text(test)[0].fetchall()
    # derived = parsed.local_concepts["local.all_store_count"]
    # assert isinstance(derived.lineage, AggregateWrapper)
    # assert derived.lineage.by == [duckdb_engine.environment.concepts["item"]]
    assert len(results) == 1
    assert results[0] == ("hammer", 2)


def test_duckdb_load():
    env = Environment(working_path=Path(__file__).parent)
    exec = Dialects.DUCK_DB.default_executor(environment=env)

    results = exec.execute_query(
        r"""
        auto csv <- _env_working_path || '/test.csv';

        RAW_SQL('''
        CREATE TABLE ages AS FROM read_csv(:csv);
        '''
        );"""
    )

    results = exec.execute_raw_sql("SELECT * FROM ages;").fetchall()

    assert results[0].age == 23


def test_duckdb_string_quotes():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(
        r"""
        const csv <- '''this string has quotes ' like this''';

    select csv;
        """
    )

    results = results.fetchall()

    assert results[0].csv == """this string has quotes ' like this"""


def test_demo_recursive_error():
    query = """key idx int;
property idx.idx_val int;
datasource numbers(
    idx: idx,
    x: idx_val
)
grain (idx)
query '''
select 1 idx, 1 x
union all
select 2, 2
union all
select 3, 2
union all
select 4, 3
''';

SELECT
  idx_val,
  count(idx) as number_count
order by
    idx_val asc;"""

    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(query)

    results = results.fetchall()


def test_union():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    exec = Dialects.DUCK_DB.default_executor()

    results = exec.execute_query(
        r"""
key space_one int;
key space_two int;


property space_one.one_name string;
property space_two.two_name string;

auto space_all <- union(space_one, space_two);
auto name <- union(one_name, two_name);

datasource sone (
x: space_one,
y: one_name )
grain (space_one)
query '''
select 2 as x , 'test' as y
union all
select 1, 'fun'
''';


datasource stwo (
x: space_two,
y: two_name )
grain (space_two)
query '''
select 4 as x, 'bravo' as y
union all
select 5, 'alpha'
''';



select
    space_all,
    name,
order by
    space_all asc
limit 100;
        """
    )

    results = list(results.fetchall())

    assert results[0].space_all == 1
    assert results[0].name == "fun"
    assert results[-1].space_all == 5


def test_multi_select_mutation():
    exec = Dialects.DUCK_DB.default_executor()

    queries = exec.parse_text(
        """

auto x <- 1;
                    
select
    x + 1 -> x_next;
                    
select
    x + 2 -> x_next;
                    
"""
    )

    for idx, x in enumerate(queries):
        results = exec.execute_query(x).fetchall()
        assert results[0].x_next == 2 + idx


def test_commit():
    exec = Dialects.DUCK_DB.default_executor()
    exec.connection.begin()
    exec.execute_raw_sql("create table test (test int);")
    exec.connection.commit()
    exec.connection.begin()
    exec.execute_raw_sql("insert into test values (1);")
    exec.execute_raw_sql("insert into test values (2);")
    results = exec.execute_raw_sql("select * from test;").fetchall()
    assert len(results) == 2
    exec.connection.rollback()
    results = exec.execute_raw_sql("select * from test;").fetchall()
    assert len(results) == 0


def test_parquet_format_access():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    parquet_path = Path(__file__).parent / "customer.parquet"
    nations_path = Path(__file__).parent / "nation.parquet"
    executor.parse_text(
        f"""

key id int;
property id.text_id string;
property id.last_name string;
property id.first_name string;
property id.salutation string;

property id.full_name <- concat(salutation, ' ', first_name, ' ', last_name);

key nation_id int;
property nation_id.nation_name string;

datasource customers (
    C_CUSTKEY: id,
    C_NAME: full_name,
    C_NATIONKEY: nation_id,
)
grain (id)
address `{parquet_path}`;

datasource nations (
    N_NATIONKEY: nation_id,
    N_NAME: nation_name,
)
grain(nation_id)
address `{nations_path}`;
"""
    )
    _ = executor.execute_raw_sql(f'select * from "{parquet_path}" limit 1;')
    r = executor.execute_query("select count(id) as customer_count;")

    assert r.fetchall()[0].customer_count == 1500

    r = executor.execute_query(
        "select nation_name,  count(id) as customer_count order by customer_count desc;"
    )

    assert r.fetchall()[0].customer_count == 72


def test_duckdb_date_add():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    r = executor.execute_query(
        r"""
auto today <- date_add(current_datetime(), day, -3);
select today;
"""
    )

    results = r.fetchall()

    assert results[0].today.date() == datetime.now().date() - timedelta(days=3)


def test_duckdb_alias():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    r = executor.execute_query(
        r"""
auto today <- date_add(current_datetime(), day, -3);
select today as tomorrow, today;
"""
    )

    results = r.fetchall()

    assert results[0].today.date() == datetime.now().date() - timedelta(days=3)
    assert results[0].tomorrow.date() == datetime.now().date() - timedelta(days=3)


def test_function_parsing():
    query = """
    import function_test as game;

where @game.is_team_game('pelican') = true
select game.home_team.name, count(game.id)->game_count;"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    executor.parse_text(query)


def test_global_aggregate_inclusion():
    """check that including a global aggregate constant in output select doesn't force changed evaluation order"""
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    query = """
    key id int;
key date date;
property <id, date>.score int;

datasource raw (
    id: id,
    date: date,
    score: score
)
grain (id,date)
query '''
select 1 as id, '2023-01-01' as date, 10 as score
union all
select 2, '2023-01-02', 20
union all
select 3, '2023-01-03', 30
union all
select 4, '2023-01-03', 40
''';

auto max_date <- max(date) by *;

"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    executor.parse_text(query)

    results = executor.execute_text(
        """where date = max_date and id >2
select date, avg(score) as avg_id;"""
    )[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_id == 35.0

    results = executor.execute_text(
        """where date = max_date and id >2
select max_date, date, avg(score) as avg_id;"""
    )[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_id == 35.0


def test_tuple_filtering():
    query = """
    key case_number int;
property case_number.primary_type string;
property case_number.ward string;

datasource crimes (
    case_number: case_number,
    primary_type: primary_type,
    ward: ward
)
grain (case_number)
query '''
select 1 as case_number, 'HOMICIDE' as primary_type, 'Ward 1' as ward
union all
select 2, 'ASSAULT', 'Ward 2'
union all
select 3, 'ROBBERY', 'Ward 1'
''';

"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    executor.parse_text(query)

    results = executor.execute_text(
        """select local.ward, count_distinct(local.case_number) as violent_crime_count  
where local.primary_type in ("HOMICIDE"::string, "ASSAULT"::string, "ROBBERY"::string, "AGGRAVATED ASSAULT"::string)  
having violent_crime_count > 0
order by local.ward asc
                                    ; """
    )[0].fetchall()

    assert len(results) == 2
    assert results[0].violent_crime_count == 2


def test_multiple_string_filters():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    query = """
    key case_number int;
property case_number.primary_type string;
property case_number.ward string;

datasource crimes (
    case_number: case_number,
    primary_type: primary_type,
    ward: ward
)
grain (case_number)
query '''
select 1 as case_number, 'HOMICIDE' as primary_type, 'Ward 1' as ward
union all
select 2, 'ASSAULT', 'Ward 2'
union all
select 3, 'ROBBERY', 'Ward 1'
''';
"""

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    executor.parse_text(query)

    results = executor.execute_text(
        """where ( local.primary_type = "HOMICIDE"::string or local.primary_type= "ASSAULT"::string)
        select 1 as test;
                                    """
    )[0].fetchall()

    assert len(results) == 1


#     assert results[0].test == 1

#     results = executor.execute_text(
#         """select local.ward, count_distinct(local.case_number) as violent_crime_count
# where ward='Ward 2' and ( local.primary_type = "HOMICIDE"::string or local.primary_type= "ASSAULT"::string)
# having violent_crime_count > 0
# order by local.ward asc
#                                     ; """
#     )[0].fetchall()

#     assert len(results) == 1
#     assert results[0].violent_crime_count == 1


# def test_variable_plan_generation():
#     from trilogy.hooks import DebuggingHook
#     DebuggingHook()
#     query = """
#     key case_number int;
# property case_number.primary_type string;
# property case_number.ward string;

# datasource crimes (
#     case_number: case_number,
#     primary_type: primary_type,
#     ward: ward
# )
# grain (case_number)
# query '''
# select 1 as case_number, 'HOMICIDE' as primary_type, 'Ward 1' as ward
# union all
# select 2, 'ASSAULT', 'Ward 2'
# union all
# select 3, 'ROBBERY', 'Ward 1'
# ''';

# """

#     executor: Executor = Dialects.DUCK_DB.default_executor(
#         environment=Environment(working_path=Path(__file__).parent)
#     )
#     executor.parse_text(query)

#     base =  executor.generate_sql(
#         """select local.ward, count_distinct(local.case_number) as violent_crime_count
# where local.primary_type in ("HOMICIDE"::string, "ASSAULT"::string, "ROBBERY"::string, "AGGRAVATED ASSAULT"::string)
# having violent_crime_count > 0
# order by local.ward asc
#                                     ; """
#     )[0]
#     comp = executor.generate_sql(
#         """
# auto homicide <- "HOMICIDE"::string;
# auto assault <- "ASSAULT"::string;
# auto robbery <- "ROBBERY"::string;

# select local.ward, count_distinct(local.case_number) as violent_crime_count
# where local.primary_type in (homicide, assault, robbery)
# having violent_crime_count > 0
# order by local.ward asc
#                                     ; """
#     )[0]

#     assert base == comp


def test_null_filtering():
    executor: Executor = Dialects.DUCK_DB.default_executor(environment=Environment())

    test = """key orid int;
key store string;
key customer int;

auto customer_orders <- count(orid) by customer;
datasource filtered_orders(
  orid: orid,
  store: store,
  customer:customer,
)
grain(orid)
query '''
select 1 orid, 'store1' store, 145 customer
union all
select 2, 'store2', 244
union all
select 3, 'store2', null
union all
select 4, 'store3', 244
'''
where store = 'store2';

where store = 'store2' and customer IS NULL
select 
    avg(customer_orders) -> avg_customer_orders,
    avg(count(orid) by store) -> avg_store_orders,
;"""
    results = executor.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].avg_customer_orders == 1


def test_recursive():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    executor.environment.parse(
        """import recursive;
# traverse parent-> id until you hit a null
auto first_parent <- recurse_edge(id, parent);"""
    )

    assert (
        executor.environment.concepts["first_parent"].derivation == Derivation.RECURSIVE
    )
    executor.generate_sql(
        """where
first_parent = 1    
select id, label
order by label asc;
"""
    )[-1]
    results = executor.execute_text(
        """where
first_parent = 1
select id, label;
"""
    )[0].fetchall()
    assert len(results) == 4
    assert results[0].label == "A"


def test_recursive_enrichment():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()

    executor: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    executor.environment.parse(
        """
import recursive;
import recursive as parent;
# traverse parent-> id until you hit a null
auto first_parent <- recurse_edge(id, parent);  

merge first_parent into parent.id;                 
                               
                               """
    )

    recursive = executor.environment.alias_origin_lookup["local.first_parent"]
    assert recursive.derivation == Derivation.RECURSIVE, "recursive should be recursive"

    results = executor.execute_text(
        """where
first_parent = 1
select id, parent.label;
"""
    )[0].fetchall()
    assert len(results) == 4
    assert results[-1].parent_label == "A"

    results = executor.execute_text(
        """where
parent.label = 'A'
select count(id) as a_children;
"""
    )[0].fetchall()
    assert len(results) == 1
    assert results[0].a_children == 4


def test_tuple_constant(default_duckdb_engine: Executor):
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    test = """
const list <- (1,2,3,4);

select list;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].list == [1, 2, 3, 4]


def test_in_with_array(default_duckdb_engine: Executor):

    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    test = """

const list <- [1,2,3,4];

const two <- 2;

where two in list
select 
    two;
    
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].two == 2


def test_map(default_duckdb_engine: Executor):

    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    test = """

const map <- {1:2, 3:4};

select 
    map;
    
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].map == {1: 2, 3: 4}


def test_regexp(default_duckdb_engine: Executor):

    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    test = """

const values <- unnest(['apple', 'banana', 'cherry', 'date']);

select 

    regexp_contains(values, '^a.*') as starts_with_a,
    regexp_extract(values, '^a(.*)') as after_a,
    regexp_extract(values, '^a(.*)', 1) as after_a_explicit,
    regexp_extract(values, '^a.*') as no_capture
where starts_with_a = true
order by 
    starts_with_a asc;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    assert results[0].starts_with_a is True
    assert results[0].after_a == "pple"
    assert results[0].after_a_explicit == "pple"
    assert results[0].no_capture == "apple"


def test_window_calc():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const list <- [1,2,3,4,5];
const orid <- unnest(list);

select 
     orid,
     case when rank sum(orid) order by orid asc  = rank sum(orid) order by orid asc then rank sum(orid) order by orid asc else rank sum(orid) order by orid asc end as window_rank
having
    4 < sum(orid) 
  ;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1


def test_replace():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- unnest(['apple', 'banana', 'cherry', 'date']);
select
    replace(values, 'a', 'o') as replaced_values
order by    
    replaced_values asc;
    """

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 4
    assert results[0].replaced_values == "bonono"
    assert results[1].replaced_values == "cherry"
    assert results[2].replaced_values == "dote"
    assert results[3].replaced_values == "opple"


def test_sum_bool():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- unnest([true, false, true, false]);

select sum(values) as true_count
where values = true;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].true_count == 2


def test_log():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- unnest([1, 10, 100, 1000]);

select 
    log(values) as log_values,
    log(values,2) as log_base_2,
    values
order by values asc;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 4
    assert results[0].log_values == 0
    assert results[1].log_values == 1
    assert results[2].log_values == 2
    assert results[3].log_values == 3


def test_trim():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- unnest([ ' abc ', ' def', 'jkl ', 'mon']);

select trim(values) as trimmed_values order by trimmed_values asc;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 4
    assert results[0].trimmed_values == "abc"
    assert results[1].trimmed_values == "def"
    assert results[2].trimmed_values == "jkl"
    assert results[3].trimmed_values == "mon"


def test_filter_constant_unrelated():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    from trilogy.core.processing.utility import get_disconnected_components
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;

"""
    default_duckdb_engine.parse_text(test)
    env = default_duckdb_engine.environment
    # assert env.concepts['dim'].grain ==
    graph_count, graphs = get_disconnected_components(
        concept_map={
            "example": [env.concepts["value"]],
            "other": [env.concepts["dim"], env.concepts["x"]],
        }
    )
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""

    # assert graph_count == 1,graphs

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1


def test_array_to_string():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const values <- [ ' abc ', ' def', 'jkl ', 'mon'];

select array_to_string(values, ', ') as value_string;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].value_string == " abc ,  def, jkl , mon"


def test_not_value():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
const value <- unnest([ true, null, false]);

select value where not value;
"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert default_duckdb_engine.environment.concepts["value"].datatype == DataType.BOOL
    assert len(results) == 2, str(
        default_duckdb_engine.environment.concepts["value"].lineage
    )


def test_validate():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    test = """validate all;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 0

    test = """validate datasources example;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 0


def test_validate_fix():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    from trilogy.core.validation.fix import validate_and_rewrite

    test = """key x int; # guessing at type
# but who cares, right
key y int;

datasource dim_y (
    y: y
)
grain (y)
query '''
select 1 as y union all select 2 as y union all select 3 as y''';

# a fun comment
datasource example (
    x: x,
    y: y
    )
grain (x)
query '''
select 'abc' as x, 1 as y union all select null as x, null as y''';
"""
    rewritten = validate_and_rewrite(test, default_duckdb_engine)

    assert (
        rewritten.strip()
        == """
key x string; # guessing at type
# but who cares, right
key y int;

datasource dim_y (
    y
)
grain (y)
query '''
select 1 as y union all select 2 as y union all select 3 as y''';
datasource example (
    x: ?x,
    y: ~?y
)
grain (x)
query '''
select 'abc' as x, 1 as y union all select null as x, null as y''';
""".strip()
    ), rewritten.strip()


def test_validate_fix_types():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    from trilogy.core.validation.fix import validate_and_rewrite

    test = """
import std.geography;
key x int; # guessing at type
key y int::latitude;
key z numeric::longitude;

# a fun comment
datasource example (
    x: x,
    y: y,
    z: z
)
grain (x)
query '''
select 'abc' as x, 1.0 as y, 2.0 as z union all select null as x, null as y, null as z''';
"""
    rewritten = validate_and_rewrite(test, default_duckdb_engine)

    assert (
        rewritten.strip()
        == """import std.geography;

key x string; # guessing at type
key y numeric::latitude;
key z numeric::longitude;

datasource example (
    x: ?x,
    y: ?y,
    z: ?z
)
grain (x)
query '''
select 'abc' as x, 1.0 as y, 2.0 as z union all select null as x, null as y, null as z''';
""".strip()
    ), rewritten.strip()


def test_show_validate():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    test = """validate all;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 0
    for row in results:
        assert row.ran is True, str(row)

    test = """show validate all;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    # this has to include a DS query
    # that inline validation doesn't need to run
    assert len(results) == 1
    for row in results:
        assert row.ran is False or row.check_type == "logical"


def test_show_validate_generation():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    test = """show validate all;"""

    results = default_duckdb_engine.parse_text(test)

    default_duckdb_engine.generate_sql(results[0])


def test_mock_statement():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    test = """
import std.metric;
import std.color;
import std.net;

key x int;
key y int;

property x.name string;
property x.email string::email_address;
property x.favorite_color string::hex;
property x.value float::kn;
property x.numeric numeric::kn;
property x.created_at timestamp;
property x.created_at_date date;
property x.labels array<string>;

key year date;
key string_key string;
key bool_key bool;
key date_key date;
key datetime_key datetime;


datasource example (
x, 
y,
name,

value,
numeric,
created_at,
created_at_date,

)
grain (x,y)
address `my-gbq-table.my-project.tbl_example`;


datasource enrichment (
x,
labels,
email,
favorite_color,
)
grain (x)
address `my-gbq-table.my-project.tbl_enrichment`;

datasource years (
    year
)
grain (year)
address `my-gbq-table.my-project.tbl_years`;


datasource keys (
    datetime_key,
    date_key,
    string_key,
    bool_key,
    date_key)
grain (datetime_key, string_key, bool_key, date_key)
address `my-gbq-table.my-project.tbl_keys`;
    

mock datasource example, enrichment, years, keys;

select x, labels, email, favorite_color;
"""

    results = default_duckdb_engine.execute_text(test)[-1].fetchall()
    assert len(results) == DEFAULT_SCALE_FACTOR
    assert isinstance(results[0].x, int)
    assert isinstance(results[0].labels, list)
    assert "@" in results[0].email


def test_group_syntax():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    test = """
key x int;
key y int;

datasource example (
x,
y
)
grain (x)
query '''
select 1 as x, 1 as y union all select 2 as x, 2 as y union all select 3 as x, 3 as y''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    test = """select
    round(avg(x) by y/avg(x) by y, 2) as rounded;
    """

    results = default_duckdb_engine.parse_text(test)

    default_duckdb_engine.generate_sql(results[0])


def test_connection_management():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""select 1 as test;""")
    executor.close()
    executor.execute_text("""select 1 as test;""")


def test_proper_basic_unnest_handling():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()
    test = """const prime <- unnest([2, 3, 5, 7, 11, 13, 17, 19, 23, 29]);

def cube_plus_one(x) -> (x * x * x + 1);

WHERE 
    prime_cubed_plus_one % 7 = 0
SELECT
    prime,
    @cube_plus_one(prime) as prime_cubed_plus_one
ORDER BY
    prime asc
LIMIT 10;"""
    executor.parse_text(test)
    c = executor.environment.concepts["prime_cubed_plus_one"]
    if c.lineage.operator == FunctionType.CONSTANT:  # type: ignore
        raise ValueError(
            "prime_cubed_plus_one should not be constant {}".format(c.lineage)
        )
    results = executor.execute_text(test)[-1].fetchall()

    assert len(results) == 5


@mark.skip("Date spine not yet supported")
def test_date_spine():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()
    with executor.generator.rendering.temporary(parameters=False):
        test = """key prime date;
        property prime.val int;
        datasource primes(  
            prime,
            val
        )
        grain(prime)
        query '''
    SELECT current_date AS prime, 2 as  val
    UNION ALL
    SELECT current_date - INTERVAL 1 DAY AS prime, 1 as  val
    UNION ALL
    SELECT current_date - INTERVAL 1 DAY AS prime, 3 as  val
    UNION ALL
    SELECT current_date - INTERVAL 2 DAY AS prime, 3 as  val
    UNION ALL
    SELECT current_date - INTERVAL 3 DAY AS prime, 4 as  val
    UNION ALL
    SELECT current_date - INTERVAL 10 DAY AS prime, 5 as  val
        '''
        ;


        auto first_date <- min(prime) by *;
        auto last_date <- max(prime) by *;

        SELECT
            date_spine(prime, current_date()) as filled_in_dates,
            sum val order by filled_in_dates asc -> day_count
        ;
        """
        sql = executor.generate_sql(executor.parse_text(test)[-1])
        results = executor.execute_text(test)[-1].fetchall()

        assert len(results) == 8, sql


def test_date_spine_merge():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()
    with executor.generator.rendering.temporary(parameters=False):
        test = """key prime date;
        property prime.val int;
        datasource primes(  
            prime,
            val
        )
        query '''
    SELECT current_date AS prime, 2 as  val
    UNION ALL
    SELECT current_date - INTERVAL 1 DAY AS prime, 1 as  val
    UNION ALL
    SELECT current_date - INTERVAL 1 DAY AS prime, 3 as  val
    UNION ALL
    SELECT current_date - INTERVAL 2 DAY AS prime, 3 as  val
    UNION ALL
    SELECT current_date - INTERVAL 3 DAY AS prime, 4 as  val
    UNION ALL
    SELECT current_date - INTERVAL 10 DAY AS prime, 5 as  val
        '''
        ;


        auto base_spine <- date_spine(date_add(current_date(), day, -10), current_date());

        merge prime into ~base_spine;

        SELECT
            base_spine,
            sum sum(val) by base_spine order by base_spine asc -> day_count
        ;
        """
        sql = executor.generate_sql(executor.parse_text(test)[-1])
        results = executor.execute_text(test)[-1].fetchall()

        assert results[-1].day_count == 18, sql


def test_const_equivalence_merge():
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    executor = Dialects.DUCK_DB.default_executor()
    with executor.generator.rendering.temporary(parameters=False):
        test = """key orid int;
        auto orid_2 <- unnest([1,2,3,4,5]);

        property orid.val int;

        datasource orders (
            ~orid,
            val
        )
        grain (orid)
        query '''
        select 1 as orid, 10 as val
        union all
        select 2, 20
        ''';

        merge orid into ~orid_2;

        select orid_2, val;
        """

        sql = executor.generate_sql(executor.parse_text(test)[-1])
        results = executor.execute_text(test)[-1].fetchall()
        assert len(results) == 5, sql


def test_multi_select_derive():
    exec = Dialects.DUCK_DB.default_executor()
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    queries = exec.parse_text(
        """

auto x <- 1;
                    
select
    1-> x_val,
    x + 1 -> x_next
merge               
select
    2-> y_val,
    x + 2 -> y_next
align val:x_val, y_val
derive x_next + y_next -> total
;
                    
"""
    )

    for idx, x in enumerate(queries):
        print(x.output_columns)
        results = exec.execute_query(x).fetchall()
        assert results[0].x_next == 2 + idx
        assert results[0].total == 5


def test_multi_select_derive_import():
    exec = Dialects.DUCK_DB.default_executor()
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    queries = exec.parse_text(
        """

auto x <- 1;

with rows as             
select
    1-> x_val,
    x + 1 -> x_next
merge               
select
    2-> y_val,
    x + 2 -> y_next
align val:x_val, y_val
derive x_next + y_next -> total
;

"""
    )
    exec2 = Dialects.DUCK_DB.default_executor()
    exec2.environment.add_import("dependent", exec.environment, None)

    assert exec2.environment.concepts["dependent.rows.x_next"]
    queries = exec2.parse_text(
        """
        select
        dependent.rows.x_next, dependent.rows.total
        ;
        """
    )

    for idx, x in enumerate(queries):
        print(x.output_columns)
        results = exec2.execute_query(x).fetchall()
        assert results[0].dependent_rows_x_next == 2 + idx
        assert results[0].dependent_rows_total == 5


def test_order_by_count():
    exec = Dialects.DUCK_DB.default_executor()
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    exec.parse_text(
        """
key state string;
property state.count int;
datasource origin (
state: state,
count: count
)
grain (state)
query '''
select 'CA' as state, 10 as count
union all
select 'NY', 20
union all
select 'TX', 30 
''';

select
   state,
   count, 
   order by count desc;

"""
    )


def test_existence():
    exec = Dialects.DUCK_DB.default_executor()
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    results = exec.execute_text(
        """
key state string;
property state.count int;
datasource origin (
state: state,
count: count
)
grain (state)
query '''
select 'CA' as state, 10 as count
union all
select 'NY', 20
union all
select 'TX', 30 
''';

where state in (state? count>20)
select
   state,
;

where state in state? count>20
select
   state,
;
"""
    )
    assert results[-1].fetchall()[0].state == "TX"


def test_datetime_functions():
    environment = Environment()
    _, queries = environment.parse(
        """
    const order_id <- 1;
    const order_timestamp <- current_datetime();
    select
        order_id,
        order_timestamp,
        date(order_timestamp) -> order_date,
        datetime(order_timestamp) -> order_timestamp_datetime,
        timestamp(order_timestamp) -> order_timestamp_dos,
        second(order_timestamp) -> order_second,
        minute(order_timestamp) -> order_minute,
        hour(order_timestamp) -> order_hour,
        day(order_timestamp) -> order_day,
        week(order_timestamp) -> order_week,
        month(order_timestamp) -> order_month,
        quarter(order_timestamp) -> order_quarter,
        year(order_timestamp) -> order_year,
        date_trunc(order_timestamp, month) -> order_month_trunc,
        date_add(order_timestamp, month, 1) -> one_month_post_order,
        date_sub(order_timestamp, month, 1) -> one_month_pre_order,
        date_trunc(order_timestamp, day) -> order_day_trunc,
        date_trunc(order_timestamp, year) -> order_year_trunc,
        date_trunc(order_timestamp, hour) -> order_hour_trunc,
        date_trunc(order_timestamp, minute) -> order_minute_trunc,
        date_trunc(order_timestamp, second) -> order_second_trunc,
        date_trunc(order_timestamp, quarter) -> order_quarter_trunc,
        date_trunc(order_timestamp, week) -> order_week_trunc,
        date_part(order_timestamp, month) -> order_month_part,
        date_part(order_timestamp, day) -> order_day_part,
        date_part(order_timestamp, year) -> order_year_part,
        date_part(order_timestamp, hour) -> order_hour_part,
        date_part(order_timestamp, minute) -> order_minute_part,
        date_part(order_timestamp, second) -> order_second_part,
        date_part(order_timestamp, quarter) -> order_quarter_part,
        date_part(order_timestamp, week) -> order_week_part,
        date_part(order_timestamp, day_of_week) -> order_day_of_week_part,
        month_name(order_timestamp) -> order_month_name,
        day_name(order_timestamp) -> order_day_name,
        format_time(order_timestamp, '%Y-%m-%d %H:%M:%S') -> order_timestamp_strftime,
        parse_time(format_time(order_timestamp, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S') -> order_timestamp_parse,
        date_sub(order_timestamp, day, 30) -> thirty_days_ago,
        date_diff(thirty_days_ago, order_timestamp, day) -> date_diff_days
    ;


        """
    )

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    results = executor.execute_query(queries[-1]).fetchall()
    row = results[0]

    # Basic identity checks
    assert row.order_id == 1

    # date_part extractions should match direct extractions
    assert row.order_second == row.order_second_part
    assert row.order_minute == row.order_minute_part
    assert row.order_hour == row.order_hour_part
    assert row.order_day == row.order_day_part
    assert row.order_week == row.order_week_part
    assert row.order_month == row.order_month_part
    assert row.order_quarter == row.order_quarter_part
    assert row.order_year == row.order_year_part

    # date_trunc in DuckDB returns date type, so check date components only
    assert row.order_second_trunc.microsecond == 0
    assert (
        row.order_minute_trunc.second == 0 and row.order_minute_trunc.microsecond == 0
    )
    assert row.order_hour_trunc.minute == 0 and row.order_hour_trunc.second == 0
    assert row.order_day_trunc.hour == 0 and row.order_day_trunc.minute == 0
    assert row.order_month_trunc.day == 1 and row.order_month_trunc.hour == 0
    assert row.order_year_trunc.month == 1 and row.order_year_trunc.day == 1
    assert (
        row.order_quarter_trunc.month in (1, 4, 7, 10)
        and row.order_quarter_trunc.day == 1
    )

    # date_add/date_sub relationships
    assert row.one_month_post_order > row.order_timestamp
    assert row.one_month_pre_order < row.order_timestamp
    assert row.thirty_days_ago < row.order_timestamp

    # date_diff should be 30 days
    assert row.date_diff_days == 30

    # time component ranges
    assert 0 <= row.order_second <= 59
    assert 0 <= row.order_minute <= 59
    assert 0 <= row.order_hour <= 23
    assert 1 <= row.order_day_of_week_part <= 7

    # month_name and day_name should be strings
    assert isinstance(row.order_month_name, str) and len(row.order_month_name) > 0
    assert isinstance(row.order_day_name, str) and len(row.order_day_name) > 0

    # format_time and parse_time round-trip
    assert row.order_timestamp_parse is not None
